package com.zilliz.spark.connector.write

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

import org.apache.arrow.c.{
  ArrowArray,
  ArrowSchema,
  CDataDictionaryProvider,
  Data
}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  BitVector,
  FixedSizeBinaryVector,
  Float4Vector,
  Float8Vector,
  IntVector,
  SmallIntVector,
  TinyIntVector,
  VarBinaryVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.complex.{
  FixedSizeListVector,
  ListVector,
  StructVector
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Verifies Arrow Java 17's C Data Interface refcount semantics:
  * `Data.exportVectorSchemaRoot` must retain each buffer independently of the
  * source root, so that closing the source root immediately after export does
  * not invalidate the exported buffers.
  *
  * This is the correctness precondition for the aggressive memory strategy in
  * [[MilvusV2BinlogWriter]] / [[MilvusLoonWriter]]: close each
  * `VectorSchemaRoot` right after exporting it to the C++ writer, instead of
  * retaining every root until the writer closes (which caused per-segment
  * direct-memory growth proportional to row count).
  *
  * The tests cover the Arrow types emitted by `MilvusSchemaUtil
  * .convertSparkSchemaToArrow` (the schema converter used by both writers):
  *   - fixed-width primitives: Int8, Int16, Int32, Int64, Float32, Float64
  *   - boolean (bit-packed validity + value)
  *   - variable-width: Utf8, Binary
  *   - FixedSizeBinary (the backfill path's representation of dense float
  *     vectors)
  *   - FixedSizeList<Float> (alternative embedded-vector representation)
  *   - List<Int>, List<Utf8> (variable-length arrays with both fixed- and
  *     variable-width children)
  *   - Struct<Int64, Utf8> (nested fields with independent buffers)
  *   - nullable validity bitmaps across all of the above
  *
  * Arrow's Map type is not exercised directly because it is physically encoded
  * as `List<Struct<key, value>>`; the refcount property for Map follows from
  * the List and Struct cases combined with Arrow C Data Interface's uniform
  * buffer-ownership model.
  *
  * Each test round-trips data through export → close(source) → import and
  * verifies byte-for-byte equality.
  */
class ArrowCDataRefcountTest extends AnyFunSuite with Matchers {

  /** Populate + export + close source + import-back, verify `check` on the
    * imported root.
    */
  private def roundTrip(
      field: Field,
      populate: VectorSchemaRoot => Unit,
      rowCount: Int
  )(check: VectorSchemaRoot => Unit): Unit = {
    val parent = new RootAllocator(Long.MaxValue)
    try {
      val alloc = parent.newChildAllocator("test", 0, Long.MaxValue)
      val schema = new Schema(java.util.List.of(field))
      val root = VectorSchemaRoot.create(schema, alloc)

      populate(root)
      root.setRowCount(rowCount)
      val bytesBeforeExport = alloc.getAllocatedMemory
      bytesBeforeExport should be > 0L

      val cSchema = ArrowSchema.allocateNew(alloc)
      val cArray = ArrowArray.allocateNew(alloc)
      try {
        Data.exportSchema(alloc, schema, null, cSchema)
        Data.exportVectorSchemaRoot(alloc, root, null, cArray)

        // THE invariant under test: the source root can be closed immediately
        // because `exportVectorSchemaRoot` retained each buffer on its own.
        root.close()

        // If retain semantics were broken, the underlying buffers would be freed
        // by root.close() and the subsequent import would either crash (SIGSEGV)
        // or observe garbage. A live allocated-byte count proves buffers survive.
        alloc.getAllocatedMemory should be > 0L

        val provider = new CDataDictionaryProvider()
        val imported =
          Data.importVectorSchemaRoot(alloc, cArray, cSchema, provider)
        try {
          imported.getRowCount shouldBe rowCount
          check(imported)
        } finally {
          imported.close()
          provider.close()
        }

        // After the imported root closes, the release callback fires and drops
        // the last refcount — allocator should be empty again.
        alloc.getAllocatedMemory shouldBe 0L
      } finally {
        cArray.close()
        cSchema.close()
        alloc.close()
      }
    } finally parent.close()
  }

  // ---------------------------------------------------------------- primitives

  test(
    "Int32 nullable: export → close source → import preserves values and nulls"
  ) {
    val field =
      new Field("i32", FieldType.nullable(new ArrowType.Int(32, true)), null)
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("i32").asInstanceOf[IntVector]
        v.allocateNew(5)
        v.setSafe(0, 42)
        v.setSafe(1, -7)
        v.setNull(2)
        v.setSafe(3, Int.MaxValue)
        v.setSafe(4, Int.MinValue)
        v.setValueCount(5)
      },
      rowCount = 5
    ) { root =>
      val v = root.getVector("i32").asInstanceOf[IntVector]
      v.get(0) shouldBe 42
      v.get(1) shouldBe -7
      v.isNull(2) shouldBe true
      v.get(3) shouldBe Int.MaxValue
      v.get(4) shouldBe Int.MinValue
    }
  }

  test("Int64 non-nullable: all values round-trip") {
    val field =
      new Field("i64", FieldType.notNullable(new ArrowType.Int(64, true)), null)
    val values = Array[Long](0L, 1L, -1L, Long.MaxValue, Long.MinValue, 42L)
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("i64").asInstanceOf[BigIntVector]
        v.allocateNew(values.length)
        values.zipWithIndex.foreach { case (x, i) => v.setSafe(i, x) }
        v.setValueCount(values.length)
      },
      rowCount = values.length
    ) { root =>
      val v = root.getVector("i64").asInstanceOf[BigIntVector]
      values.zipWithIndex.foreach { case (x, i) => v.get(i) shouldBe x }
    }
  }

  test("Float32 with alternating nulls: validity bitmap survives") {
    val field = new Field(
      "f32",
      FieldType.nullable(
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      ),
      null
    )
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("f32").asInstanceOf[Float4Vector]
        v.allocateNew(6)
        v.setSafe(0, 1.5f)
        v.setNull(1)
        v.setSafe(2, -3.25f)
        v.setNull(3)
        v.setSafe(4, Float.PositiveInfinity)
        v.setSafe(5, Float.NaN)
        v.setValueCount(6)
      },
      rowCount = 6
    ) { root =>
      val v = root.getVector("f32").asInstanceOf[Float4Vector]
      v.get(0) shouldBe 1.5f
      v.isNull(1) shouldBe true
      v.get(2) shouldBe -3.25f
      v.isNull(3) shouldBe true
      v.get(4) shouldBe Float.PositiveInfinity
      java.lang.Float.isNaN(v.get(5)) shouldBe true
    }
  }

  test("Float64 with NaN / infinities and nulls: round-trip preserves bits") {
    val field = new Field(
      "f64",
      FieldType.nullable(
        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      ),
      null
    )
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("f64").asInstanceOf[Float8Vector]
        v.allocateNew(6)
        v.setSafe(0, 1.25)
        v.setNull(1)
        v.setSafe(2, -0.0)
        v.setSafe(3, Double.PositiveInfinity)
        v.setSafe(4, Double.NegativeInfinity)
        v.setSafe(5, Double.NaN)
        v.setValueCount(6)
      },
      rowCount = 6
    ) { root =>
      val v = root.getVector("f64").asInstanceOf[Float8Vector]
      v.get(0) shouldBe 1.25
      v.isNull(1) shouldBe true
      java.lang.Double.doubleToRawLongBits(v.get(2)) shouldBe
        java.lang.Double.doubleToRawLongBits(-0.0)
      v.get(3) shouldBe Double.PositiveInfinity
      v.get(4) shouldBe Double.NegativeInfinity
      java.lang.Double.isNaN(v.get(5)) shouldBe true
    }
  }

  test("Int16 (ShortType) boundary values round-trip") {
    val field =
      new Field("i16", FieldType.nullable(new ArrowType.Int(16, true)), null)
    val values: Seq[java.lang.Short] =
      Seq[Short](0, 1, -1, Short.MaxValue, Short.MinValue, 42).map(
        java.lang.Short.valueOf
      )
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("i16").asInstanceOf[SmallIntVector]
        v.allocateNew(values.length + 1)
        values.zipWithIndex.foreach { case (x, i) =>
          v.setSafe(i, x.shortValue)
        }
        v.setNull(values.length)
        v.setValueCount(values.length + 1)
      },
      rowCount = values.length + 1
    ) { root =>
      val v = root.getVector("i16").asInstanceOf[SmallIntVector]
      values.zipWithIndex.foreach { case (x, i) =>
        v.get(i) shouldBe x.shortValue
      }
      v.isNull(values.length) shouldBe true
    }
  }

  test("Int8 (ByteType) boundary values round-trip") {
    val field =
      new Field("i8", FieldType.nullable(new ArrowType.Int(8, true)), null)
    val values: Seq[Byte] =
      Seq[Byte](0, 1, -1, Byte.MaxValue, Byte.MinValue, 42)
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("i8").asInstanceOf[TinyIntVector]
        v.allocateNew(values.length + 1)
        values.zipWithIndex.foreach { case (x, i) => v.setSafe(i, x) }
        v.setNull(values.length)
        v.setValueCount(values.length + 1)
      },
      rowCount = values.length + 1
    ) { root =>
      val v = root.getVector("i8").asInstanceOf[TinyIntVector]
      values.zipWithIndex.foreach { case (x, i) => v.get(i) shouldBe x }
      v.isNull(values.length) shouldBe true
    }
  }

  test("Boolean: bit-packed buffer round-trips") {
    val field =
      new Field("b", FieldType.nullable(new ArrowType.Bool()), null)
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("b").asInstanceOf[BitVector]
        v.allocateNew(8)
        v.setSafe(0, 1)
        v.setSafe(1, 0)
        v.setNull(2)
        v.setSafe(3, 1)
        v.setSafe(4, 1)
        v.setSafe(5, 0)
        v.setSafe(6, 0)
        v.setSafe(7, 1)
        v.setValueCount(8)
      },
      rowCount = 8
    ) { root =>
      val v = root.getVector("b").asInstanceOf[BitVector]
      v.get(0) shouldBe 1
      v.get(1) shouldBe 0
      v.isNull(2) shouldBe true
      v.get(3) shouldBe 1
      v.get(4) shouldBe 1
      v.get(5) shouldBe 0
      v.get(6) shouldBe 0
      v.get(7) shouldBe 1
    }
  }

  // --------------------------------------------------------------- var-width

  test(
    "Utf8 VarChar with empty / null / multi-byte values: offsets + data survive"
  ) {
    val field = new Field("s", FieldType.nullable(new ArrowType.Utf8()), null)
    val payloads =
      Seq("foo", "", "日本語", null, "a longer string used to grow the buffer")
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("s").asInstanceOf[VarCharVector]
        v.allocateNew(1024L, payloads.size)
        payloads.zipWithIndex.foreach {
          case (null, i) => v.setNull(i)
          case (s, i) =>
            v.setSafe(i, s.getBytes(StandardCharsets.UTF_8))
        }
        v.setValueCount(payloads.size)
      },
      rowCount = payloads.size
    ) { root =>
      val v = root.getVector("s").asInstanceOf[VarCharVector]
      payloads.zipWithIndex.foreach {
        case (null, i) => v.isNull(i) shouldBe true
        case (s, i) =>
          new String(v.get(i), StandardCharsets.UTF_8) shouldBe s
      }
    }
  }

  test("Binary VarBinary preserves byte payloads including empty") {
    val field =
      new Field("bin", FieldType.nullable(new ArrowType.Binary()), null)
    val payloads: Seq[Array[Byte]] = Seq(
      Array[Byte](1, 2, 3),
      Array[Byte](),
      null,
      Array.fill[Byte](256)(0x7f.toByte),
      Array[Byte](0, 0, 0, 1)
    )
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("bin").asInstanceOf[VarBinaryVector]
        v.allocateNew(1024L, payloads.size)
        payloads.zipWithIndex.foreach {
          case (null, i)  => v.setNull(i)
          case (bytes, i) => v.setSafe(i, bytes)
        }
        v.setValueCount(payloads.size)
      },
      rowCount = payloads.size
    ) { root =>
      val v = root.getVector("bin").asInstanceOf[VarBinaryVector]
      payloads.zipWithIndex.foreach {
        case (null, i) => v.isNull(i) shouldBe true
        case (bytes, i) =>
          v.get(i) shouldBe bytes
      }
    }
  }

  test("FixedSizeBinary: fixed-length byte payloads round-trip") {
    val field =
      new Field(
        "fsb",
        FieldType.nullable(new ArrowType.FixedSizeBinary(8)),
        null
      )
    val payloads: Seq[Array[Byte]] = Seq(
      Array[Byte](0, 1, 2, 3, 4, 5, 6, 7),
      null,
      Array.fill[Byte](8)(0xff.toByte),
      Array[Byte](8, 7, 6, 5, 4, 3, 2, 1)
    )
    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("fsb").asInstanceOf[FixedSizeBinaryVector]
        v.allocateNew(payloads.size)
        payloads.zipWithIndex.foreach {
          case (null, i)  => v.setNull(i)
          case (bytes, i) => v.setSafe(i, bytes)
        }
        v.setValueCount(payloads.size)
      },
      rowCount = payloads.size
    ) { root =>
      val v = root.getVector("fsb").asInstanceOf[FixedSizeBinaryVector]
      payloads.zipWithIndex.foreach {
        case (null, i) => v.isNull(i) shouldBe true
        case (bytes, i) =>
          v.getObject(i) shouldBe bytes
      }
    }
  }

  // --------------------------------------------------------------- containers

  test("FixedSizeList<Float> (vector embedding) round-trips") {
    val listType = new ArrowType.FixedSizeList(4)
    val child = new Field(
      "$data$",
      FieldType.nullable(
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      ),
      null
    )
    val field =
      new Field("vec", FieldType.nullable(listType), java.util.List.of(child))

    val rows: Seq[Array[Float]] = Seq(
      Array(1.0f, 2.0f, 3.0f, 4.0f),
      Array(Float.NaN, 0.0f, -0.0f, Float.MinValue),
      null,
      Array(1e10f, 1e-10f, -1e10f, 1.5f)
    )

    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("vec").asInstanceOf[FixedSizeListVector]
        val inner = v.getDataVector.asInstanceOf[Float4Vector]
        v.allocateNew()
        inner.allocateNew(rows.length * 4)
        rows.zipWithIndex.foreach {
          case (null, i) => v.setNull(i)
          case (arr, i) =>
            v.setNotNull(i)
            (0 until 4).foreach { k =>
              inner.setSafe(i * 4 + k, arr(k))
            }
        }
        inner.setValueCount(rows.length * 4)
        v.setValueCount(rows.length)
      },
      rowCount = rows.length
    ) { root =>
      val v = root.getVector("vec").asInstanceOf[FixedSizeListVector]
      val inner = v.getDataVector.asInstanceOf[Float4Vector]
      rows.zipWithIndex.foreach {
        case (null, i) => v.isNull(i) shouldBe true
        case (arr, i) =>
          (0 until 4).foreach { k =>
            val got = inner.get(i * 4 + k)
            if (java.lang.Float.isNaN(arr(k)))
              java.lang.Float.isNaN(got) shouldBe true
            else got shouldBe arr(k)
          }
      }
    }
  }

  test(
    "List<Int> (variable-length array) round-trips with nulls and empty lists"
  ) {
    val listType = new ArrowType.List()
    val child = new Field(
      "$data$",
      FieldType.nullable(new ArrowType.Int(32, true)),
      null
    )
    val field =
      new Field("xs", FieldType.nullable(listType), java.util.List.of(child))

    val rows: Seq[Seq[Int]] =
      Seq(Seq(1, 2, 3), Seq.empty, null, Seq(42), Seq(-1, -2, -3, -4, -5))

    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("xs").asInstanceOf[ListVector]
        val inner = v.getDataVector.asInstanceOf[IntVector]
        inner.allocateNew(64)
        v.allocateNew()

        var flat = 0
        rows.zipWithIndex.foreach {
          case (null, i) =>
            v.setNull(i)
          case (xs, i) =>
            v.startNewValue(i)
            xs.foreach { x =>
              inner.setSafe(flat, x)
              flat += 1
            }
            v.endValue(i, xs.length)
        }
        inner.setValueCount(flat)
        v.setValueCount(rows.length)
      },
      rowCount = rows.length
    ) { root =>
      val v = root.getVector("xs").asInstanceOf[ListVector]
      val inner = v.getDataVector.asInstanceOf[IntVector]
      rows.zipWithIndex.foreach {
        case (null, i) => v.isNull(i) shouldBe true
        case (xs, i) =>
          v.isNull(i) shouldBe false
          val start = v.getOffsetBuffer.getInt(i.toLong * 4)
          val end = v.getOffsetBuffer.getInt((i.toLong + 1) * 4)
          (end - start) shouldBe xs.length
          (0 until xs.length).foreach { k =>
            inner.get(start + k) shouldBe xs(k)
          }
      }
    }
  }

  test(
    "List<Utf8>: list with a variable-width child preserves offsets and data"
  ) {
    val listType = new ArrowType.List()
    val child =
      new Field("$data$", FieldType.nullable(new ArrowType.Utf8()), null)
    val field =
      new Field("ls", FieldType.nullable(listType), java.util.List.of(child))

    val rows: Seq[Seq[String]] = Seq(
      Seq("a", "bb", "ccc"),
      Seq.empty,
      null,
      Seq("日本語", ""),
      Seq("a longer string used to grow the child buffer")
    )

    roundTrip(
      field,
      populate = { root =>
        val v = root.getVector("ls").asInstanceOf[ListVector]
        val inner = v.getDataVector.asInstanceOf[VarCharVector]
        inner.allocateNew(1024L, 16)
        v.allocateNew()

        var flat = 0
        rows.zipWithIndex.foreach {
          case (null, i) =>
            v.setNull(i)
          case (xs, i) =>
            v.startNewValue(i)
            xs.foreach { s =>
              inner.setSafe(flat, s.getBytes(StandardCharsets.UTF_8))
              flat += 1
            }
            v.endValue(i, xs.length)
        }
        inner.setValueCount(flat)
        v.setValueCount(rows.length)
      },
      rowCount = rows.length
    ) { root =>
      val v = root.getVector("ls").asInstanceOf[ListVector]
      val inner = v.getDataVector.asInstanceOf[VarCharVector]
      rows.zipWithIndex.foreach {
        case (null, i) => v.isNull(i) shouldBe true
        case (xs, i) =>
          v.isNull(i) shouldBe false
          val start = v.getOffsetBuffer.getInt(i.toLong * 4)
          val end = v.getOffsetBuffer.getInt((i.toLong + 1) * 4)
          (end - start) shouldBe xs.length
          xs.zipWithIndex.foreach { case (s, k) =>
            new String(inner.get(start + k), StandardCharsets.UTF_8) shouldBe s
          }
      }
    }
  }

  test(
    "Struct<Int64, Utf8>: nested fields preserve buffers independently"
  ) {
    val children = java.util.List.of(
      new Field("k", FieldType.notNullable(new ArrowType.Int(64, true)), null),
      new Field("v", FieldType.nullable(new ArrowType.Utf8()), null)
    )
    val field =
      new Field("s", FieldType.nullable(new ArrowType.Struct()), children)

    val rows: Seq[(Long, String)] = Seq(
      (100L, "alpha"),
      (200L, null),
      (300L, "日本語"),
      (400L, "")
    )

    roundTrip(
      field,
      populate = { root =>
        val sv = root.getVector("s").asInstanceOf[StructVector]
        sv.allocateNew()
        val kv = sv.getChild("k", classOf[BigIntVector])
        val vv = sv.getChild("v", classOf[VarCharVector])

        rows.zipWithIndex.foreach { case ((k, s), i) =>
          sv.setIndexDefined(i)
          kv.setSafe(i, k)
          if (s == null) vv.setNull(i)
          else vv.setSafe(i, s.getBytes(StandardCharsets.UTF_8))
        }
        kv.setValueCount(rows.length)
        vv.setValueCount(rows.length)
        sv.setValueCount(rows.length)
      },
      rowCount = rows.length
    ) { root =>
      val sv = root.getVector("s").asInstanceOf[StructVector]
      val kv = sv.getChild("k", classOf[BigIntVector])
      val vv = sv.getChild("v", classOf[VarCharVector])
      rows.zipWithIndex.foreach { case ((k, s), i) =>
        sv.isNull(i) shouldBe false
        kv.get(i) shouldBe k
        if (s == null) vv.isNull(i) shouldBe true
        else new String(vv.get(i), StandardCharsets.UTF_8) shouldBe s
      }
    }
  }

  // --------------------------------------------------------- multi-batch path

  test(
    "multi-batch: close each source root immediately — all exports remain readable"
  ) {
    // Mirrors the writer's flushBatch loop: export batch, close source root,
    // verify the exported cArray still imports cleanly and data is intact.
    // Running sequentially (export + close + import per batch) matches the
    // sub-batch flush semantics in the real writer and keeps the test's own
    // resource management trivial.
    val parent = new RootAllocator(Long.MaxValue)
    try {
      val alloc = parent.newChildAllocator("multi", 0, Long.MaxValue)
      val field =
        new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)
      val schema = new Schema(java.util.List.of(field))

      val numBatches = 20
      val rowsPerBatch = 32

      (0 until numBatches).foreach { b =>
        val root = VectorSchemaRoot.create(schema, alloc)
        val v = root.getVector("v").asInstanceOf[BigIntVector]
        v.allocateNew(rowsPerBatch)
        (0 until rowsPerBatch).foreach { i =>
          v.setSafe(i, (b * rowsPerBatch + i).toLong)
        }
        v.setValueCount(rowsPerBatch)
        root.setRowCount(rowsPerBatch)

        val cSchema = ArrowSchema.allocateNew(alloc)
        val cArray = ArrowArray.allocateNew(alloc)
        try {
          Data.exportSchema(alloc, schema, null, cSchema)
          Data.exportVectorSchemaRoot(alloc, root, null, cArray)
          // Aggressive close: source root goes away immediately. If the export
          // did not retain buffers, the subsequent import would see garbage or
          // SIGSEGV.
          root.close()
          alloc.getAllocatedMemory should be > 0L

          val provider = new CDataDictionaryProvider()
          val imported =
            Data.importVectorSchemaRoot(alloc, cArray, cSchema, provider)
          try {
            val iv = imported.getVector("v").asInstanceOf[BigIntVector]
            (0 until rowsPerBatch).foreach { i =>
              iv.get(i) shouldBe (b * rowsPerBatch + i).toLong
            }
          } finally {
            imported.close()
            provider.close()
          }
        } finally {
          cArray.close()
          cSchema.close()
        }

        alloc.getAllocatedMemory shouldBe 0L
      }

      alloc.close()
    } finally parent.close()
  }

  test(
    "concurrent holders: multiple imports outlive source — all see intact data"
  ) {
    // Simulates the real writer scenario more precisely: source root closes
    // immediately; the exported cArray is NOT imported right away. Multiple
    // batches' exports coexist in memory simultaneously (bounded by the C++
    // writer's 16 MB threshold, not by the source roots' lifetimes).
    val parent = new RootAllocator(Long.MaxValue)
    try {
      val alloc = parent.newChildAllocator("concurrent", 0, Long.MaxValue)
      val field =
        new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)
      val schema = new Schema(java.util.List.of(field))

      val numBatches = 8
      val rowsPerBatch = 16

      case class Held(cSchema: ArrowSchema, cArray: ArrowArray)

      // Export every batch; close each source root right after export.
      val held = (0 until numBatches).map { b =>
        val root = VectorSchemaRoot.create(schema, alloc)
        val v = root.getVector("v").asInstanceOf[BigIntVector]
        v.allocateNew(rowsPerBatch)
        (0 until rowsPerBatch).foreach(i =>
          v.setSafe(i, (b * rowsPerBatch + i).toLong)
        )
        v.setValueCount(rowsPerBatch)
        root.setRowCount(rowsPerBatch)

        val cSchema = ArrowSchema.allocateNew(alloc)
        val cArray = ArrowArray.allocateNew(alloc)
        Data.exportSchema(alloc, schema, null, cSchema)
        Data.exportVectorSchemaRoot(alloc, root, null, cArray)
        root.close()
        Held(cSchema, cArray)
      }

      // All sources closed; exports keep buffers alive independently.
      alloc.getAllocatedMemory should be > 0L

      // Import every batch in order and verify — proves each export's refcount
      // independently survived the source root's close.
      held.zipWithIndex.foreach { case (h, b) =>
        val provider = new CDataDictionaryProvider()
        try {
          val imported =
            Data.importVectorSchemaRoot(alloc, h.cArray, h.cSchema, provider)
          try {
            val iv = imported.getVector("v").asInstanceOf[BigIntVector]
            (0 until rowsPerBatch).foreach(i =>
              iv.get(i) shouldBe (b * rowsPerBatch + i).toLong
            )
          } finally imported.close()
        } finally {
          provider.close()
          h.cArray.close()
          h.cSchema.close()
        }
      }

      alloc.getAllocatedMemory shouldBe 0L
      alloc.close()
    } finally parent.close()
  }

  // -------------------------------------------------- combined row all-types

  test(
    "combined schema with all backfill-relevant types round-trips in one export"
  ) {
    val fields = java.util.List.of(
      new Field(
        "pk",
        FieldType.notNullable(new ArrowType.Int(64, true)),
        null
      ),
      new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("blob", FieldType.nullable(new ArrowType.Binary()), null),
      new Field(
        "hash",
        FieldType.nullable(new ArrowType.FixedSizeBinary(4)),
        null
      ),
      new Field(
        "flag",
        FieldType.nullable(new ArrowType.Bool()),
        null
      ),
      new Field(
        "vec",
        FieldType.nullable(new ArrowType.FixedSizeList(2)),
        java.util.List.of(
          new Field(
            "$data$",
            FieldType.nullable(
              new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
            ),
            null
          )
        )
      )
    )
    val schema = new Schema(fields)
    val parent = new RootAllocator(Long.MaxValue)
    try {
      val alloc = parent.newChildAllocator("mixed", 0, Long.MaxValue)
      val root = VectorSchemaRoot.create(schema, alloc)

      val pk = root.getVector("pk").asInstanceOf[BigIntVector]
      val name = root.getVector("name").asInstanceOf[VarCharVector]
      val blob = root.getVector("blob").asInstanceOf[VarBinaryVector]
      val hash = root.getVector("hash").asInstanceOf[FixedSizeBinaryVector]
      val flag = root.getVector("flag").asInstanceOf[BitVector]
      val vec = root.getVector("vec").asInstanceOf[FixedSizeListVector]
      val vecInner = vec.getDataVector.asInstanceOf[Float4Vector]

      val n = 3
      pk.allocateNew(n)
      name.allocateNew(1024L, n)
      blob.allocateNew(1024L, n)
      hash.allocateNew(n)
      flag.allocateNew(n)
      vec.allocateNew()
      vecInner.allocateNew(n * 2)

      pk.setSafe(0, 100L); pk.setSafe(1, 200L); pk.setSafe(2, 300L)
      name.setSafe(0, "a".getBytes(StandardCharsets.UTF_8))
      name.setNull(1)
      name.setSafe(2, "xyz".getBytes(StandardCharsets.UTF_8))
      blob.setSafe(0, Array[Byte](0x01.toByte, 0x02.toByte))
      blob.setNull(1)
      blob.setSafe(2, Array[Byte]())
      hash.setSafe(
        0,
        Array[Byte](0x00.toByte, 0x11.toByte, 0x22.toByte, 0x33.toByte)
      )
      hash.setSafe(
        1,
        Array[Byte](0xaa.toByte, 0xbb.toByte, 0xcc.toByte, 0xdd.toByte)
      )
      hash.setNull(2)
      flag.setSafe(0, 1); flag.setNull(1); flag.setSafe(2, 0)
      (0 until n).foreach { i =>
        vec.setNotNull(i)
        vecInner.setSafe(i * 2, i.toFloat)
        vecInner.setSafe(i * 2 + 1, (i + 0.5).toFloat)
      }

      pk.setValueCount(n)
      name.setValueCount(n)
      blob.setValueCount(n)
      hash.setValueCount(n)
      flag.setValueCount(n)
      vecInner.setValueCount(n * 2)
      vec.setValueCount(n)
      root.setRowCount(n)

      val cSchema = ArrowSchema.allocateNew(alloc)
      val cArray = ArrowArray.allocateNew(alloc)
      try {
        Data.exportSchema(alloc, schema, null, cSchema)
        Data.exportVectorSchemaRoot(alloc, root, null, cArray)

        root.close()
        alloc.getAllocatedMemory should be > 0L

        val provider = new CDataDictionaryProvider()
        val imported =
          Data.importVectorSchemaRoot(alloc, cArray, cSchema, provider)
        try {
          val ipk = imported.getVector("pk").asInstanceOf[BigIntVector]
          val iname = imported.getVector("name").asInstanceOf[VarCharVector]
          val iblob = imported.getVector("blob").asInstanceOf[VarBinaryVector]
          val ihash =
            imported.getVector("hash").asInstanceOf[FixedSizeBinaryVector]
          val iflag = imported.getVector("flag").asInstanceOf[BitVector]
          val ivec = imported.getVector("vec").asInstanceOf[FixedSizeListVector]
          val ivecInner = ivec.getDataVector.asInstanceOf[Float4Vector]

          ipk.get(0) shouldBe 100L
          ipk.get(1) shouldBe 200L
          ipk.get(2) shouldBe 300L
          new String(iname.get(0), StandardCharsets.UTF_8) shouldBe "a"
          iname.isNull(1) shouldBe true
          new String(iname.get(2), StandardCharsets.UTF_8) shouldBe "xyz"
          iblob.get(0) shouldBe Array[Byte](0x01.toByte, 0x02.toByte)
          iblob.isNull(1) shouldBe true
          iblob.get(2).length shouldBe 0
          ihash.getObject(0) shouldBe Array[Byte](
            0x00.toByte,
            0x11.toByte,
            0x22.toByte,
            0x33.toByte
          )
          ihash.getObject(1) shouldBe Array[Byte](
            0xaa.toByte,
            0xbb.toByte,
            0xcc.toByte,
            0xdd.toByte
          )
          ihash.isNull(2) shouldBe true
          iflag.get(0) shouldBe 1
          iflag.isNull(1) shouldBe true
          iflag.get(2) shouldBe 0
          (0 until n).foreach { i =>
            ivecInner.get(i * 2) shouldBe i.toFloat
            ivecInner.get(i * 2 + 1) shouldBe (i + 0.5).toFloat
          }
        } finally {
          imported.close()
          provider.close()
        }
        alloc.getAllocatedMemory shouldBe 0L
      } finally {
        cArray.close()
        cSchema.close()
        alloc.close()
      }
    } finally parent.close()
  }
}
