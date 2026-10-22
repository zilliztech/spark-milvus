package com.zilliz.spark.connector.types

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.FixedSizeBinaryVector
import org.apache.spark.sql.types.{ArrayType, BinaryType, FloatType, ShortType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.FloatConverter
import io.milvus.grpc.schema.DataType

/** The columnar side of decision 6.
  *
  * Every case checks against `FloatConverter`, which is what the row path uses,
  * so the two paths cannot drift: the same stored bytes have to read back as
  * the same values whichever one the scan took.
  */
class MilvusVectorColumnTest extends AnyFunSuite with Matchers {

  private def withVector(byteWidth: Int, rows: Seq[Array[Byte]])(
      body: FixedSizeBinaryVector => Unit
  ): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new FixedSizeBinaryVector("v", allocator, byteWidth)
    try {
      vector.allocateNew(rows.size)
      rows.zipWithIndex.foreach { case (bytes, i) => vector.set(i, bytes) }
      vector.setValueCount(rows.size)
      body(vector)
    } finally {
      vector.close()
      allocator.close()
    }
  }

  private def floatBytes(values: Seq[Float]): Array[Byte] =
    values.flatMap(FloatConverter.toFloatBytes).toArray

  test("a float vector reads back element by element") {
    val a = Seq(1.5f, -2.25f, 0.0f, 1e10f)
    val b = Seq(3.5f, 4.5f, -5.5f, 6.5f)
    withVector(16, Seq(floatBytes(a), floatBytes(b))) { vector =>
      val column =
        MilvusVectorColumn(
          vector,
          DataType.FloatVector,
          dimension = 4,
          raw = false
        )
      column.dataType shouldBe ArrayType(FloatType, containsNull = false)

      val first = column.getArray(0)
      first.numElements() shouldBe 4
      (0 until 4).map(first.getFloat) shouldBe a
      val second = column.getArray(1)
      (0 until 4).map(second.getFloat) shouldBe b
    }
  }

  test("a half-precision vector decodes the same way the row path does") {
    val values = Seq(1.0f, -2.5f, 0.0f, 65504.0f)
    val bytes = values.flatMap(FloatConverter.toFloat16Bytes).toArray
    withVector(8, Seq(bytes)) { vector =>
      val column =
        MilvusVectorColumn(
          vector,
          DataType.Float16Vector,
          dimension = 4,
          raw = false
        )
      val row = column.getArray(0)
      (0 until 4).map(row.getFloat) shouldBe
        values.map(v =>
          FloatConverter.fromFloat16Bytes(FloatConverter.toFloat16Bytes(v))
        )
    }
  }

  test("a bfloat16 vector decodes the same way the row path does") {
    val values = Seq(1.0f, -2.5f, 0.0f, 100.0f)
    val bytes = values.flatMap(FloatConverter.toBFloat16Bytes).toArray
    withVector(8, Seq(bytes)) { vector =>
      val column =
        MilvusVectorColumn(
          vector,
          DataType.BFloat16Vector,
          dimension = 4,
          raw = false
        )
      val row = column.getArray(0)
      (0 until 4).map(row.getFloat) shouldBe
        values.map(v =>
          FloatConverter.fromBFloat16Bytes(FloatConverter.toBFloat16Bytes(v))
        )
    }
  }

  // Signed, and Spark has no byte array type that survives the round trip, so
  // the row path widens to Short and this has to agree.
  test("an int8 vector widens to short, sign and all") {
    val bytes = Array[Byte](127, -128, 0, -1)
    withVector(4, Seq(bytes)) { vector =>
      val column =
        MilvusVectorColumn(
          vector,
          DataType.Int8Vector,
          dimension = 4,
          raw = false
        )
      column.dataType shouldBe ArrayType(ShortType, containsNull = false)
      val row = column.getArray(0)
      (0 until 4).map(row.getShort) shouldBe Seq[Short](127, -128, 0, -1)
    }
  }

  test("a binary vector stays binary") {
    val bytes = Array[Byte](1, 2, 3, 4)
    withVector(4, Seq(bytes)) { vector =>
      val column =
        MilvusVectorColumn(
          vector,
          DataType.BinaryVector,
          dimension = 32,
          raw = false
        )
      column.dataType shouldBe BinaryType
      column.getBinary(0) shouldBe bytes
    }
  }

  // The point of milvus.read.vector.raw: the stored bytes, undecoded.
  test("raw mode hands over the stored bytes whatever the vector type is") {
    val values = Seq(1.5f, -2.25f)
    val bytes = floatBytes(values)
    withVector(8, Seq(bytes)) { vector =>
      val column =
        MilvusVectorColumn(
          vector,
          DataType.FloatVector,
          dimension = 2,
          raw = true
        )
      column.dataType shouldBe BinaryType
      column.getBinary(0) shouldBe bytes
    }
  }

  // A dimension that disagrees with the stored width means the schema and the
  // data describe different columns. Reading on would hand back vectors that
  // are silently wrong, which is worse than refusing.
  test("a dimension that does not match the stored width is refused") {
    withVector(16, Seq(floatBytes(Seq(1f, 2f, 3f, 4f)))) { vector =>
      val err = intercept[IllegalArgumentException] {
        MilvusVectorColumn(
          vector,
          DataType.FloatVector,
          dimension = 8,
          raw = false
        )
      }
      err.getMessage should include("32 bytes a row")
      err.getMessage should include("16")
    }
  }

  test("a null row is null, and its neighbours still read") {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new FixedSizeBinaryVector("v", allocator, 8)
    try {
      vector.allocateNew(2)
      vector.setNull(0)
      vector.set(1, floatBytes(Seq(7.5f, 8.5f)))
      vector.setValueCount(2)

      val column =
        MilvusVectorColumn(
          vector,
          DataType.FloatVector,
          dimension = 2,
          raw = false
        )
      column.hasNull shouldBe true
      column.numNulls shouldBe 1
      column.isNullAt(0) shouldBe true
      column.isNullAt(1) shouldBe false
      val row = column.getArray(1)
      (0 until 2).map(row.getFloat) shouldBe Seq(7.5f, 8.5f)
    } finally {
      vector.close()
      allocator.close()
    }
  }

  test("a non-vector type is refused rather than guessed at") {
    withVector(4, Seq(Array[Byte](1, 2, 3, 4))) { vector =>
      an[IllegalArgumentException] should be thrownBy
        MilvusVectorColumn(vector, DataType.Int64, dimension = 1, raw = false)
    }
  }

  test("the presented Spark type follows decision 6") {
    MilvusVectorColumn.sparkType(DataType.FloatVector, raw = false) shouldBe
      ArrayType(FloatType, containsNull = false)
    MilvusVectorColumn.sparkType(DataType.Float16Vector, raw = false) shouldBe
      ArrayType(FloatType, containsNull = false)
    MilvusVectorColumn.sparkType(DataType.BFloat16Vector, raw = false) shouldBe
      ArrayType(FloatType, containsNull = false)
    MilvusVectorColumn.sparkType(DataType.Int8Vector, raw = false) shouldBe
      ArrayType(ShortType, containsNull = false)
    MilvusVectorColumn.sparkType(DataType.BinaryVector, raw = false) shouldBe
      BinaryType
    MilvusVectorColumn.sparkType(DataType.FloatVector, raw = true) shouldBe
      BinaryType
  }
}
