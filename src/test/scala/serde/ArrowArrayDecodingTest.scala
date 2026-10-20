package com.zilliz.spark.connector.serde

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.{DataTypeUtil, MilvusSchemaUtil}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  BoolArray,
  CollectionSchema,
  DataType => MilvusDataType,
  DoubleArray,
  FieldSchema,
  FloatArray,
  IntArray,
  LongArray,
  ScalarField,
  StringArray
}

class ArrowArrayDecodingTest extends AnyFunSuite with Matchers {
  private case class Fixture(
      elementType: MilvusDataType,
      payload: ScalarField,
      empty: ScalarField,
      expected: Seq[Any]
  )

  private val fixtures = Seq(
    Fixture(
      MilvusDataType.Bool,
      ScalarField().withBoolData(BoolArray(Seq(true, false, true))),
      ScalarField().withBoolData(BoolArray()),
      Seq(true, false, true)
    ),
    Fixture(
      MilvusDataType.Int8,
      ScalarField().withIntData(IntArray(Seq(-128, 0, 127))),
      ScalarField().withIntData(IntArray()),
      Seq[Short](-128, 0, 127)
    ),
    Fixture(
      MilvusDataType.Int16,
      ScalarField().withIntData(IntArray(Seq(-32768, 0, 32767))),
      ScalarField().withIntData(IntArray()),
      Seq[Short](-32768, 0, 32767)
    ),
    Fixture(
      MilvusDataType.Int32,
      ScalarField().withIntData(IntArray(Seq(Int.MinValue, 0, Int.MaxValue))),
      ScalarField().withIntData(IntArray()),
      Seq(Int.MinValue, 0, Int.MaxValue)
    ),
    Fixture(
      MilvusDataType.Int64,
      ScalarField().withLongData(
        LongArray(Seq(Long.MinValue, 0L, Long.MaxValue))
      ),
      ScalarField().withLongData(LongArray()),
      Seq(Long.MinValue, 0L, Long.MaxValue)
    ),
    Fixture(
      MilvusDataType.Float,
      ScalarField().withFloatData(FloatArray(Seq(-1.5f, 0.0f, 2.25f))),
      ScalarField().withFloatData(FloatArray()),
      Seq(-1.5f, 0.0f, 2.25f)
    ),
    Fixture(
      MilvusDataType.Double,
      ScalarField().withDoubleData(DoubleArray(Seq(-1.5d, 0.0d, 2.25d))),
      ScalarField().withDoubleData(DoubleArray()),
      Seq(-1.5d, 0.0d, 2.25d)
    )
  ) ++ Seq(MilvusDataType.VarChar, MilvusDataType.String).map { elementType =>
    val strings =
      Seq("spring", "", "夏天", "repeat", "repeat", "a\u0000b", "\ud83d\ude80")
    Fixture(
      elementType,
      ScalarField().withStringData(StringArray(strings)),
      ScalarField().withStringData(StringArray()),
      strings.map(UTF8String.fromString)
    )
  }

  private def sparkField(elementType: MilvusDataType): StructField = {
    val field = FieldSchema(
      name = "values",
      fieldID = 104,
      dataType = MilvusDataType.Array,
      elementType = elementType,
      nullable = true
    )
    StructField(
      field.name,
      DataTypeUtil.toDataType(field),
      nullable = true,
      DataTypeUtil.metadata(field)
    )
  }

  private def withRoot(
      schema: Schema
  )(check: VectorSchemaRoot => Unit): Unit = {
    val allocator = new RootAllocator(16 * 1024 * 1024)
    try {
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        root.allocateNew()
        check(root)
      } finally root.close()
    } finally allocator.close()
  }

  private def withBinary(
      payloads: Seq[Option[Array[Byte]]]
  )(check: VectorSchemaRoot => Unit): Unit = {
    val schema = new Schema(
      Seq(
        new Field("104", FieldType.nullable(new ArrowType.Binary()), null)
      ).asJava
    )
    withRoot(schema) { root =>
      val vector = root.getVector("104").asInstanceOf[VarBinaryVector]
      payloads.zipWithIndex.foreach { case (payload, index) =>
        payload match {
          case Some(bytes) => vector.setSafe(index, bytes)
          case None        => vector.setNull(index)
        }
      }
      root.setRowCount(payloads.size)
      check(root)
    }
  }

  private def read(
      root: VectorSchemaRoot,
      field: StructField,
      row: Int = 0
  ): InternalRow =
    ArrowConverter.arrowToInternalRow(
      root,
      row,
      StructType(Seq(field)),
      Map(field.name -> "104")
    )

  private def values(array: ArrayData, elementType: DataType): Seq[Any] =
    (0 until array.numElements()).map { index =>
      if (array.isNullAt(index)) null else array.get(index, elementType)
    }

  fixtures.foreach { fixture =>
    val field = sparkField(fixture.elementType)
    val elementType = field.dataType.asInstanceOf[ArrayType].elementType

    test(
      s"decode stored ${fixture.elementType} arrays with schema and field ID mapping"
    ) {
      withBinary(Seq(Some(fixture.payload.toByteArray))) { root =>
        values(
          read(root, field).getArray(0),
          elementType
        ) shouldBe fixture.expected
      }
    }

    test(
      s"distinguish null, typed and untyped empty ${fixture.elementType} arrays across rows"
    ) {
      withBinary(
        Seq(
          Some(fixture.payload.toByteArray),
          None,
          Some(fixture.empty.toByteArray),
          Some(fixture.payload.toByteArray),
          Some(Array.emptyByteArray)
        )
      ) { root =>
        values(
          read(root, field, 0).getArray(0),
          elementType
        ) shouldBe fixture.expected
        read(root, field, 1).isNullAt(0) shouldBe true
        read(root, field, 2).getArray(0).numElements() shouldBe 0
        values(
          read(root, field, 3).getArray(0),
          elementType
        ) shouldBe fixture.expected
        read(root, field, 4).getArray(0).numElements() shouldBe 0
      }
    }

    test(
      s"retain Arrow List decoding for ${fixture.elementType} with and without metadata"
    ) {
      // The existing Spark List writer is an independent source of list data.
      val plainField = field.copy(metadata = Metadata.empty)
      val schema = StructType(Seq(plainField))
      // Preserve the existing Int8 array -> Spark ShortType mapping.
      val listElementType =
        if (fixture.elementType == MilvusDataType.Int8) MilvusDataType.Int16
        else fixture.elementType
      val arrowSchema = new Schema(
        Seq(
          new Field(
            field.name,
            FieldType.nullable(new ArrowType.List()),
            Seq(
              new Field(
                "element",
                FieldType.nullable(
                  DataTypeUtil.toArrowType(0, listElementType)
                ),
                null
              )
            ).asJava
          )
        ).asJava
      )
      withRoot(arrowSchema) { root =>
        val input = ArrayData.toArrayData((fixture.expected :+ null).toArray)
        ArrowConverter.internalRowToArrow(root, 0, InternalRow(input), schema)
        ArrowConverter.internalRowToArrow(
          root,
          1,
          InternalRow(ArrayData.toArrayData(Array.empty[Any])),
          schema
        )
        ArrowConverter.internalRowToArrow(
          root,
          2,
          InternalRow.fromSeq(Seq(null)),
          schema
        )
        root.setRowCount(3)
        Seq(plainField, field).foreach { projected =>
          val projectedSchema = StructType(Seq(projected))
          val actual = ArrowConverter
            .arrowToInternalRow(root, 0, projectedSchema)
            .getArray(0)
          values(actual, elementType) shouldBe (fixture.expected :+ null)
          ArrowConverter
            .arrowToInternalRow(root, 1, projectedSchema)
            .getArray(0)
            .numElements() shouldBe 0
          ArrowConverter
            .arrowToInternalRow(root, 2, projectedSchema)
            .isNullAt(0) shouldBe true
        }
      }
    }

    test(
      s"reject mismatched oneof including empty payloads for ${fixture.elementType}"
    ) {
      fixtures
        .filterNot(_.payload.data.getClass == fixture.payload.data.getClass)
        .foreach { other =>
          Seq(other.payload, other.empty).foreach { payload =>
            withBinary(Seq(Some(payload.toByteArray))) { root =>
              intercept[IllegalArgumentException](
                read(root, field)
              ).getMessage should include("incompatible ScalarField oneof")
            }
          }
        }
    }
  }

  test(
    "decode Milvus string-array wire bytes independently of the protobuf encoder"
  ) {
    // ScalarField.string_data -> StringArray.data = ["x", ""].
    withBinary(
      Seq(Some(Array[Byte](0x32, 0x05, 0x0a, 0x01, 0x78, 0x0a, 0x00)))
    ) { root =>
      values(
        read(root, sparkField(MilvusDataType.VarChar)).getArray(0),
        StringType
      ) shouldBe
        Seq(UTF8String.fromString("x"), UTF8String.fromString(""))
    }
  }

  test("accept both packed and unpacked repeated integer wire encodings") {
    val payloads = Seq(
      Array[Byte](0x12, 0x05, 0x0a, 0x03, 1, 2, 3),
      Array[Byte](0x12, 0x06, 0x08, 1, 0x08, 2, 0x08, 3)
    )
    payloads.foreach { payload =>
      withBinary(Seq(Some(payload))) { root =>
        read(root, sparkField(MilvusDataType.Int32))
          .getArray(0)
          .toIntArray() shouldBe Array(1, 2, 3)
      }
    }
  }

  test(
    "retain known array data when protobuf contains unrelated unknown fields"
  ) {
    val fixture = fixtures.last
    val payload = fixture.payload.toByteArray ++ Array[Byte](0x7a, 0)
    withBinary(Seq(Some(payload))) { root =>
      values(
        read(root, sparkField(fixture.elementType)).getArray(0),
        StringType
      ) shouldBe fixture.expected
    }
  }

  test(
    "reject truncated protobuf, invalid tags and invalid UTF-8 without exposing payload"
  ) {
    val badPayloads = Seq(
      Array[Byte](0x32, 0x05, 0x0a, 0x01, 0x78),
      Array[Byte](0),
      Array[Byte](0x32, 0x04, 0x0a, 0x02, 0xc3.toByte, 0x28)
    )
    badPayloads.foreach { payload =>
      withBinary(Seq(Some(payload))) { root =>
        val error = intercept[IllegalArgumentException](
          read(root, sparkField(MilvusDataType.VarChar))
        )
        error.getMessage should include("column '104' at row 0")
        error.getMessage should include("invalid ScalarField protobuf")
      }
    }
  }

  test(
    "reject an unknown oneof instead of silently returning an empty array"
  ) {
    withBinary(Seq(Some(Array[Byte](0x7a, 0)))) { root =>
      intercept[IllegalArgumentException](
        read(root, sparkField(MilvusDataType.VarChar))
      ).getMessage should include("incompatible ScalarField oneof")
    }
  }

  test("check bounds before narrowing integer arrays") {
    Seq(ByteType -> Seq(-129, 128), ShortType -> Seq(-32769, 32768)).foreach {
      case (elementType, invalid) =>
        val field = sparkField(MilvusDataType.Int16)
          .copy(dataType = ArrayType(elementType))
        invalid.foreach { value =>
          val payload =
            ScalarField().withIntData(IntArray(Seq(value))).toByteArray
          withBinary(Seq(Some(payload))) { root =>
            intercept[IllegalArgumentException](
              read(root, field)
            ).getMessage should include("overflow")
          }
        }
    }
  }

  test("byte array projection preserves signed boundaries") {
    val field =
      sparkField(MilvusDataType.Int8).copy(dataType = ArrayType(ByteType))
    val payload =
      ScalarField().withIntData(IntArray(Seq(-128, 0, 127))).toByteArray
    withBinary(Seq(Some(payload))) { root =>
      read(root, field).getArray(0).toByteArray() shouldBe Array[Byte](
        -128,
        0,
        127
      )
    }
  }

  test(
    "require Array metadata for protobuf decoding and retain raw BinaryType"
  ) {
    val payload = fixtures.last.payload.toByteArray
    withBinary(Seq(Some(payload))) { root =>
      val field = sparkField(MilvusDataType.VarChar)
      intercept[IllegalArgumentException](
        read(root, field.copy(metadata = Metadata.empty))
      )
      val jsonMetadata =
        DataTypeUtil.metadata(FieldSchema(dataType = MilvusDataType.JSON))
      intercept[IllegalArgumentException](
        read(root, field.copy(metadata = jsonMetadata))
      )
      read(root, StructField("values", BinaryType)).getBinary(
        0
      ) shouldBe payload
    }
  }

  test(
    "reject unsupported array elements and fixed-size binary for scalar arrays"
  ) {
    val field = sparkField(MilvusDataType.VarChar)
    withBinary(Seq(Some(fixtures.last.payload.toByteArray))) { root =>
      intercept[IllegalArgumentException](
        read(root, field.copy(dataType = ArrayType(BinaryType)))
      )
      intercept[IllegalArgumentException](
        read(root, field.copy(dataType = ArrayType(ArrayType(StringType))))
      )
    }
    val schema = new Schema(
      Seq(
        new Field(
          "104",
          FieldType.nullable(new ArrowType.FixedSizeBinary(2)),
          null
        )
      ).asJava
    )
    withRoot(schema) { root =>
      root
        .getVector("104")
        .asInstanceOf[FixedSizeBinaryVector]
        .setSafe(0, Array[Byte](0x32, 0))
      root.setRowCount(1)
      intercept[IllegalArgumentException](
        read(root, field)
      ).getMessage should include("FixedSizeBinaryVector")
    }
  }

  test(
    "read stored float arrays alongside dense vectors, JSON, strings and scalars"
  ) {
    val fields = Seq(
      FieldSchema(
        name = "array",
        fieldID = 100,
        dataType = MilvusDataType.Array,
        elementType = MilvusDataType.Float
      ),
      FieldSchema(
        name = "vector",
        fieldID = 101,
        dataType = MilvusDataType.FloatVector,
        typeParams = Seq(KeyValuePair("dim", "2"))
      ),
      FieldSchema(name = "json", fieldID = 102, dataType = MilvusDataType.JSON),
      FieldSchema(
        name = "label",
        fieldID = 103,
        dataType = MilvusDataType.VarChar
      ),
      FieldSchema(name = "id", fieldID = 104, dataType = MilvusDataType.Int64),
      FieldSchema(name = "flag", fieldID = 105, dataType = MilvusDataType.Bool)
    )
    val schema = StructType(fields.map { field =>
      StructField(
        field.name,
        DataTypeUtil.toDataType(field),
        metadata = DataTypeUtil.metadata(field)
      )
    })
    val arrowSchema = MilvusSchemaUtil.convertToArrowSchemaWithFieldIdNames(
      CollectionSchema(fields = fields)
    )
    withRoot(arrowSchema) { root =>
      root
        .getVector("100")
        .asInstanceOf[VarBinaryVector]
        .setSafe(
          0,
          ScalarField().withFloatData(FloatArray(Seq(1.5f, -2.0f))).toByteArray
        )
      root
        .getVector("101")
        .asInstanceOf[FixedSizeBinaryVector]
        .setSafe(
          0,
          ByteBuffer
            .allocate(8)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putFloat(3.0f)
            .putFloat(4.0f)
            .array()
        )
      root
        .getVector("102")
        .asInstanceOf[VarBinaryVector]
        .setSafe(0, """{"enabled":true}""".getBytes(UTF_8))
      root
        .getVector("103")
        .asInstanceOf[VarCharVector]
        .setSafe(0, "synthetic".getBytes(UTF_8))
      root.getVector("104").asInstanceOf[BigIntVector].setSafe(0, 42L)
      root.getVector("105").asInstanceOf[BitVector].setSafe(0, 1)
      root.setRowCount(1)
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        schema,
        fields.map(f => f.name -> f.fieldID.toString).toMap
      )
      row.getArray(0).toFloatArray() shouldBe Array(1.5f, -2.0f)
      row.getArray(1).toFloatArray() shouldBe Array(3.0f, 4.0f)
      row.getUTF8String(2).toString shouldBe """{"enabled":true}"""
      row.getUTF8String(3).toString shouldBe "synthetic"
      row.getLong(4) shouldBe 42L
      row.getBoolean(5) shouldBe true
    }
  }
}
