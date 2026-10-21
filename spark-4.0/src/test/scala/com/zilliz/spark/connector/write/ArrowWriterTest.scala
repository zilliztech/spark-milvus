package com.zilliz.spark.connector.write

import java.nio.{ByteBuffer, ByteOrder}
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  FixedSizeBinaryVector,
  VarBinaryVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  ByteType,
  FloatType,
  LongType,
  MapType,
  MetadataBuilder,
  ShortType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.write.codec.SparseFloatVectorEncoder
import com.zilliz.spark.connector.types.{ArrowConverter, SparkSchemaMapper}
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Writer assertions retained for the later private writer migration. */
class ArrowWriterTest extends AnyFunSuite with Matchers {

  private def vectorField(
      name: String,
      sparkType: org.apache.spark.sql.types.DataType,
      milvusType: MilvusDataType,
      dimension: Option[Int] = None
  ): StructField = {
    val metadata = new MetadataBuilder()
      .putLong(FieldMetadata.MilvusDataTypeMetadataKey, milvusType.value)
    dimension.foreach(value =>
      metadata.putLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey,
        value
      )
    )
    StructField(
      name,
      sparkType,
      nullable = true,
      metadata = metadata.build()
    )
  }

  private def milvusField(
      name: String,
      dataType: org.apache.spark.sql.types.DataType,
      milvusType: MilvusDataType,
      fieldId: Long
  ): StructField =
    StructField(
      name,
      dataType,
      nullable = true,
      new MetadataBuilder()
        .putLong(FieldMetadata.MilvusDataTypeMetadataKey, milvusType.value)
        .putLong(FieldMetadata.MilvusFieldIdMetadataKey, fieldId)
        .build()
    )

  test("internalRowToArrow writes BinaryType to fixed and variable binary") {
    val bytes = Array[Byte](1, 2, 3, 4)
    val sparkSchema = StructType(Seq(StructField("vec", BinaryType)))

    Seq[ArrowType](
      new ArrowType.FixedSizeBinary(bytes.length),
      new ArrowType.Binary()
    ).foreach { arrowType =>
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val schema = new Schema(
          Seq(new Field("vec", FieldType.nullable(arrowType), null)).asJava
        )
        val root = VectorSchemaRoot.create(schema, allocator)
        try {
          root.allocateNew()
          ArrowWriter.internalRowToArrow(
            root,
            0,
            InternalRow(bytes),
            sparkSchema
          )
          root.getVector("vec") match {
            case vector: FixedSizeBinaryVector => vector.get(0) shouldBe bytes
            case vector: VarBinaryVector       => vector.get(0) shouldBe bytes
            case other => fail(s"unexpected vector ${other.getClass}")
          }
        } finally root.close()
      } finally allocator.close()
    }
  }

  test("internalRowToArrow writes nullable dense arrays to VarBinary") {
    import com.zilliz.spark.connector.types.SparkSchemaMapper

    def write(
        field: StructField,
        value: ArrayData
    ): Array[Byte] = {
      val sparkSchema = StructType(Seq(field))
      val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(sparkSchema)
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          root.allocateNew()
          ArrowWriter.internalRowToArrow(
            root,
            0,
            InternalRow.fromSeq(Seq(value)),
            sparkSchema
          )
          ArrowWriter.internalRowToArrow(
            root,
            1,
            InternalRow.fromSeq(Seq(null)),
            sparkSchema
          )
          val vector = root.getVector(field.name).asInstanceOf[VarBinaryVector]
          vector.setValueCount(2)
          vector.isNull(1) shouldBe true
          vector.get(0)
        } finally root.close()
      } finally allocator.close()
    }

    val floats = Array(1.5f, -2.0f)
    val floatBytes = ByteBuffer
      .allocate(8)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putFloat(floats(0))
      .putFloat(floats(1))
      .array()

    write(
      vectorField(
        "float",
        ArrayType(FloatType),
        MilvusDataType.FloatVector,
        Some(2)
      ),
      ArrayData.toArrayData(floats)
    ) shouldBe floatBytes
    write(
      vectorField(
        "float16",
        ArrayType(FloatType),
        MilvusDataType.Float16Vector,
        Some(2)
      ),
      ArrayData.toArrayData(floats)
    ) shouldBe floats.flatMap(FloatConverter.toFloat16Bytes)
    write(
      vectorField(
        "bfloat16",
        ArrayType(FloatType),
        MilvusDataType.BFloat16Vector,
        Some(2)
      ),
      ArrayData.toArrayData(floats)
    ) shouldBe floats.flatMap(FloatConverter.toBFloat16Bytes)
    write(
      vectorField(
        "int8",
        ArrayType(ShortType),
        MilvusDataType.Int8Vector,
        Some(4)
      ),
      ArrayData.toArrayData(Array[Short](-128, -1, 0, 127))
    ) shouldBe Array[Byte](-128, -1, 0, 127)
  }

  test("internalRowToArrow rejects wrong-width nullable dense vectors") {
    import com.zilliz.spark.connector.types.SparkSchemaMapper

    def reject(
        field: StructField,
        value: Any,
        expectedBytes: Int,
        actualBytes: Int,
        vectorDimensions: Map[String, Int] = Map.empty
    ): Unit = {
      val sparkSchema = StructType(Seq(field))
      val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(
        sparkSchema,
        vectorDimensions
      )
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          root.allocateNew()
          val error = intercept[IllegalArgumentException] {
            ArrowWriter.internalRowToArrow(
              root,
              0,
              InternalRow.fromSeq(Seq(value)),
              sparkSchema
            )
          }
          error.getMessage should include(s"expected $expectedBytes bytes")
          error.getMessage should include(s"got $actualBytes")
        } finally root.close()
      } finally allocator.close()
    }

    // No Spark dimension metadata here: exercise the Arrow `dim` metadata
    // emitted from the normal writer's vectorDimensions option.
    reject(
      vectorField(
        "float",
        ArrayType(FloatType),
        MilvusDataType.FloatVector
      ),
      ArrayData.toArrayData(Array(1.0f)),
      expectedBytes = 8,
      actualBytes = 4,
      vectorDimensions = Map("float" -> 2)
    )
    reject(
      vectorField(
        "float16",
        ArrayType(FloatType),
        MilvusDataType.Float16Vector,
        Some(2)
      ),
      ArrayData.toArrayData(Array(1.0f)),
      expectedBytes = 4,
      actualBytes = 2
    )
    reject(
      vectorField(
        "bfloat16",
        ArrayType(FloatType),
        MilvusDataType.BFloat16Vector,
        Some(2)
      ),
      ArrayData.toArrayData(Array(1.0f)),
      expectedBytes = 4,
      actualBytes = 2
    )
    reject(
      vectorField(
        "int8",
        ArrayType(ShortType),
        MilvusDataType.Int8Vector,
        Some(2)
      ),
      ArrayData.toArrayData(Array[Short](1)),
      expectedBytes = 2,
      actualBytes = 1
    )
    reject(
      vectorField(
        "binary_array",
        ArrayType(ByteType),
        MilvusDataType.BinaryVector,
        Some(16)
      ),
      ArrayData.toArrayData(Array[Byte](1)),
      expectedBytes = 2,
      actualBytes = 1
    )
    reject(
      vectorField(
        "binary_bytes",
        BinaryType,
        MilvusDataType.BinaryVector,
        Some(16)
      ),
      Array[Byte](1),
      expectedBytes = 2,
      actualBytes = 1
    )
  }

  test("internalRowToArrow rejects non-byte-aligned BinaryVector dimension") {
    import com.zilliz.spark.connector.types.SparkSchemaMapper

    val field = vectorField(
      "binary",
      BinaryType,
      MilvusDataType.BinaryVector,
      Some(10)
    )
    val sparkSchema = StructType(Seq(field))
    val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(sparkSchema)
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      try {
        root.allocateNew()
        val error = intercept[IllegalArgumentException] {
          ArrowWriter.internalRowToArrow(
            root,
            0,
            InternalRow(Array[Byte](1, 2)),
            sparkSchema
          )
        }
        error.getMessage should include("multiple of 8")
      } finally root.close()
    } finally allocator.close()
  }

  test("internalRowToArrow rejects null dense vector array elements") {
    def reject(field: StructField, value: ArrayData): Unit = {
      val sparkSchema = StructType(Seq(field))
      val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(sparkSchema)
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          root.allocateNew()
          val error = intercept[IllegalArgumentException] {
            ArrowWriter.internalRowToArrow(
              root,
              0,
              InternalRow(value),
              sparkSchema
            )
          }
          error.getMessage should include("null element at index 1")
        } finally root.close()
      } finally allocator.close()
    }

    reject(
      vectorField(
        "float",
        ArrayType(FloatType, containsNull = true),
        MilvusDataType.FloatVector,
        Some(2)
      ),
      ArrayData.toArrayData(Array[Any](1.0f, null))
    )
    reject(
      vectorField(
        "int8",
        ArrayType(ShortType, containsNull = true),
        MilvusDataType.Int8Vector,
        Some(2)
      ),
      ArrayData.toArrayData(Array[Any](1.toShort, null))
    )
    reject(
      vectorField(
        "binary",
        ArrayType(ByteType, containsNull = true),
        MilvusDataType.BinaryVector,
        Some(16)
      ),
      ArrayData.toArrayData(Array[Any](1.toByte, null))
    )
  }

  test("internalRowToArrow round-trips SparseFloatVector MapType as Binary") {
    val field = vectorField(
      "sparse",
      MapType(LongType, FloatType, valueContainsNull = false),
      MilvusDataType.SparseFloatVector
    )
    val sparkSchema = StructType(Seq(field))
    val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(sparkSchema)
    arrowSchema.findField("sparse").getType shouldBe new ArrowType.Binary()

    val mapData = ArrayBasedMapData(
      Array[Any](3L, 1L),
      Array[Any](2.5f, 1.25f)
    )
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      try {
        root.allocateNew()
        ArrowWriter.internalRowToArrow(
          root,
          0,
          InternalRow(mapData),
          sparkSchema
        )
        val vector = root.getVector("sparse").asInstanceOf[VarBinaryVector]
        vector.setValueCount(1)
        root.setRowCount(1)

        vector.get(0) shouldBe SparseFloatVectorEncoder
          .encodeSparseFloatVector(Map(1L -> 1.25f, 3L -> 2.5f))

        val decoded = ArrowConverter
          .arrowToInternalRow(root, 0, sparkSchema)
          .getMap(0)
        decoded.keyArray().toLongArray shouldBe Array(1L, 3L)
        decoded.valueArray().toFloatArray shouldBe Array(1.25f, 2.5f)
      } finally root.close()
    } finally allocator.close()
  }

  test("internalRowToArrow rejects negative SparseFloatVector values") {
    val field = vectorField(
      "sparse",
      MapType(LongType, FloatType, valueContainsNull = false),
      MilvusDataType.SparseFloatVector
    )
    val sparkSchema = StructType(Seq(field))

    val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(sparkSchema)
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      try {
        root.allocateNew()
        val error = intercept[com.zilliz.milvus.storage.DataParseException] {
          ArrowWriter.internalRowToArrow(
            root,
            0,
            InternalRow(
              ArrayBasedMapData(
                Array[Any](1L),
                Array[Any](-0.5f)
              )
            ),
            sparkSchema
          )
        }
        error.getMessage should include("non-negative")
      } finally root.close()
    } finally allocator.close()
  }

  test(
    "internalRowToArrow writes ByteType to TinyIntVector and reads it back"
  ) {
    val sparkSchema = StructType(Seq(StructField("i8", ByteType)))
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val schema = new Schema(
        Seq(
          new Field("i8", FieldType.nullable(new ArrowType.Int(8, true)), null)
        ).asJava
      )
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        root.allocateNew()
        val values = Seq[Byte](-128, 0, 7, 127)
        values.zipWithIndex.foreach { case (v, i) =>
          ArrowWriter.internalRowToArrow(
            root,
            i,
            InternalRow(v),
            sparkSchema
          )
        }
        ArrowWriter.internalRowToArrow(
          root,
          values.size,
          InternalRow(null),
          sparkSchema
        )
        root.setRowCount(values.size + 1)
        val vector =
          root
            .getVector("i8")
            .asInstanceOf[org.apache.arrow.vector.TinyIntVector]
        values.zipWithIndex.foreach { case (v, i) =>
          vector.isNull(i) shouldBe false
          vector.get(i) shouldBe v
          ArrowConverter
            .arrowToInternalRow(root, i, sparkSchema)
            .getByte(0) shouldBe v
        }
        vector.isNull(values.size) shouldBe true
      } finally root.close()
    } finally allocator.close()
  }

  test(
    "internalRowToArrow refuses a type it cannot write instead of dropping it"
  ) {
    val sparkSchema = StructType(
      Seq(StructField("d", org.apache.spark.sql.types.DecimalType(10, 2)))
    )
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val schema = new Schema(
        Seq(
          new Field("d", FieldType.nullable(new ArrowType.Int(32, true)), null)
        ).asJava
      )
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        root.allocateNew()
        an[IllegalArgumentException] should be thrownBy
          ArrowWriter.internalRowToArrow(
            root,
            0,
            InternalRow(org.apache.spark.sql.types.Decimal(1)),
            sparkSchema
          )
      } finally root.close()
    } finally allocator.close()
  }

  test(
    "a Milvus Array is written as one ScalarField per row, every element type"
  ) {
    import com.zilliz.milvus.storage.codec.ArrayCodec
    val cases: Seq[
      (org.apache.spark.sql.types.DataType, MilvusDataType, Seq[Any], Seq[Any])
    ] =
      Seq(
        (
          org.apache.spark.sql.types.BooleanType,
          MilvusDataType.Bool,
          Seq(true, false),
          Seq(true, false)
        ),
        (ShortType, MilvusDataType.Int8, Seq[Short](-128, 127), Seq(-128, 127)),
        (
          ShortType,
          MilvusDataType.Int16,
          Seq[Short](-300, 300),
          Seq(-300, 300)
        ),
        (
          org.apache.spark.sql.types.IntegerType,
          MilvusDataType.Int32,
          Seq(1, -2, 3),
          Seq(1, -2, 3)
        ),
        (
          LongType,
          MilvusDataType.Int64,
          Seq(1L, Long.MaxValue),
          Seq(1L, Long.MaxValue)
        ),
        (FloatType, MilvusDataType.Float, Seq(1.5f, -2f), Seq(1.5f, -2f)),
        (
          org.apache.spark.sql.types.DoubleType,
          MilvusDataType.Double,
          Seq(0.25, -1.0),
          Seq(0.25, -1.0)
        ),
        (
          StringType,
          MilvusDataType.VarChar,
          Seq(
            org.apache.spark.unsafe.types.UTF8String.fromString("a"),
            org.apache.spark.unsafe.types.UTF8String.fromString("中")
          ),
          Seq("a", "中")
        )
      )
    cases.foreach { case (elementType, milvusElement, sparkValues, stored) =>
      withClue(s"elements $milvusElement: ") {
        val plain = milvusField(
          "arr",
          ArrayType(elementType),
          MilvusDataType.Array,
          105L
        )
        val field = plain.copy(metadata =
          new MetadataBuilder()
            .withMetadata(plain.metadata)
            .putLong(
              FieldMetadata.MilvusElementTypeMetadataKey,
              milvusElement.value.toLong
            )
            .build()
        )
        val sparkSchema = StructType(Seq(field))
        val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(
          sparkSchema,
          fieldIds = Map("arr" -> 105L)
        )
        arrowSchema.getFields.get(0).getType shouldBe a[ArrowType.Binary]
        val allocator = new RootAllocator(Long.MaxValue)
        try {
          val root = VectorSchemaRoot.create(arrowSchema, allocator)
          try {
            root.allocateNew()
            ArrowWriter.internalRowToArrow(
              root,
              0,
              InternalRow(ArrayData.toArrayData(sparkValues.toArray)),
              sparkSchema
            )
            ArrowWriter.internalRowToArrow(
              root,
              1,
              InternalRow(ArrayData.toArrayData(Array.empty[Any])),
              sparkSchema
            )
            ArrowWriter.internalRowToArrow(
              root,
              2,
              InternalRow(null),
              sparkSchema
            )
            root.setRowCount(3)
            val v = root.getVector(0).asInstanceOf[VarBinaryVector]
            ArrayCodec.elements(v.get(0)) shouldBe stored
            // An empty array must not serialize to zero bytes: Milvus reads a
            // zero-length Binary value as null (segcore FieldData.cpp).
            v.get(1).length should be > 0
            ArrayCodec.elements(v.get(1)) shouldBe Seq.empty
            v.isNull(2) shouldBe true
            // And the connector reads back what it wrote, on the row path.
            // The column is named by field id, as the manifest records it.
            val names = Map("arr" -> "105")
            val back =
              ArrowConverter.arrowToInternalRow(root, 0, sparkSchema, names)
            back.getArray(0).numElements() shouldBe stored.size
            ArrowConverter
              .arrowToInternalRow(root, 1, sparkSchema, names)
              .getArray(0)
              .numElements() shouldBe 0
            ArrowConverter
              .arrowToInternalRow(root, 2, sparkSchema, names)
              .isNullAt(0) shouldBe true
          } finally root.close()
        } finally allocator.close()
      }
    }
  }

  test("a Milvus JSON field is written as Binary holding the JSON text") {
    val field = milvusField("j", StringType, MilvusDataType.JSON, 106L)
    val sparkSchema = StructType(Seq(field))
    val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(
      sparkSchema,
      fieldIds = Map("j" -> 106L)
    )
    arrowSchema.getFields.get(0).getType shouldBe a[ArrowType.Binary]
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      try {
        root.allocateNew()
        val json = """{"k":1,"tag":"中"}"""
        ArrowWriter.internalRowToArrow(
          root,
          0,
          InternalRow(
            org.apache.spark.unsafe.types.UTF8String.fromString(json)
          ),
          sparkSchema
        )
        root.setRowCount(1)
        new String(
          root.getVector(0).asInstanceOf[VarBinaryVector].get(0),
          java.nio.charset.StandardCharsets.UTF_8
        ) shouldBe json
        ArrowConverter
          .arrowToInternalRow(root, 0, sparkSchema, Map("j" -> "106"))
          .getUTF8String(0)
          .toString shouldBe json
      } finally root.close()
    } finally allocator.close()
  }
}
