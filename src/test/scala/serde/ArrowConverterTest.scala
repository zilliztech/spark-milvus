package com.zilliz.spark.connector.serde

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  FixedSizeBinaryVector,
  VarBinaryVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  ByteType,
  FloatType,
  MetadataBuilder,
  ShortType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.FloatConverter
import io.milvus.grpc.schema.{DataType => MilvusDataType}

class ArrowConverterTest extends AnyFunSuite with Matchers {

  private def withFixedSizeBinaryRoot(
      name: String,
      byteWidth: Int,
      bytes: Array[Byte]
  )(check: VectorSchemaRoot => Unit): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val schema = new Schema(
        Seq(
          new Field(
            name,
            FieldType.nullable(new ArrowType.FixedSizeBinary(byteWidth)),
            null
          )
        ).asJava
      )
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        val vector = root.getVector(name).asInstanceOf[FixedSizeBinaryVector]
        vector.allocateNew(1)
        vector.setSafe(0, bytes)
        vector.setValueCount(1)
        root.setRowCount(1)
        check(root)
      } finally root.close()
    } finally allocator.close()
  }

  private def withVariableWidthRoot(
      name: String,
      arrowType: ArrowType,
      bytes: Array[Byte]
  )(check: VectorSchemaRoot => Unit): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val schema = new Schema(
        Seq(new Field(name, FieldType.nullable(arrowType), null)).asJava
      )
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        root.getVector(name) match {
          case vector: VarBinaryVector =>
            vector.allocateNew()
            vector.setSafe(0, bytes)
            vector.setValueCount(1)
          case vector: VarCharVector =>
            vector.allocateNew()
            vector.setSafe(0, bytes)
            vector.setValueCount(1)
          case other =>
            throw new IllegalArgumentException(
              s"Unexpected vector type ${other.getClass.getSimpleName}"
            )
        }
        root.setRowCount(1)
        check(root)
      } finally root.close()
    } finally allocator.close()
  }

  private def vectorField(
      name: String,
      sparkType: org.apache.spark.sql.types.DataType,
      milvusType: MilvusDataType,
      dimension: Option[Int] = None
  ): StructField = {
    val metadata = new MetadataBuilder()
      .putLong(ArrowConverter.MilvusDataTypeMetadataKey, milvusType.value)
    dimension.foreach(value =>
      metadata.putLong(
        ArrowConverter.MilvusVectorDimensionMetadataKey,
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

  test("arrowToInternalRow reads BinaryVector fixed-size bytes") {
    val bytes = Array[Byte](0x01, 0x23, 0x45, 0x67)
    withFixedSizeBinaryRoot("binary", bytes.length, bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(
          Seq(vectorField("binary", BinaryType, MilvusDataType.BinaryVector))
        )
      )
      row.getBinary(0) shouldBe bytes
    }
  }

  test("arrowToInternalRow keeps dense vector fixed-size bytes as BinaryType") {
    val bytes = java.nio.ByteBuffer
      .allocate(8)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .putFloat(1.5f)
      .putFloat(-2.0f)
      .array()
    withFixedSizeBinaryRoot("float", bytes.length, bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(
          Seq(vectorField("float", BinaryType, MilvusDataType.FloatVector))
        )
      )
      row.getBinary(0) shouldBe bytes
    }
  }

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
          ArrowConverter.internalRowToArrow(
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
    import com.zilliz.spark.connector.MilvusSchemaUtil

    def write(
        field: StructField,
        value: ArrayData
    ): Array[Byte] = {
      val sparkSchema = StructType(Seq(field))
      val arrowSchema = MilvusSchemaUtil.convertSparkSchemaToArrow(sparkSchema)
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          root.allocateNew()
          ArrowConverter.internalRowToArrow(
            root,
            0,
            InternalRow.fromSeq(Seq(value)),
            sparkSchema
          )
          ArrowConverter.internalRowToArrow(
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
    import com.zilliz.spark.connector.MilvusSchemaUtil

    def reject(
        field: StructField,
        value: Any,
        expectedBytes: Int,
        actualBytes: Int,
        vectorDimensions: Map[String, Int] = Map.empty
    ): Unit = {
      val sparkSchema = StructType(Seq(field))
      val arrowSchema = MilvusSchemaUtil.convertSparkSchemaToArrow(
        sparkSchema,
        vectorDimensions
      )
      val allocator = new RootAllocator(Long.MaxValue)
      try {
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          root.allocateNew()
          val error = intercept[IllegalArgumentException] {
            ArrowConverter.internalRowToArrow(
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
    import com.zilliz.spark.connector.MilvusSchemaUtil

    val field = vectorField(
      "binary",
      BinaryType,
      MilvusDataType.BinaryVector,
      Some(10)
    )
    val sparkSchema = StructType(Seq(field))
    val arrowSchema = MilvusSchemaUtil.convertSparkSchemaToArrow(sparkSchema)
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      try {
        root.allocateNew()
        val error = intercept[IllegalArgumentException] {
          ArrowConverter.internalRowToArrow(
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

  test("arrowToInternalRow decodes nullable dense vectors from VarBinary") {
    val floats = Array(1.5f, -2.0f)
    val floatBytes = ByteBuffer
      .allocate(8)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putFloat(floats(0))
      .putFloat(floats(1))
      .array()

    def readFloats(
        name: String,
        bytes: Array[Byte],
        milvusType: MilvusDataType
    ): Array[Float] = {
      var result: Array[Float] = null
      withVariableWidthRoot(name, new ArrowType.Binary(), bytes) { root =>
        val row = ArrowConverter.arrowToInternalRow(
          root,
          0,
          StructType(
            Seq(vectorField(name, ArrayType(FloatType), milvusType))
          )
        )
        result = row.getArray(0).toFloatArray
      }
      result
    }

    readFloats("float", floatBytes, MilvusDataType.FloatVector) shouldBe floats
    readFloats(
      "float16",
      floats.flatMap(FloatConverter.toFloat16Bytes),
      MilvusDataType.Float16Vector
    ) shouldBe floats
    readFloats(
      "bfloat16",
      floats.flatMap(FloatConverter.toBFloat16Bytes),
      MilvusDataType.BFloat16Vector
    ) shouldBe floats

    val int8Bytes = Array[Byte](-128, -1, 0, 127)
    withVariableWidthRoot("int8", new ArrowType.Binary(), int8Bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(
          Seq(
            vectorField(
              "int8",
              ArrayType(ShortType),
              MilvusDataType.Int8Vector
            )
          )
        )
      )
      row.getArray(0).toShortArray shouldBe int8Bytes.map(_.toShort)
    }
  }

  test("arrowToInternalRow decodes valid UTF-8 from VarBinary as StringType") {
    val bytes = "hello, 世界".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    withVariableWidthRoot("text", new ArrowType.Binary(), bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(Seq(StructField("text", StringType)))
      )
      row.getUTF8String(0).toString shouldBe "hello, 世界"
    }
  }

  test(
    "arrowToInternalRow rejects invalid UTF-8 from VarBinary as StringType"
  ) {
    val bytes = Array[Byte](0xc3.toByte, 0x28.toByte)
    withVariableWidthRoot("text", new ArrowType.Binary(), bytes) { root =>
      val err = intercept[IllegalArgumentException] {
        ArrowConverter.arrowToInternalRow(
          root,
          0,
          StructType(Seq(StructField("text", StringType)))
        )
      }
      err.getMessage should include("not valid UTF-8")
    }
  }

  test(
    "arrowToInternalRow rejects Array[Short] for BinaryVector fixed-size bytes"
  ) {
    val bytes = Array[Byte](0x01, 0x23, 0x45, 0x67)
    withFixedSizeBinaryRoot("binary", bytes.length, bytes) { root =>
      val err = intercept[IllegalArgumentException] {
        ArrowConverter.arrowToInternalRow(
          root,
          0,
          StructType(
            Seq(
              vectorField(
                "binary",
                ArrayType(ShortType),
                MilvusDataType.BinaryVector
              )
            )
          )
        )
      }
      err.getMessage should include("Array[Short]")
    }
  }

  test(
    "arrowToInternalRow rejects Array[Byte] for Int8Vector fixed-size bytes"
  ) {
    val bytes = Array[Byte](-128, -1, 0, 127)
    withFixedSizeBinaryRoot("int8", bytes.length, bytes) { root =>
      val err = intercept[IllegalArgumentException] {
        ArrowConverter.arrowToInternalRow(
          root,
          0,
          StructType(
            Seq(
              vectorField(
                "int8",
                ArrayType(ByteType),
                MilvusDataType.Int8Vector
              )
            )
          )
        )
      }
      err.getMessage should include("Array[Byte]")
    }
  }

  test(
    "arrowToInternalRow rejects Array[Float] for BinaryVector fixed-size bytes"
  ) {
    val bytes = Array[Byte](0x01, 0x23, 0x45, 0x67)
    withFixedSizeBinaryRoot("binary", bytes.length, bytes) { root =>
      val err = intercept[IllegalArgumentException] {
        ArrowConverter.arrowToInternalRow(
          root,
          0,
          StructType(
            Seq(
              vectorField(
                "binary",
                ArrayType(FloatType),
                MilvusDataType.BinaryVector
              )
            )
          )
        )
      }
      err.getMessage should include("Array[Float]")
    }
  }

  test("arrowToInternalRow rejects BinaryType without vector metadata") {
    val bytes = Array[Byte](0x01, 0x23, 0x45, 0x67)
    withFixedSizeBinaryRoot("binary", bytes.length, bytes) { root =>
      val err = intercept[IllegalArgumentException] {
        ArrowConverter.arrowToInternalRow(
          root,
          0,
          StructType(Seq(StructField("binary", BinaryType)))
        )
      }
      err.getMessage should include(ArrowConverter.MilvusDataTypeMetadataKey)
    }
  }

  test("arrowToInternalRow rejects Array[Byte] without fixed-size metadata") {
    val bytes = Array[Byte](0x01, 0x23, 0x45, 0x67)
    withFixedSizeBinaryRoot("binary", bytes.length, bytes) { root =>
      val err = intercept[IllegalArgumentException] {
        ArrowConverter.arrowToInternalRow(
          root,
          0,
          StructType(Seq(StructField("binary", ArrayType(ByteType))))
        )
      }
      err.getMessage should include(ArrowConverter.MilvusDataTypeMetadataKey)
    }
  }

  test("arrowToInternalRow decodes UTF-8 from VarChar as StringType") {
    val bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    withVariableWidthRoot("text", new ArrowType.Utf8(), bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(Seq(StructField("text", StringType)))
      )
      row.getUTF8String(0).toString shouldBe "hello"
    }
  }

  test("arrowToInternalRow reads VarBinary as BinaryType") {
    val bytes = Array[Byte](1, 2, 3, 4)
    withVariableWidthRoot("blob", new ArrowType.Binary(), bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(Seq(StructField("blob", BinaryType)))
      )
      row.getBinary(0) shouldBe bytes
    }
  }

  test("arrowToInternalRow reads ByteType from TinyIntVector") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val schema = new Schema(
        Seq(
          new Field(
            "tiny",
            FieldType.nullable(new ArrowType.Int(8, true)),
            null
          )
        ).asJava
      )
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        val vector = root
          .getVector("tiny")
          .asInstanceOf[org.apache.arrow.vector.TinyIntVector]
        vector.allocateNew(1)
        vector.setSafe(0, 7.toByte)
        vector.setValueCount(1)
        root.setRowCount(1)
        val row = ArrowConverter.arrowToInternalRow(
          root,
          0,
          StructType(Seq(StructField("tiny", ByteType)))
        )
        row.getByte(0) shouldBe 7.toByte
      } finally root.close()
    } finally allocator.close()
  }

  test("arrowToInternalRow rejects unsupported BinaryType vector clearly") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val schema = new Schema(
        Seq(
          new Field(
            "bad",
            FieldType.nullable(new ArrowType.Int(32, true)),
            null
          )
        ).asJava
      )
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        val vector = root
          .getVector("bad")
          .asInstanceOf[org.apache.arrow.vector.IntVector]
        vector.allocateNew(1)
        vector.setSafe(0, 42)
        vector.setValueCount(1)
        root.setRowCount(1)
        val err = intercept[IllegalArgumentException] {
          ArrowConverter.arrowToInternalRow(
            root,
            0,
            StructType(Seq(StructField("bad", BinaryType)))
          )
        }
        err.getMessage should include("IntVector")
      } finally root.close()
    } finally allocator.close()
  }

  test(
    "arrowToInternalRow keeps legacy Array[Byte] BinaryVector fixed-size decoding"
  ) {
    val bytes = Array[Byte](0x01, 0x23, 0x45, 0x67)
    withFixedSizeBinaryRoot("binary", bytes.length, bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(
          Seq(
            vectorField(
              "binary",
              ArrayType(ByteType),
              MilvusDataType.BinaryVector
            )
          )
        )
      )
      row.getArray(0).toByteArray() shouldBe bytes
    }
  }

  test("arrowToInternalRow decodes Float16Vector fixed-size bytes") {
    val bytes = FloatConverter.toFloat16Bytes(1.5f).toArray ++
      FloatConverter.toFloat16Bytes(-2.0f).toArray
    withFixedSizeBinaryRoot("float16", bytes.length, bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(
          Seq(
            vectorField(
              "float16",
              ArrayType(FloatType),
              MilvusDataType.Float16Vector
            )
          )
        )
      )
      val out = row.getArray(0).toFloatArray
      out(0) shouldBe 1.5f
      out(1) shouldBe -2.0f
    }
  }

  test("arrowToInternalRow decodes BFloat16Vector fixed-size bytes") {
    val bytes = FloatConverter.toBFloat16Bytes(1.5f).toArray ++
      FloatConverter.toBFloat16Bytes(-2.0f).toArray
    withFixedSizeBinaryRoot("bfloat16", bytes.length, bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(
          Seq(
            vectorField(
              "bfloat16",
              ArrayType(FloatType),
              MilvusDataType.BFloat16Vector
            )
          )
        )
      )
      val out = row.getArray(0).toFloatArray
      out(0) shouldBe 1.5f
      out(1) shouldBe -2.0f
    }
  }

  test("arrowToInternalRow reads Int8Vector fixed-size bytes as shorts") {
    val bytes = Array[Byte](-128, -1, 0, 127)
    withFixedSizeBinaryRoot("int8", bytes.length, bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(
          Seq(
            vectorField(
              "int8",
              ArrayType(ShortType),
              MilvusDataType.Int8Vector
            )
          )
        )
      )
      row.getArray(0).toShortArray shouldBe bytes.map(_.toShort)
    }
  }

  test("arrowValueToSparkValue keeps legacy FloatVector fixed-size decoding") {
    val bytes = java.nio.ByteBuffer
      .allocate(8)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .putFloat(1.5f)
      .putFloat(-2.0f)
      .array()
    withFixedSizeBinaryRoot("float", bytes.length, bytes) { root =>
      val vector = root.getVector("float").asInstanceOf[FixedSizeBinaryVector]
      val out = ArrowConverter
        .arrowValueToSparkValue(vector, 0, ArrayType(FloatType))
        .asInstanceOf[ArrayData]
        .toFloatArray
      out shouldBe Array(1.5f, -2.0f)
    }
  }

  test(
    "arrowToInternalRow decodes FloatVector fixed-size bytes with metadata"
  ) {
    val bytes = java.nio.ByteBuffer
      .allocate(8)
      .order(java.nio.ByteOrder.LITTLE_ENDIAN)
      .putFloat(1.5f)
      .putFloat(-2.0f)
      .array()
    withFixedSizeBinaryRoot("float", bytes.length, bytes) { root =>
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        StructType(
          Seq(
            vectorField(
              "float",
              ArrayType(FloatType),
              MilvusDataType.FloatVector
            )
          )
        )
      )
      row.getArray(0).toFloatArray shouldBe Array(1.5f, -2.0f)
    }
  }

  test(
    "Float16Vector fixed-size bytes without metadata fail instead of corrupting"
  ) {
    val bytes = FloatConverter.toFloat16Bytes(1.5f).toArray ++
      FloatConverter.toFloat16Bytes(-2.0f).toArray
    withFixedSizeBinaryRoot("float16", bytes.length, bytes) { root =>
      val err = intercept[IllegalArgumentException] {
        ArrowConverter.arrowToInternalRow(
          root,
          0,
          StructType(Seq(StructField("float16", ArrayType(FloatType))))
        )
      }
      err.getMessage should include(ArrowConverter.MilvusDataTypeMetadataKey)
    }
  }
}
