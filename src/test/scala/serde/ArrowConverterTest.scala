package com.zilliz.spark.connector.serde

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{FixedSizeBinaryVector, VarBinaryVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.FloatConverter

class ArrowConverterTest extends AnyFunSuite with Matchers {
  test("arrowToInternalRow reads Spark StringType from Arrow VarBinaryVector") {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowField = new Field(
      "103",
      FieldType.nullable(new ArrowType.Binary()),
      null
    )
    val root =
      VectorSchemaRoot.create(new Schema(Seq(arrowField).asJava), allocator)

    try {
      val vector = root.getVector("103").asInstanceOf[VarBinaryVector]
      vector.allocateNew()
      vector.setSafe(
        0,
        "{\"source\":\"json\"}".getBytes(StandardCharsets.UTF_8)
      )
      vector.setValueCount(1)
      root.setRowCount(1)

      val sparkSchema = StructType(Seq(StructField("$meta", StringType, true)))
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        sparkSchema,
        Map("$meta" -> "103")
      )

      row.getUTF8String(0).toString shouldBe "{\"source\":\"json\"}"
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("arrowToInternalRow rejects non UTF-8 VarBinary as Spark StringType") {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowField = new Field(
      "103",
      FieldType.nullable(new ArrowType.Binary()),
      null
    )
    val root =
      VectorSchemaRoot.create(new Schema(Seq(arrowField).asJava), allocator)

    try {
      val vector = root.getVector("103").asInstanceOf[VarBinaryVector]
      vector.allocateNew()
      vector.setSafe(0, Array[Byte](0xc3.toByte, 0x28.toByte))
      vector.setValueCount(1)
      root.setRowCount(1)

      val sparkSchema = StructType(Seq(StructField("$meta", StringType, true)))
      val err = intercept[IllegalArgumentException] {
        ArrowConverter.arrowToInternalRow(
          root,
          0,
          sparkSchema,
          Map("$meta" -> "103")
        )
      }

      err.getMessage should include("not valid UTF-8")
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("arrowToInternalRow reads BinaryVector from FixedSizeBinary as bytes") {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowField = new Field(
      "100",
      FieldType.nullable(new ArrowType.FixedSizeBinary(2)),
      null
    )
    val root =
      VectorSchemaRoot.create(new Schema(Seq(arrowField).asJava), allocator)

    try {
      val vector = root.getVector("100").asInstanceOf[FixedSizeBinaryVector]
      vector.allocateNew()
      vector.set(0, Array[Byte](0x01.toByte, 0x02.toByte))
      vector.setValueCount(1)
      root.setRowCount(1)

      val sparkSchema =
        StructType(Seq(StructField("binary", ArrayType(ByteType))))
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        sparkSchema,
        Map("binary" -> "100")
      )

      row.getArray(0).toByteArray().toSeq shouldBe Seq(1.toByte, 2.toByte)
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("arrowToInternalRow decodes Float16Vector from FixedSizeBinary") {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowField = new Field(
      "102",
      new FieldType(
        true,
        new ArrowType.FixedSizeBinary(4),
        null,
        Map("milvus_data_type" -> "102").asJava
      ),
      null
    )
    val root =
      VectorSchemaRoot.create(new Schema(Seq(arrowField).asJava), allocator)

    try {
      val vector = root.getVector("102").asInstanceOf[FixedSizeBinaryVector]
      vector.allocateNew()
      vector.set(
        0,
        (FloatConverter.toFloat16Bytes(1.5f) ++ FloatConverter
          .toFloat16Bytes(-2.0f)).toArray
      )
      vector.setValueCount(1)
      root.setRowCount(1)

      val sparkSchema =
        StructType(Seq(StructField("float16", ArrayType(FloatType))))
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        sparkSchema,
        Map("float16" -> "102")
      )

      row.getArray(0).toFloatArray.toSeq shouldBe Seq(1.5f, -2.0f)
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("arrowToInternalRow decodes BFloat16Vector from FixedSizeBinary") {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowField = new Field(
      "103",
      new FieldType(
        true,
        new ArrowType.FixedSizeBinary(4),
        null,
        Map("milvus_data_type" -> "103").asJava
      ),
      null
    )
    val root =
      VectorSchemaRoot.create(new Schema(Seq(arrowField).asJava), allocator)

    try {
      val vector = root.getVector("103").asInstanceOf[FixedSizeBinaryVector]
      vector.allocateNew()
      vector.set(
        0,
        (FloatConverter.toBFloat16Bytes(1.5f) ++ FloatConverter
          .toBFloat16Bytes(-2.0f)).toArray
      )
      vector.setValueCount(1)
      root.setRowCount(1)

      val sparkSchema =
        StructType(Seq(StructField("bfloat16", ArrayType(FloatType))))
      val row = ArrowConverter.arrowToInternalRow(
        root,
        0,
        sparkSchema,
        Map("bfloat16" -> "103")
      )

      row.getArray(0).toFloatArray.toSeq shouldBe Seq(1.5f, -2.0f)
    } finally {
      root.close()
      allocator.close()
    }
  }
}
