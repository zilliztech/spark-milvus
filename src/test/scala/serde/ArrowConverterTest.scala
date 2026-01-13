package com.zilliz.spark.connector.serde

import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.ListVector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class ArrowConverterTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  val allocator = new RootAllocator(Long.MaxValue)

  test("ByteType (Int8) conversion") {
    val vector = new TinyIntVector("test", allocator)
    vector.allocateNew(1)
    vector.set(0, 42.toByte)
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, ByteType, "test_field", Map.empty)
    result shouldBe 42.toByte

    vector.close()
  }

  test("ShortType (Int16) conversion") {
    val vector = new SmallIntVector("test", allocator)
    vector.allocateNew(1)
    vector.set(0, 1234.toShort)
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, ShortType, "test_field", Map.empty)
    result shouldBe 1234.toShort

    vector.close()
  }

  test("IntegerType (Int32) conversion") {
    val vector = new IntVector("test", allocator)
    vector.allocateNew(1)
    vector.set(0, 123456)
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, IntegerType, "test_field", Map.empty)
    result shouldBe 123456

    vector.close()
  }

  test("LongType (Int64 and Timestamptz) conversion") {
    val vector = new BigIntVector("test", allocator)
    vector.allocateNew(1)
    vector.set(0, 123456789L)
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, LongType, "test_field", Map.empty)
    result shouldBe 123456789L

    vector.close()
  }

  test("FloatType conversion") {
    val vector = new Float4Vector("test", allocator)
    vector.allocateNew(1)
    vector.set(0, 3.14f)
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, FloatType, "test_field", Map.empty)
    result shouldBe 3.14f

    vector.close()
  }

  test("DoubleType conversion") {
    val vector = new Float8Vector("test", allocator)
    vector.allocateNew(1)
    vector.set(0, 3.14159265)
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, DoubleType, "test_field", Map.empty)
    result shouldBe 3.14159265

    vector.close()
  }

  test("BooleanType conversion") {
    val vector = new BitVector("test", allocator)
    vector.allocateNew(2)
    vector.set(0, 1)
    vector.set(1, 0)
    vector.setValueCount(2)

    val result1 = ArrowConverter.arrowValueToSparkValue(vector, 0, BooleanType, "test_field", Map.empty)
    result1 shouldBe true

    val result2 = ArrowConverter.arrowValueToSparkValue(vector, 1, BooleanType, "test_field", Map.empty)
    result2 shouldBe false

    vector.close()
  }

  test("StringType (VarChar) conversion") {
    val vector = new VarCharVector("test", allocator)
    vector.allocateNew(1)
    vector.set(0, "Hello, Milvus!".getBytes("UTF-8"))
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, StringType, "test_field", Map.empty)
    result shouldBe UTF8String.fromString("Hello, Milvus!")

    vector.close()
  }

  test("StringType (VarChar) basic conversion") {
    val vector = new VarCharVector("test", allocator)
    vector.allocateNew(1)
    vector.set(0, "test string".getBytes("UTF-8"))
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, StringType, "test_field", Map.empty)
    result shouldBe UTF8String.fromString("test string")

    vector.close()
  }

  test("BinaryType (VarBinary) conversion") {
    val vector = new VarBinaryVector("test", allocator)
    vector.allocateNew(1)
    val bytes = Array[Byte](1, 2, 3, 4, 5)
    vector.set(0, bytes)
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, BinaryType, "test_field", Map.empty)
    result shouldBe bytes

    vector.close()
  }

  test("BinaryVector (ArrayType(ByteType) from FixedSizeBinary) conversion") {
    val byteWidth = 8
    val vector = new FixedSizeBinaryVector("binary_vec", allocator, byteWidth)
    vector.allocateNew(1)
    val bytes = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)
    vector.set(0, bytes)
    vector.setValueCount(1)

    import io.milvus.grpc.schema.{DataType => MilvusDataType}
    val schemaMap = Map("binary_vec" -> MilvusDataType.BinaryVector)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, ArrayType(ByteType), "binary_vec", schemaMap)
    val arrayData = result.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
    arrayData.numElements() shouldBe 8
    (0 until 8).foreach { i =>
      arrayData.getByte(i) shouldBe bytes(i)
    }

    vector.close()
  }

  test("FloatVector (ArrayType(FloatType) from FixedSizeBinary) conversion") {
    val dim = 4
    val byteWidth = dim * 4
    val vector = new FixedSizeBinaryVector("test", allocator, byteWidth)
    vector.allocateNew(1)

    val floats = Array(1.0f, 2.0f, 3.0f, 4.0f)
    val buffer = ByteBuffer.allocate(byteWidth).order(ByteOrder.LITTLE_ENDIAN)
    floats.foreach(buffer.putFloat)
    vector.set(0, buffer.array())
    vector.setValueCount(1)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, ArrayType(FloatType), "test_field", Map.empty)
    val arrayData = result.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]

    arrayData.numElements() shouldBe 4
    arrayData.getFloat(0) shouldBe 1.0f
    arrayData.getFloat(1) shouldBe 2.0f
    arrayData.getFloat(2) shouldBe 3.0f
    arrayData.getFloat(3) shouldBe 4.0f

    vector.close()
  }

  test("Float16Vector (ArrayType(FloatType) from FixedSizeBinary with 2-byte elements) conversion") {
    val dim = 3  // Use odd dimension to avoid confusion with FloatVector
    val byteWidth = dim * 2  // 2 bytes per float16 = 6 bytes total
    val vector = new FixedSizeBinaryVector("float16_vec", allocator, byteWidth)
    vector.allocateNew(1)

    // Create float16 data: using simple values that convert cleanly
    // Float16 value 0x3C00 = 1.0f, 0x4000 = 2.0f, 0x4200 = 3.0f
    val float16Values = Array[Short](0x3C00, 0x4000, 0x4200)
    val buffer = ByteBuffer.allocate(byteWidth).order(ByteOrder.LITTLE_ENDIAN)
    float16Values.foreach(buffer.putShort)
    vector.set(0, buffer.array())
    vector.setValueCount(1)

    import io.milvus.grpc.schema.{DataType => MilvusDataType}
    val schemaMap = Map("float16_vec" -> MilvusDataType.Float16Vector)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, ArrayType(FloatType), "float16_vec", schemaMap)
    val arrayData = result.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]

    arrayData.numElements() shouldBe 3
    // Allow small floating point errors
    Math.abs(arrayData.getFloat(0) - 1.0f) should be < 0.01f
    Math.abs(arrayData.getFloat(1) - 2.0f) should be < 0.01f
    Math.abs(arrayData.getFloat(2) - 3.0f) should be < 0.01f

    vector.close()
  }

  test("Int8Vector (ArrayType(ShortType) from FixedSizeBinary) conversion") {
    val dim = 4
    val byteWidth = dim  // 1 byte per int8
    val vector = new FixedSizeBinaryVector("int8_vec", allocator, byteWidth)
    vector.allocateNew(1)

    val bytes = Array[Byte](1, 2, 3, 4)
    vector.set(0, bytes)
    vector.setValueCount(1)

    import io.milvus.grpc.schema.{DataType => MilvusDataType}
    val schemaMap = Map("int8_vec" -> MilvusDataType.Int8Vector)

    val result = ArrowConverter.arrowValueToSparkValue(vector, 0, ArrayType(ShortType), "int8_vec", schemaMap)
    val arrayData = result.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]

    arrayData.numElements() shouldBe 4
    arrayData.getShort(0) shouldBe 1.toShort
    arrayData.getShort(1) shouldBe 2.toShort
    arrayData.getShort(2) shouldBe 3.toShort
    arrayData.getShort(3) shouldBe 4.toShort

    vector.close()
  }

  test("Null value handling") {
    val vector = new VarCharVector("test", allocator)
    vector.allocateNew(1)
    vector.setNull(0)
    vector.setValueCount(1)

    // For null values, arrowValueToSparkValue should be called after checking isNull
    // In actual usage, the caller checks vector.isNull(rowIndex) before calling
    // So we just verify the vector reports it as null
    vector.isNull(0) shouldBe true

    vector.close()
  }

  override def afterAll(): Unit = {
    allocator.close()
  }
}
