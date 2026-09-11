package com.zilliz.milvus.storage.schema

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Milvus 类型到 Arrow 类型的映射。 */
class ArrowTypesTest extends AnyFunSuite with Matchers {

  test("toArrowType converts Bool to Arrow Bool") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.Bool)
    result shouldBe a[ArrowType.Bool]
  }

  test("toArrowType converts Int8 to Arrow Int(8, signed)") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.Int8)
    result shouldBe a[ArrowType.Int]
    result.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 8
    result.asInstanceOf[ArrowType.Int].getIsSigned shouldBe true
  }

  test("toArrowType converts Int16 to Arrow Int(16, signed)") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.Int16)
    result shouldBe a[ArrowType.Int]
    result.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 16
  }

  test("toArrowType converts Int32 to Arrow Int(32, signed)") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.Int32)
    result shouldBe a[ArrowType.Int]
    result.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 32
  }

  test("toArrowType converts Int64 to Arrow Int(64, signed)") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.Int64)
    result shouldBe a[ArrowType.Int]
    result.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 64
  }

  test("toArrowType converts Float to Arrow FloatingPoint SINGLE") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.Float)
    result shouldBe a[ArrowType.FloatingPoint]
    result
      .asInstanceOf[ArrowType.FloatingPoint]
      .getPrecision shouldBe FloatingPointPrecision.SINGLE
  }

  test("toArrowType converts Double to Arrow FloatingPoint DOUBLE") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.Double)
    result shouldBe a[ArrowType.FloatingPoint]
    result
      .asInstanceOf[ArrowType.FloatingPoint]
      .getPrecision shouldBe FloatingPointPrecision.DOUBLE
  }

  test("toArrowType converts VarChar to Arrow Utf8") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.VarChar)
    result shouldBe a[ArrowType.Utf8]
  }

  test("toArrowType converts String to Arrow Utf8") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.String)
    result shouldBe a[ArrowType.Utf8]
  }

  test("toArrowType converts JSON to Arrow Binary") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.JSON)
    result shouldBe a[ArrowType.Binary]
  }

  test(
    "toArrowType converts FloatVector to FixedSizeBinary with correct size"
  ) {
    val dim = 128
    val result = ArrowTypes.toArrowType(dim, MilvusDataType.FloatVector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    result
      .asInstanceOf[ArrowType.FixedSizeBinary]
      .getByteWidth shouldBe (dim * 4)
  }

  test(
    "toArrowType converts BinaryVector to FixedSizeBinary with correct size"
  ) {
    val dim = 128
    val result = ArrowTypes.toArrowType(dim, MilvusDataType.BinaryVector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    // Binary vector: (dim + 7) / 8 bytes
    result
      .asInstanceOf[ArrowType.FixedSizeBinary]
      .getByteWidth shouldBe ((dim + 7) / 8)
  }

  test(
    "toArrowType converts Float16Vector to FixedSizeBinary with correct size"
  ) {
    val dim = 64
    val result = ArrowTypes.toArrowType(dim, MilvusDataType.Float16Vector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    result
      .asInstanceOf[ArrowType.FixedSizeBinary]
      .getByteWidth shouldBe (dim * 2)
  }

  test(
    "toArrowType converts BFloat16Vector to FixedSizeBinary with correct size"
  ) {
    val dim = 64
    val result = ArrowTypes.toArrowType(dim, MilvusDataType.BFloat16Vector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    result
      .asInstanceOf[ArrowType.FixedSizeBinary]
      .getByteWidth shouldBe (dim * 2)
  }

  test("toArrowType converts Int8Vector to FixedSizeBinary with correct size") {
    val dim = 256
    val result = ArrowTypes.toArrowType(dim, MilvusDataType.Int8Vector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    result.asInstanceOf[ArrowType.FixedSizeBinary].getByteWidth shouldBe dim
  }

  test("toArrowType converts SparseFloatVector to Binary") {
    val result = ArrowTypes.toArrowType(0, MilvusDataType.SparseFloatVector)
    result shouldBe a[ArrowType.Binary]
  }

  // ============ toDataType tests ============
}
