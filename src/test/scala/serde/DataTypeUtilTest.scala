package com.zilliz.spark.connector

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types.DataTypes
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.FloatingPointPrecision

import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/**
 * Unit tests for DataTypeUtil type conversions
 */
class DataTypeUtilTest extends AnyFunSuite with Matchers {

  // ============ toArrowType tests ============

  test("toArrowType converts Bool to Arrow Bool") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.Bool)
    result shouldBe a[ArrowType.Bool]
  }

  test("toArrowType converts Int8 to Arrow Int(8, signed)") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.Int8)
    result shouldBe a[ArrowType.Int]
    result.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 8
    result.asInstanceOf[ArrowType.Int].getIsSigned shouldBe true
  }

  test("toArrowType converts Int16 to Arrow Int(16, signed)") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.Int16)
    result shouldBe a[ArrowType.Int]
    result.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 16
  }

  test("toArrowType converts Int32 to Arrow Int(32, signed)") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.Int32)
    result shouldBe a[ArrowType.Int]
    result.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 32
  }

  test("toArrowType converts Int64 to Arrow Int(64, signed)") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.Int64)
    result shouldBe a[ArrowType.Int]
    result.asInstanceOf[ArrowType.Int].getBitWidth shouldBe 64
  }

  test("toArrowType converts Float to Arrow FloatingPoint SINGLE") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.Float)
    result shouldBe a[ArrowType.FloatingPoint]
    result.asInstanceOf[ArrowType.FloatingPoint].getPrecision shouldBe FloatingPointPrecision.SINGLE
  }

  test("toArrowType converts Double to Arrow FloatingPoint DOUBLE") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.Double)
    result shouldBe a[ArrowType.FloatingPoint]
    result.asInstanceOf[ArrowType.FloatingPoint].getPrecision shouldBe FloatingPointPrecision.DOUBLE
  }

  test("toArrowType converts VarChar to Arrow Utf8") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.VarChar)
    result shouldBe a[ArrowType.Utf8]
  }

  test("toArrowType converts String to Arrow Utf8") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.String)
    result shouldBe a[ArrowType.Utf8]
  }

  test("toArrowType converts JSON to Arrow Binary") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.JSON)
    result shouldBe a[ArrowType.Binary]
  }

  test("toArrowType converts FloatVector to FixedSizeBinary with correct size") {
    val dim = 128
    val result = DataTypeUtil.toArrowType(dim, MilvusDataType.FloatVector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    result.asInstanceOf[ArrowType.FixedSizeBinary].getByteWidth shouldBe (dim * 4)
  }

  test("toArrowType converts BinaryVector to FixedSizeBinary with correct size") {
    val dim = 128
    val result = DataTypeUtil.toArrowType(dim, MilvusDataType.BinaryVector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    // Binary vector: (dim + 7) / 8 bytes
    result.asInstanceOf[ArrowType.FixedSizeBinary].getByteWidth shouldBe ((dim + 7) / 8)
  }

  test("toArrowType converts Float16Vector to FixedSizeBinary with correct size") {
    val dim = 64
    val result = DataTypeUtil.toArrowType(dim, MilvusDataType.Float16Vector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    result.asInstanceOf[ArrowType.FixedSizeBinary].getByteWidth shouldBe (dim * 2)
  }

  test("toArrowType converts BFloat16Vector to FixedSizeBinary with correct size") {
    val dim = 64
    val result = DataTypeUtil.toArrowType(dim, MilvusDataType.BFloat16Vector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    result.asInstanceOf[ArrowType.FixedSizeBinary].getByteWidth shouldBe (dim * 2)
  }

  test("toArrowType converts Int8Vector to FixedSizeBinary with correct size") {
    val dim = 256
    val result = DataTypeUtil.toArrowType(dim, MilvusDataType.Int8Vector)
    result shouldBe a[ArrowType.FixedSizeBinary]
    result.asInstanceOf[ArrowType.FixedSizeBinary].getByteWidth shouldBe dim
  }

  test("toArrowType converts SparseFloatVector to Binary") {
    val result = DataTypeUtil.toArrowType(0, MilvusDataType.SparseFloatVector)
    result shouldBe a[ArrowType.Binary]
  }

  // ============ toDataType tests ============

  test("toDataType converts Bool to Spark BooleanType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Bool)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.BooleanType
  }

  test("toDataType converts Int8 to Spark ByteType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int8)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.ByteType
  }

  test("toDataType converts Int16 to Spark ShortType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int16)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.ShortType
  }

  test("toDataType converts Int32 to Spark IntegerType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int32)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.IntegerType
  }

  test("toDataType converts Int64 to Spark LongType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int64)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.LongType
  }

  test("toDataType converts Float to Spark FloatType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Float)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.FloatType
  }

  test("toDataType converts Double to Spark DoubleType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Double)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.DoubleType
  }

  test("toDataType converts String to Spark StringType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.String)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.StringType
  }

  test("toDataType converts VarChar to Spark StringType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.VarChar)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.StringType
  }

  test("toDataType converts JSON to Spark StringType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.JSON)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.StringType
  }

  test("toDataType converts FloatVector to Spark Array[Float]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.FloatVector)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.FloatType)
  }

  test("toDataType converts Int8Vector to Spark Array[Short]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int8Vector)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.ShortType)
  }

  test("toDataType converts Float16Vector to Spark Array[Float]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Float16Vector)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.FloatType)
  }

  test("toDataType converts BFloat16Vector to Spark Array[Float]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.BFloat16Vector)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.FloatType)
  }

  test("toDataType converts SparseFloatVector to Spark Map[Long, Float]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.SparseFloatVector)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.createMapType(DataTypes.LongType, DataTypes.FloatType)
  }

  test("toDataType converts Array with Int64 element to Spark Array[Long]") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType = MilvusDataType.Int64
    )
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.LongType)
  }

  test("toDataType converts Array with Float element to Spark Array[Float]") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType = MilvusDataType.Float
    )
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.FloatType)
  }

  test("toDataType converts Array with VarChar element to Spark Array[String]") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType = MilvusDataType.VarChar
    )
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.StringType)
  }

  test("toDataType converts Array with Bool element to Spark Array[Boolean]") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType = MilvusDataType.Bool
    )
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.BooleanType)
  }

  test("toDataType throws exception for unsupported data type") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.None)

    an[DataParseException] should be thrownBy {
      DataTypeUtil.toDataType(fieldSchema)
    }
  }

  test("toDataType throws exception for unsupported array element type") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType = MilvusDataType.FloatVector  // Vectors are not valid array elements
    )

    an[DataParseException] should be thrownBy {
      DataTypeUtil.toDataType(fieldSchema)
    }
  }
}
