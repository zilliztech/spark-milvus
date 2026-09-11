package com.zilliz.spark.connector

import org.apache.spark.sql.types.DataTypes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.DataParseException
import com.zilliz.spark.connector.serde.ArrowConverter
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** DataTypeUtil 的 Spark 侧映射。到 Arrow 类型的用例在 core 的 ArrowTypesTest。
  */
class DataTypeUtilTest extends AnyFunSuite with Matchers {

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

  test("toDataType converts BinaryVector to Spark BinaryType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.BinaryVector)
    val result = DataTypeUtil.toDataType(fieldSchema)
    result shouldBe DataTypes.BinaryType
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
    result shouldBe DataTypes.createMapType(
      DataTypes.LongType,
      DataTypes.FloatType
    )
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

  test(
    "toDataType converts Array with VarChar element to Spark Array[String]"
  ) {
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

  test("metadata stores Milvus data type code") {
    val fieldSchema = FieldSchema(
      name = "float16_vec",
      dataType = MilvusDataType.Float16Vector,
      typeParams = Seq(KeyValuePair(key = "dim", value = "128"))
    )
    val result = DataTypeUtil.metadata(fieldSchema)

    result.getLong(ArrowConverter.MilvusDataTypeMetadataKey) shouldBe
      MilvusDataType.Float16Vector.value.toLong
    result.getLong(ArrowConverter.MilvusVectorDimensionMetadataKey) shouldBe
      128L
  }

  test("toDataType throws exception for unsupported array element type") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType =
        MilvusDataType.FloatVector // Vectors are not valid array elements
    )

    an[DataParseException] should be thrownBy {
      DataTypeUtil.toDataType(fieldSchema)
    }
  }
}
