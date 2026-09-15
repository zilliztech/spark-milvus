package com.zilliz.spark.connector.types

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.types.DataTypes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** The Spark half of SparkTypes. The Arrow half is covered by ArrowTypesTest in
  * core.
  */
class SparkTypesTest extends AnyFunSuite with Matchers {

  test("toDataType converts Bool to Spark BooleanType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Bool)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.BooleanType
  }

  test("toDataType converts Int8 to Spark ByteType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int8)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.ByteType
  }

  test("toDataType converts Int16 to Spark ShortType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int16)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.ShortType
  }

  test("toDataType converts Int32 to Spark IntegerType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int32)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.IntegerType
  }

  test("toDataType converts Int64 to Spark LongType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int64)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.LongType
  }

  test("toDataType converts Float to Spark FloatType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Float)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.FloatType
  }

  test("toDataType converts Double to Spark DoubleType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Double)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.DoubleType
  }

  test("toDataType converts String to Spark StringType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.String)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.StringType
  }

  test("toDataType converts VarChar to Spark StringType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.VarChar)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.StringType
  }

  test("toDataType converts JSON to Spark StringType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.JSON)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.StringType
  }

  test("toDataType converts FloatVector to Spark Array[Float]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.FloatVector)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.FloatType)
  }

  test("toDataType converts BinaryVector to Spark BinaryType") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.BinaryVector)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.BinaryType
  }

  test("toDataType converts Int8Vector to Spark Array[Short]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Int8Vector)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.ShortType)
  }

  test("toDataType converts Float16Vector to Spark Array[Float]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Float16Vector)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.FloatType)
  }

  test("toDataType converts BFloat16Vector to Spark Array[Float]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.BFloat16Vector)
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.FloatType)
  }

  test("toDataType converts SparseFloatVector to Spark Map[Long, Float]") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.SparseFloatVector)
    val result = SparkTypes.toDataType(fieldSchema)
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
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.LongType)
  }

  test("toDataType converts Array with Float element to Spark Array[Float]") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType = MilvusDataType.Float
    )
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.FloatType)
  }

  test(
    "toDataType converts Array with VarChar element to Spark Array[String]"
  ) {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType = MilvusDataType.VarChar
    )
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.StringType)
  }

  test("toDataType converts Array with Bool element to Spark Array[Boolean]") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType = MilvusDataType.Bool
    )
    val result = SparkTypes.toDataType(fieldSchema)
    result shouldBe DataTypes.createArrayType(DataTypes.BooleanType)
  }

  test("toDataType throws exception for unsupported data type") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.None)

    an[DataParseException] should be thrownBy {
      SparkTypes.toDataType(fieldSchema)
    }
  }

  test("metadata stores Milvus data type code") {
    val fieldSchema = FieldSchema(
      name = "float16_vec",
      dataType = MilvusDataType.Float16Vector,
      typeParams = Seq(KeyValuePair(key = "dim", value = "128"))
    )
    val result = SparkTypes.metadata(fieldSchema)

    result.getLong(FieldMetadata.MilvusDataTypeMetadataKey) shouldBe
      MilvusDataType.Float16Vector.value.toLong
    result.getLong(FieldMetadata.MilvusVectorDimensionMetadataKey) shouldBe
      128L
  }

  test("toStructField keeps nullability, field identity and key metadata") {
    val fieldSchema = FieldSchema(
      fieldID = 407L,
      name = "keyed",
      dataType = MilvusDataType.Int64,
      isPrimaryKey = true,
      isPartitionKey = true,
      isClusteringKey = true,
      nullable = true
    )

    val field = SparkTypes.toStructField(fieldSchema)

    field.name shouldBe "keyed"
    field.dataType shouldBe DataTypes.LongType
    field.nullable shouldBe true
    field.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey) shouldBe 407L
    field.metadata.getBoolean(
      FieldMetadata.MilvusPrimaryKeyMetadataKey
    ) shouldBe true
    field.metadata.getBoolean(
      FieldMetadata.MilvusPartitionKeyMetadataKey
    ) shouldBe true
    field.metadata.getBoolean(
      FieldMetadata.MilvusClusteringKeyMetadataKey
    ) shouldBe true
  }

  test("toDataType throws exception for unsupported array element type") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType =
        MilvusDataType.FloatVector // Vectors are not valid array elements
    )

    an[DataParseException] should be thrownBy {
      SparkTypes.toDataType(fieldSchema)
    }
  }

  test(
    "toDataType goes through the Arrow type: Text reads back as StringType"
  ) {
    // Text has no row in any Milvus-to-Spark table; ArrowTypes stores it as
    // Utf8, so it arrives here as a string like VarChar does.
    val fieldSchema = FieldSchema(dataType = MilvusDataType.Text)
    SparkTypes.toDataType(fieldSchema) shouldBe DataTypes.StringType
  }

  test("JSON stays StringType although Arrow stores it as Binary") {
    val fieldSchema = FieldSchema(dataType = MilvusDataType.JSON)
    SparkTypes.toDataType(fieldSchema) shouldBe DataTypes.StringType
    SparkTypes.fromArrow(
      new ArrowType.Binary(),
      MilvusDataType.JSON
    ) shouldBe DataTypes.StringType
  }

  test("a nullable dense vector is still Array[Float]") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.FloatVector,
      nullable = true,
      typeParams = Seq(KeyValuePair("dim", "4"))
    )
    SparkTypes.toDataType(fieldSchema) shouldBe DataTypes.createArrayType(
      DataTypes.FloatType
    )
  }

  test("fromArrow refuses an Arrow Binary whose Milvus type it does not know") {
    an[DataParseException] should be thrownBy {
      SparkTypes.fromArrow(new ArrowType.Binary(), MilvusDataType.Int64)
    }
  }

  test("fromArrow requires the Arrow and Milvus types to agree") {
    an[DataParseException] should be thrownBy {
      SparkTypes.fromArrow(new ArrowType.Utf8(), MilvusDataType.Int64)
    }
  }

  test("toDataType refuses logical types whose readers are not implemented") {
    Seq(MilvusDataType.Geometry, MilvusDataType.Timestamptz).foreach {
      dataType =>
        withClue(dataType) {
          an[DataParseException] should be thrownBy {
            SparkTypes.toDataType(FieldSchema(dataType = dataType))
          }
        }
    }
  }

  test("toDataType converts Array with Int8 element to Spark Array[Short]") {
    val fieldSchema = FieldSchema(
      dataType = MilvusDataType.Array,
      elementType = MilvusDataType.Int8
    )
    SparkTypes.toDataType(fieldSchema) shouldBe DataTypes.createArrayType(
      DataTypes.ShortType
    )
  }
}
