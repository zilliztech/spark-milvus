package com.zilliz.spark.connector.table

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.databind.node.LongNode
import org.apache.arrow.vector.types.pojo.ArrowType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.json.{
  FieldJson,
  KeyValueJson,
  SnapshotJson
}
import com.zilliz.milvus.storage.DataParseException

/** A snapshot's CollectionSchemaJson to a Spark StructType. */
class SnapshotSparkSchemaTest extends AnyFunSuite with Matchers {

  private val snapshotFilePath = "core/src/test/data/sample_snapshot.json"

  private def readFile(path: String): Either[Throwable, SnapshotJson] =
    SnapshotJson.parse(
      new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
    )

  test(
    "Convert snapshot schema to Spark StructType (excluding system fields)"
  ) {
    import org.apache.spark.sql.types._

    val result =
      readFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    // Convert to Spark schema without system fields
    val sparkSchema = SnapshotSparkSchema.toSparkSchema(
      metadata.collection.schema,
      includeSystemFields = false
    )

    // Should have 5 user fields (excluding RowID and Timestamp)
    sparkSchema.fields should have size 5

    // Verify field names and types
    val fieldNames = sparkSchema.fields.map(_.name)
    fieldNames should contain allOf ("id", "int64", "float", "varchar", "vector")
    fieldNames should not contain "RowID"
    fieldNames should not contain "Timestamp"

    // Verify data types
    sparkSchema("id").dataType shouldBe LongType
    sparkSchema("int64").dataType shouldBe LongType
    sparkSchema("float").dataType shouldBe FloatType
    sparkSchema("varchar").dataType shouldBe StringType
    sparkSchema("vector").dataType shouldBe ArrayType(FloatType)
    sparkSchema("vector").metadata.getLong(
      FieldMetadata.MilvusDataTypeMetadataKey
    ) shouldBe 101L
  }

  test(
    "Convert snapshot schema to Spark StructType (including system fields)"
  ) {
    import org.apache.spark.sql.types._

    val result =
      readFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    // Convert to Spark schema with system fields
    val sparkSchema = SnapshotSparkSchema.toSparkSchema(
      metadata.collection.schema,
      includeSystemFields = true
    )

    // Should have 7 fields (including RowID and Timestamp)
    sparkSchema.fields should have size 7

    // Verify field names
    val fieldNames = sparkSchema.fields.map(_.name)
    fieldNames should contain allOf ("id", "int64", "float", "varchar", "vector", "RowID", "Timestamp")
  }

  test("toSparkSchema uses array element type and snapshot nullable flag") {
    val json = """
    {
      "snapshot-info": {
        "name": "test",
        "id": 1,
        "collection_id": 1,
        "partition_ids": [1],
        "create_ts": 1
      },
      "collection": {
        "schema": {
          "name": "test",
          "fields": [
            {
              "fieldID": 100,
              "name": "id",
              "data_type": "Int64",
              "nullable": false
            },
            {
              "fieldID": 101,
              "name": "tags",
              "data_type": "Array",
              "element_type": "VarChar",
              "nullable": true
            }
          ]
        }
      },
      "indexes": [],
      "manifest-list": []
    }
    """

    val schema = SnapshotJson.parse(json).toOption.get.collection.schema
    val sparkSchema = SnapshotSparkSchema.toSparkSchema(schema)

    sparkSchema("id").nullable shouldBe false
    sparkSchema("tags").dataType shouldBe org.apache.spark.sql.types.ArrayType(
      org.apache.spark.sql.types.StringType
    )
    sparkSchema("tags").nullable shouldBe true
  }

  test("toSparkSchema maps BinaryVector and Int8Vector consistently") {
    import org.apache.spark.sql.types.{ArrayType, BinaryType, ShortType}

    val json = """
    {
      "snapshot-info": {
        "name": "test",
        "id": 1,
        "collection_id": 1,
        "partition_ids": [1],
        "create_ts": 1
      },
      "collection": {
        "schema": {
          "name": "test",
          "fields": [
            {
              "fieldID": 100,
              "name": "binary_vec",
              "data_type": "BinaryVector",
              "type_params": [{"key": "dim", "value": "128"}]
            },
            {
              "fieldID": 101,
              "name": "int8_vec",
              "data_type": "Int8Vector",
              "type_params": [{"key": "dim", "value": "4"}]
            }
          ]
        }
      },
      "indexes": [],
      "manifest-list": []
    }
    """

    val schema = SnapshotJson.parse(json).toOption.get.collection.schema
    val sparkSchema = SnapshotSparkSchema.toSparkSchema(schema)

    sparkSchema("binary_vec").dataType shouldBe BinaryType
    sparkSchema("binary_vec").metadata.getLong(
      FieldMetadata.MilvusDataTypeMetadataKey
    ) shouldBe 100L
    sparkSchema("binary_vec").metadata.getLong(
      FieldMetadata.MilvusVectorDimensionMetadataKey
    ) shouldBe 128L
    sparkSchema("int8_vec").dataType shouldBe ArrayType(ShortType)
    sparkSchema("int8_vec").metadata.getLong(
      FieldMetadata.MilvusDataTypeMetadataKey
    ) shouldBe 105L
    sparkSchema("int8_vec").metadata.getLong(
      FieldMetadata.MilvusVectorDimensionMetadataKey
    ) shouldBe 4L

    import scala.collection.JavaConverters._
    val arrowFields = com.zilliz.spark.connector.types.SparkSchemaMapper
      .convertSparkSchemaToArrow(sparkSchema)
      .getFields
      .asScala
      .map(field => field.getName -> field)
      .toMap
    arrowFields("binary_vec").getType shouldBe
      new ArrowType.FixedSizeBinary(16)
    arrowFields("int8_vec").getType shouldBe new ArrowType.FixedSizeBinary(4)
  }

  test("fieldToStructField preserves milvus.data_type metadata") {
    val field = FieldJson(
      fieldID = Some(LongNode.valueOf(407L)),
      name = "binary_vec",
      rawDataType =
        Some(com.fasterxml.jackson.databind.node.IntNode.valueOf(100)),
      isPrimaryKey = Some(true),
      isClusteringKey = Some(true),
      typeParams = Some(Seq(KeyValueJson("dim", "128"))),
      isPartitionKey = Some(true),
      nullable = Some(false)
    )

    val structField = SnapshotSparkSchema.fieldToStructField(field)

    structField.name shouldBe "binary_vec"
    structField.dataType shouldBe org.apache.spark.sql.types.BinaryType
    structField.nullable shouldBe false
    structField.metadata.getLong(
      FieldMetadata.MilvusDataTypeMetadataKey
    ) shouldBe 100L
    structField.metadata.getLong(
      FieldMetadata.MilvusVectorDimensionMetadataKey
    ) shouldBe 128L
    structField.metadata.getLong(
      FieldMetadata.MilvusFieldIdMetadataKey
    ) shouldBe 407L
    structField.metadata.getBoolean(
      FieldMetadata.MilvusPrimaryKeyMetadataKey
    ) shouldBe true
    structField.metadata.getBoolean(
      FieldMetadata.MilvusPartitionKeyMetadataKey
    ) shouldBe true
    structField.metadata.getBoolean(
      FieldMetadata.MilvusClusteringKeyMetadataKey
    ) shouldBe true
  }

  test("a recorded dynamic field keeps its type, id and nullability") {
    val field = FieldJson(
      fieldID = Some(LongNode.valueOf(407L)),
      name = "$meta",
      rawDataType =
        Some(com.fasterxml.jackson.databind.node.IntNode.valueOf(23)),
      isDynamic = Some(true),
      nullable = Some(true)
    )

    val structField = SnapshotSparkSchema.fieldToStructField(field)

    structField.name shouldBe "$meta"
    structField.dataType shouldBe org.apache.spark.sql.types.StringType
    structField.nullable shouldBe true
    structField.metadata.getLong(
      FieldMetadata.MilvusFieldIdMetadataKey
    ) shouldBe 407L
    structField.metadata.getLong(
      FieldMetadata.MilvusDataTypeMetadataKey
    ) shouldBe 23L
  }

  test("a missing nullable flag uses the protobuf non-nullable default") {
    val field = FieldJson(
      name = "id",
      rawDataType = Some(com.fasterxml.jackson.databind.node.IntNode.valueOf(5))
    )

    SnapshotSparkSchema.fieldToStructField(field).nullable shouldBe false
  }

  test("snapshot fields use SparkTypes unsupported-type failures") {
    val field = FieldJson(
      name = "unsupported",
      rawDataType =
        Some(com.fasterxml.jackson.databind.node.IntNode.valueOf(26))
    )

    an[DataParseException] should be thrownBy
      SnapshotSparkSchema.fieldToSparkType(field)
  }
}
