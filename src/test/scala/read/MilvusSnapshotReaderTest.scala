package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Test suite for MilvusSnapshotReader
 */
class MilvusSnapshotReaderTest extends AnyFunSuite with Matchers {

  private val snapshotFilePath = "src/test/data/sample_snapshot.json"

  test("Parse complete snapshot metadata successfully") {
    val result = MilvusSnapshotReader.readSnapshotMetadataFromFile(snapshotFilePath)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    // Verify snapshot info
    metadata.snapshotInfo.name shouldBe "backfill_snapshot"
    metadata.snapshotInfo.id shouldBe 462324574599774209L
    metadata.snapshotInfo.description shouldBe Some("add field backfill snapshot")
    metadata.snapshotInfo.collectionId shouldBe 462324574592960519L
    metadata.snapshotInfo.partitionIds should contain(462324574592960520L)
    metadata.snapshotInfo.createTs shouldBe 462324677975474190L

    // Verify collection
    metadata.collection.numPartitions shouldBe Some(1)
    metadata.collection.numShards shouldBe Some(1)
    // consistency_level is optional and may not be present in sample data
    metadata.collection.consistencyLevel shouldBe a[Option[_]]

    // Verify manifest list
    metadata.manifestList should have size 1
    metadata.manifestList.head should include("data-file-manifest")

    // Verify storage v2 manifest list
    metadata.storageV2ManifestList shouldBe defined
    metadata.storageV2ManifestList.get should have size 1
    val storageV2Item = metadata.storageV2ManifestList.get.head
    storageV2Item.segmentID shouldBe 462416429317820786L
    storageV2Item.manifest should include("\"ver\":2")
    storageV2Item.manifest should include("\"base_path\"")
    storageV2Item.manifest should include("a-bucket/files/insert_log")
  }

  test("Parse collection schema successfully") {
    val result = MilvusSnapshotReader.readSnapshotMetadataFromFile(snapshotFilePath)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    val schema = metadata.collection.schema

    // Verify schema basic info
    schema.name shouldBe "backfilltestcollection"
    schema.description shouldBe Some("Test collection for MilvusBackfill")
    schema.fields should have size 7

    // Verify schema properties
    schema.properties shouldBe defined
    schema.properties.get should have size 1
    schema.properties.get.head.key shouldBe "timezone"
    schema.properties.get.head.value shouldBe "UTC"

    // Verify all field names
    val fieldNames = schema.fields.map(_.name)
    fieldNames should contain allOf("id", "int64", "float", "varchar", "vector", "RowID", "Timestamp")

    // Verify primary key field (id)
    val idField = schema.getFieldByName("id").get
    idField.getFieldIDAsLong shouldBe 100L
    idField.dataType shouldBe 5  // Int64
    idField.isPrimaryKey shouldBe Some(true)

    // Verify clustering key field (int64)
    val int64Field = schema.getFieldByName("int64").get
    int64Field.getFieldIDAsLong shouldBe 101L
    int64Field.dataType shouldBe 5  // Int64
    int64Field.isClusteringKey shouldBe Some(true)

    // Verify float field
    val floatField = schema.getFieldByName("float").get
    floatField.getFieldIDAsLong shouldBe 102L
    floatField.dataType shouldBe 10  // Float

    // Verify varchar field with type params
    val varcharField = schema.getFieldByName("varchar").get
    varcharField.getFieldIDAsLong shouldBe 103L
    varcharField.dataType shouldBe 21  // VarChar
    varcharField.typeParams shouldBe defined
    varcharField.getTypeParam("max_length") shouldBe Some("1024")

    // Verify vector field with type params
    val vectorField = schema.getFieldByName("vector").get
    vectorField.getFieldIDAsLong shouldBe 104L
    vectorField.dataType shouldBe 101  // FloatVector
    vectorField.typeParams shouldBe defined
    vectorField.getTypeParam("dim") shouldBe Some("128")

    // Verify system field RowID
    val rowIdField = schema.getFieldByName("RowID").get
    rowIdField.getFieldIDAsLong shouldBe 0L  // No fieldID specified
    rowIdField.dataType shouldBe 5  // Int64
    rowIdField.description shouldBe Some("row id")

    // Verify system field Timestamp
    val timestampField = schema.getFieldByName("Timestamp").get
    timestampField.getFieldIDAsLong shouldBe 1L
    timestampField.dataType shouldBe 5  // Int64
    timestampField.description shouldBe Some("timestamp")
  }

  test("Get primary key name from snapshot JSON") {
    val source = scala.io.Source.fromFile(snapshotFilePath)
    val json = try source.mkString finally source.close()

    val result = MilvusSnapshotReader.getPkName(json)

    result shouldBe a[Right[_, _]]
    result.toOption.get shouldBe "id"
  }

  test("Get primary key name fails when no primary key exists") {
    val jsonWithoutPk = """
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
              "name": "field1",
              "data_type": 5
            }
          ]
        }
      },
      "indexes": [],
      "manifest-list": []
    }
    """

    val result = MilvusSnapshotReader.getPkName(jsonWithoutPk)

    result shouldBe a[Left[_, _]]
    result.left.toOption.get.getMessage should include("No primary key field found")
  }

  test("Parse consistency_level from snapshot JSON") {
    val jsonWithConsistencyLevel = """
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
              "data_type": 5,
              "is_primary_key": true
            }
          ]
        },
        "consistency_level": 2
      },
      "indexes": [],
      "manifest-list": []
    }
    """

    val result = MilvusSnapshotReader.parseSnapshotMetadata(jsonWithConsistencyLevel)

    result shouldBe a[Right[_, _]]
    result.toOption.get.collection.consistencyLevel shouldBe Some(2)
  }

  test("Parse snapshot with unknown fields should not fail") {
    val jsonWithUnknownFields = """
    {
      "snapshot-info": {
        "name": "test",
        "id": 1,
        "collection_id": 1,
        "partition_ids": [1],
        "create_ts": 1,
        "unknown_field": "some_value"
      },
      "collection": {
        "schema": {
          "name": "test",
          "fields": [
            {
              "fieldID": 100,
              "name": "id",
              "data_type": 5,
              "is_primary_key": true
            }
          ]
        },
        "future_field": 123
      },
      "indexes": [],
      "manifest-list": []
    }
    """

    val result = MilvusSnapshotReader.parseSnapshotMetadata(jsonWithUnknownFields)

    result shouldBe a[Right[_, _]]
    result.toOption.get.snapshotInfo.name shouldBe "test"
  }

  test("Get Storage V2 manifest map from snapshot file") {
    val result = MilvusSnapshotReader.getStorageV2ManifestMap(snapshotFilePath)

    result shouldBe a[Right[_, _]]
    val manifestMap = result.toOption.get

    // Verify map contains the expected segment ID
    manifestMap should contain key 462416429317820786L

    // Verify the manifest content for this segment
    val content = manifestMap(462416429317820786L)
    content.ver shouldBe 2
    content.basePath shouldBe "a-bucket/files/insert_log/462416429317620777/462416429317620778/462416429317820786"

    // Verify map size
    manifestMap should have size 1
  }

  test("Convert snapshot schema to Spark StructType (excluding system fields)") {
    import org.apache.spark.sql.types._

    val result = MilvusSnapshotReader.readSnapshotMetadataFromFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    // Convert to Spark schema without system fields
    val sparkSchema = MilvusSnapshotReader.toSparkSchema(metadata.collection.schema, includeSystemFields = false)

    // Should have 5 user fields (excluding RowID and Timestamp)
    sparkSchema.fields should have size 5

    // Verify field names and types
    val fieldNames = sparkSchema.fields.map(_.name)
    fieldNames should contain allOf("id", "int64", "float", "varchar", "vector")
    fieldNames should not contain "RowID"
    fieldNames should not contain "Timestamp"

    // Verify data types
    sparkSchema("id").dataType shouldBe LongType
    sparkSchema("int64").dataType shouldBe LongType
    sparkSchema("float").dataType shouldBe FloatType
    sparkSchema("varchar").dataType shouldBe StringType
    sparkSchema("vector").dataType shouldBe ArrayType(FloatType)
  }

  test("Convert snapshot schema to Spark StructType (including system fields)") {
    import org.apache.spark.sql.types._

    val result = MilvusSnapshotReader.readSnapshotMetadataFromFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    // Convert to Spark schema with system fields
    val sparkSchema = MilvusSnapshotReader.toSparkSchema(metadata.collection.schema, includeSystemFields = true)

    // Should have 7 fields (including RowID and Timestamp)
    sparkSchema.fields should have size 7

    // Verify field names
    val fieldNames = sparkSchema.fields.map(_.name)
    fieldNames should contain allOf("id", "int64", "float", "varchar", "vector", "RowID", "Timestamp")
  }

  test("Get field ID to name mapping") {
    val result = MilvusSnapshotReader.readSnapshotMetadataFromFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    val fieldIdMap = MilvusSnapshotReader.getFieldIdMap(metadata.collection.schema)

    // Verify mappings
    fieldIdMap(100L) shouldBe "id"
    fieldIdMap(101L) shouldBe "int64"
    fieldIdMap(102L) shouldBe "float"
    fieldIdMap(103L) shouldBe "varchar"
    fieldIdMap(104L) shouldBe "vector"
    fieldIdMap(0L) shouldBe "RowID"
    fieldIdMap(1L) shouldBe "Timestamp"
  }

  test("Get field name to ID mapping") {
    val result = MilvusSnapshotReader.readSnapshotMetadataFromFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    val fieldNameToIdMap = MilvusSnapshotReader.getFieldNameToIdMap(metadata.collection.schema)

    // Verify mappings
    fieldNameToIdMap("id") shouldBe 100L
    fieldNameToIdMap("int64") shouldBe 101L
    fieldNameToIdMap("float") shouldBe 102L
    fieldNameToIdMap("varchar") shouldBe 103L
    fieldNameToIdMap("vector") shouldBe 104L
    fieldNameToIdMap("RowID") shouldBe 0L
    fieldNameToIdMap("Timestamp") shouldBe 1L
  }

  test("Serialize and deserialize manifest list") {
    val result = MilvusSnapshotReader.readSnapshotMetadataFromFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    val originalManifestList = metadata.storageV2ManifestList.get

    // Serialize
    val json = MilvusSnapshotReader.serializeManifestList(originalManifestList)
    json should not be empty

    // Deserialize
    val deserializeResult = MilvusSnapshotReader.deserializeManifestList(json)
    deserializeResult shouldBe a[Right[_, _]]
    val deserializedList = deserializeResult.toOption.get

    // Verify round-trip
    deserializedList should have size originalManifestList.size
    deserializedList.head.segmentID shouldBe originalManifestList.head.segmentID
    deserializedList.head.manifest shouldBe originalManifestList.head.manifest
  }

  test("Parse data_type as string format (e.g., 'Int64' instead of 5)") {
    val jsonWithStringDataType = """
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
              "is_primary_key": true
            },
            {
              "fieldID": 101,
              "name": "score",
              "data_type": "Float"
            },
            {
              "fieldID": 102,
              "name": "name",
              "data_type": "VarChar",
              "type_params": [{"key": "max_length", "value": "256"}]
            },
            {
              "fieldID": 103,
              "name": "embedding",
              "data_type": "FloatVector",
              "type_params": [{"key": "dim", "value": "128"}]
            },
            {
              "fieldID": 104,
              "name": "flag",
              "data_type": "Bool"
            }
          ]
        }
      },
      "indexes": [],
      "manifest-list": []
    }
    """

    val result = MilvusSnapshotReader.parseSnapshotMetadata(jsonWithStringDataType)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    val schema = metadata.collection.schema

    // Verify string data types are correctly converted to numeric codes
    schema.getFieldByName("id").get.dataType shouldBe 5       // Int64
    schema.getFieldByName("score").get.dataType shouldBe 10   // Float
    schema.getFieldByName("name").get.dataType shouldBe 21    // VarChar
    schema.getFieldByName("embedding").get.dataType shouldBe 101  // FloatVector
    schema.getFieldByName("flag").get.dataType shouldBe 1     // Bool
  }

  test("Parse data_type with mixed formats (some int, some string)") {
    val jsonWithMixedDataType = """
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
              "data_type": 5,
              "is_primary_key": true
            },
            {
              "fieldID": 101,
              "name": "score",
              "data_type": "Float"
            }
          ]
        }
      },
      "indexes": [],
      "manifest-list": []
    }
    """

    val result = MilvusSnapshotReader.parseSnapshotMetadata(jsonWithMixedDataType)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    val schema = metadata.collection.schema

    // Verify both formats work correctly
    schema.getFieldByName("id").get.dataType shouldBe 5       // Int format
    schema.getFieldByName("score").get.dataType shouldBe 10   // String format
  }
}
