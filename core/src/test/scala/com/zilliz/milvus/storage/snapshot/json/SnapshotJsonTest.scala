package com.zilliz.milvus.storage.snapshot.json

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.snapshot.{
  DeltaLogFile,
  V2ColumnGroup,
  V2SegmentInfo
}
import io.milvus.grpc.schema.{
  CollectionSchema => ProtoCollectionSchema,
  DataType
}

/** The snapshot JSON shapes, parsed from strings and from the sample file, and
  * the option-string codec.
  */
class SnapshotJsonTest extends AnyFunSuite with Matchers {

  private val snapshotFilePath = "core/src/test/data/sample_snapshot.json"

  private def readFile(path: String): Either[Throwable, SnapshotJson] =
    SnapshotJson.parse(
      new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
    )

  test("readUtf8WithLimit reads utf8 content within limit") {
    val bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val in = new java.io.ByteArrayInputStream(bytes)
    SnapshotJson.readUtf8WithLimit(in, "memory", 10) shouldBe "hello"
  }

  test("readUtf8WithLimit rejects content beyond limit") {
    val bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val in = new java.io.ByteArrayInputStream(bytes)
    val err = intercept[IllegalArgumentException] {
      SnapshotJson.readUtf8WithLimit(in, "memory", 4)
    }
    err.getMessage should include("exceeds")
  }

  test("readUtf8WithLimit rejects non-positive limit clearly") {
    val bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val in = new java.io.ByteArrayInputStream(bytes)
    val err = intercept[IllegalArgumentException] {
      SnapshotJson.readUtf8WithLimit(in, "memory", -1)
    }
    err.getMessage should include("must be positive")
  }

  test("Parse complete snapshot metadata successfully") {
    val result =
      readFile(snapshotFilePath)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    metadata.allSegments shouldBe empty

    // Verify snapshot info
    metadata.snapshotInfo.name shouldBe "backfill_snapshot"
    metadata.snapshotInfo.id shouldBe 462324574599774209L
    metadata.snapshotInfo.description shouldBe Some(
      "add field backfill snapshot"
    )
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
    val result =
      readFile(snapshotFilePath)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    val schema = metadata.collection.schema

    // Verify schema basic info
    schema.name shouldBe "backfilltestcollection"
    schema.description shouldBe Some("Test collection for MilvusBackfill")
    schema.version shouldBe 0
    schema.fields should have size 7

    // Verify schema properties
    schema.properties shouldBe defined
    schema.properties.get should have size 1
    schema.properties.get.head.key shouldBe "timezone"
    schema.properties.get.head.value shouldBe "UTC"

    // Verify all field names
    val fieldNames = schema.fields.map(_.name)
    fieldNames should contain allOf ("id", "int64", "float", "varchar", "vector", "RowID", "Timestamp")

    // Verify primary key field (id)
    val idField = schema.getFieldByName("id").get
    idField.getFieldIDAsLong shouldBe 100L
    idField.dataType shouldBe 5 // Int64
    idField.isPrimaryKey shouldBe Some(true)

    // Verify clustering key field (int64)
    val int64Field = schema.getFieldByName("int64").get
    int64Field.getFieldIDAsLong shouldBe 101L
    int64Field.dataType shouldBe 5 // Int64
    int64Field.isClusteringKey shouldBe Some(true)

    // Verify float field
    val floatField = schema.getFieldByName("float").get
    floatField.getFieldIDAsLong shouldBe 102L
    floatField.dataType shouldBe 10 // Float

    // Verify varchar field with type params
    val varcharField = schema.getFieldByName("varchar").get
    varcharField.getFieldIDAsLong shouldBe 103L
    varcharField.dataType shouldBe 21 // VarChar
    varcharField.typeParams shouldBe defined
    varcharField.getTypeParam("max_length") shouldBe Some("1024")

    // Verify vector field with type params
    val vectorField = schema.getFieldByName("vector").get
    vectorField.getFieldIDAsLong shouldBe 104L
    vectorField.dataType shouldBe 101 // FloatVector
    vectorField.typeParams shouldBe defined
    vectorField.getTypeParam("dim") shouldBe Some("128")

    // Verify system field RowID
    val rowIdField = schema.getFieldByName("RowID").get
    rowIdField.getFieldIDAsLong shouldBe 0L // No fieldID specified
    rowIdField.dataType shouldBe 5 // Int64
    rowIdField.description shouldBe Some("row id")

    // Verify system field Timestamp
    val timestampField = schema.getFieldByName("Timestamp").get
    timestampField.getFieldIDAsLong shouldBe 1L
    timestampField.dataType shouldBe 5 // Int64
    timestampField.description shouldBe Some("timestamp")
  }

  test("Parse collection schema version from snapshot JSON") {
    val json = """
    {
      "snapshot_info": {
        "name": "test",
        "id": 1,
        "collection_id": 1,
        "partition_ids": [1],
        "create_ts": 1
      },
      "collection": {
        "schema": {
          "name": "test",
          "version": 7,
          "fields": []
        }
      }
    }
    """

    val metadata =
      SnapshotJson.parse(json).toOption.get

    metadata.collection.schema.version shouldBe 7
  }

  test("Get primary key name from snapshot JSON") {
    val source = scala.io.Source.fromFile(snapshotFilePath)
    val json =
      try source.mkString
      finally source.close()

    SnapshotJson
      .parse(json)
      .toOption
      .get
      .collection
      .schema
      .primaryKey
      .map(_.name) shouldBe Some("id")
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

    SnapshotJson
      .parse(jsonWithoutPk)
      .toOption
      .get
      .collection
      .schema
      .primaryKey shouldBe None
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

    val result =
      SnapshotJson.parse(jsonWithConsistencyLevel)

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

    val result =
      SnapshotJson.parse(jsonWithUnknownFields)

    result shouldBe a[Right[_, _]]
    result.toOption.get.snapshotInfo.name shouldBe "test"
  }

  test("manifestSchemaVersion defaults legacy snapshots to v1") {
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
              "data_type": 5,
              "is_primary_key": true
            }
          ]
        }
      },
      "indexes": [],
      "manifest-list": []
    }
    """

    val metadata =
      SnapshotJson.parse(json).toOption.get

    metadata.manifestSchemaVersion shouldBe 1
  }

  test("manifestSchemaVersion uses snapshot format_version when present") {
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
              "data_type": 5,
              "is_primary_key": true
            }
          ]
        }
      },
      "format_version": 4,
      "indexes": [],
      "manifest-list": []
    }
    """

    val metadata =
      SnapshotJson.parse(json).toOption.get

    metadata.manifestSchemaVersion shouldBe 4
  }

  test("manifestSchemaVersion treats version 0 metadata as v1 manifests") {
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
              "data_type": 5,
              "is_primary_key": true
            }
          ]
        }
      },
      "format_version": 0,
      "indexes": [],
      "manifest-list": []
    }
    """

    val metadata =
      SnapshotJson.parse(json).toOption.get

    metadata.manifestSchemaVersion shouldBe 1
  }

  test("manifestSchemaVersion preserves v2 and v3 metadata versions") {
    val jsonV2 = """
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
        }
      },
      "format_version": 2,
      "indexes": [],
      "manifest-list": []
    }
    """
    val jsonV3 =
      jsonV2.replace("\"format_version\": 2", "\"format_version\": 3")

    SnapshotJson.parse(jsonV2).toOption.get.manifestSchemaVersion shouldBe 2
    SnapshotJson.parse(jsonV3).toOption.get.manifestSchemaVersion shouldBe 3
  }

  test(
    "Parse snapshot segment delete metadata from segments and segment_infos"
  ) {
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
              "data_type": 5,
              "is_primary_key": true
            }
          ]
        }
      },
      "indexes": [],
      "manifest-list": [],
      "segments": [
        {
          "segment_id": 10,
          "partition_id": 20,
          "segment_level": 1,
          "storage_version": 2,
          "deltalog_files": [
            {
              "field_id": 100,
              "binlogs": [
                {
                  "entries_num": 3,
                  "log_path": "files/delete-a",
                  "log_id": 7
                }
              ]
            }
          ]
        }
      ],
      "segment_infos": [
        {
          "segment_id": 11,
          "partition_id": 21,
          "segment_level": 0,
          "storage_version": 2,
          "deltalog_files": [
            {
              "field_id": 100,
              "binlogs": [
                {
                  "entries_num": 1,
                  "log_path": "files/delete-b",
                  "log_id": 8
                }
              ]
            }
          ]
        }
      ]
    }
    """

    val result = SnapshotJson.parse(json)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    metadata.allSegments should have size 1
    metadata.allSegments.head.segmentId shouldBe 10L
    metadata.allSegments.head.partitionId shouldBe 20L
    metadata.allSegments.head.segmentLevel shouldBe Some(1L)
    metadata.allSegments.head.storageVersion shouldBe 2L
    metadata.allSegments.head.deltaLogFiles should have size 1
    metadata.allSegments.head.deltaLogFiles.head.fieldId shouldBe 100L
    metadata.allSegments.head.deltaLogFiles.head.binlogs.head.entriesNum shouldBe 3L
    metadata.allSegments.head.deltaLogFiles.head.binlogs.head.logPath shouldBe "files/delete-a"
    metadata.allSegments.head.deltaLogFiles.head.binlogs.head.logId shouldBe 7L
  }

  test("allSegments falls back to segment_infos when segments is empty") {
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
              "data_type": 5,
              "is_primary_key": true
            }
          ]
        }
      },
      "indexes": [],
      "manifest-list": [],
      "segment_infos": [
        {
          "segment_id": 11,
          "partition_id": 21,
          "segment_level": 0,
          "storage_version": 2,
          "deltalog_files": [
            {
              "field_id": 100,
              "binlogs": [
                {
                  "entries_num": 1,
                  "log_path": "files/delete-b",
                  "log_id": 8
                }
              ]
            }
          ]
        }
      ]
    }
    """

    val result = SnapshotJson.parse(json)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    metadata.segments shouldBe empty
    metadata.segmentInfos should have size 1
    metadata.allSegments should have size 1
    metadata.allSegments.head.segmentId shouldBe 11L
    metadata.allSegments.head.segmentLevel shouldBe Some(0L)
    metadata.allSegments.head.deltaLogFiles.head.binlogs.head.logPath shouldBe "files/delete-b"
  }

  test("Get Storage V2 manifest map from snapshot file") {
    val manifestMap = readFile(
      snapshotFilePath
    ).toOption.get.storageV2ManifestList
      .getOrElse(Seq.empty)
      .map(item =>
        item.segmentID -> ManifestContentJson.parse(item.manifest).toOption.get
      )
      .toMap

    // Verify map contains the expected segment ID
    manifestMap should contain key 462416429317820786L

    // Verify the manifest content for this segment
    val content = manifestMap(462416429317820786L)
    content.ver shouldBe 2
    content.basePath shouldBe "a-bucket/files/insert_log/462416429317620777/462416429317620778/462416429317820786"

    // Verify map size
    manifestMap should have size 1
  }

  test("Get field ID to name mapping") {
    val result =
      readFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    val fieldIdMap =
      metadata.collection.schema.fieldNamesById

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
    val result =
      readFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get

    val fieldNameToIdMap =
      metadata.collection.schema.fieldIdsByName

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
    val result =
      readFile(snapshotFilePath)
    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    val originalManifestList = metadata.storageV2ManifestList.get

    // Serialize
    val json = SegmentListJson.encodeManifestItems(originalManifestList)
    json should not be empty

    // Deserialize
    val deserializeResult = SegmentListJson.decodeManifestItems(json)
    deserializeResult shouldBe a[Right[_, _]]
    val deserializedList = deserializeResult.toOption.get

    // Verify round-trip
    deserializedList should have size originalManifestList.size
    deserializedList.head.segmentID shouldBe originalManifestList.head.segmentID
    deserializedList.head.manifest shouldBe originalManifestList.head.manifest
  }

  test("serialize and deserialize V2 segments keeps delta logs") {
    val segments = Seq(
      V2SegmentInfo(
        segmentId = 10L,
        partitionId = 20L,
        numOfRows = 30L,
        storageVersion = 2L,
        columnGroups = Seq(
          V2ColumnGroup(
            fieldIds = Seq(100L, 0L, 1L),
            filePaths = Seq("files/insert_log/.../0/1"),
            fileRowCounts = Seq(30L)
          )
        ),
        deltaLogs = Seq(
          DeltaLogFile(
            logId = 9L,
            logPath = "files/delete_log/.../9",
            entriesNum = 2L
          ),
          DeltaLogFile(
            logId = 11L,
            logPath = "files/delete_log/.../11",
            entriesNum = 1L
          )
        )
      )
    )

    val json = SegmentListJson.encodeV2Segments(segments)
    json should not be empty

    val result = SegmentListJson.decodeV2Segments(json)
    result shouldBe a[Right[_, _]]
    val roundTripped = result.toOption.get

    roundTripped should have size 1
    roundTripped.head shouldBe segments.head
  }

  test(
    "deserializeV2Segments defaults legacy column-group JSON to unknown slot"
  ) {
    // JSON that predates slot_field_id: the group must decode to -1 so
    // slot-based dedup stays off rather than reading stale data.
    val json =
      """[{"segment_id":1,"partition_id":1,"num_of_rows":1,"storage_version":2,""" +
        """"column_groups":[{"field_ids":[100],"file_paths":["p"],"file_row_counts":[1]}]}]"""
    val segs = SegmentListJson.decodeV2Segments(json).toOption.get
    segs.head.columnGroups.head.slotFieldId shouldBe -1L
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

    val result =
      SnapshotJson.parse(jsonWithStringDataType)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    val schema = metadata.collection.schema

    // Verify string data types are correctly converted to numeric codes
    schema.getFieldByName("id").get.dataType shouldBe 5 // Int64
    schema.getFieldByName("score").get.dataType shouldBe 10 // Float
    schema.getFieldByName("name").get.dataType shouldBe 21 // VarChar
    schema.getFieldByName("embedding").get.dataType shouldBe 101 // FloatVector
    schema.getFieldByName("flag").get.dataType shouldBe 1 // Bool
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

    val result =
      SnapshotJson.parse(jsonWithMixedDataType)

    result shouldBe a[Right[_, _]]
    val metadata = result.toOption.get
    val schema = metadata.collection.schema

    // Verify both formats work correctly
    schema.getFieldByName("id").get.dataType shouldBe 5 // Int format
    schema.getFieldByName("score").get.dataType shouldBe 10 // String format
  }

  test("toProtobufSchemaBytes preserves read-path schema attributes") {
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
          "description": "schema desc",
          "autoID": true,
          "enable_dynamic_field": true,
          "properties": [{"key": "timezone", "value": "UTC"}],
          "fields": [
            {
              "fieldID": 100,
              "name": "id",
              "data_type": "Int64",
              "is_primary_key": true,
              "is_partition_key": true,
              "nullable": false,
              "state": "FieldCreated",
              "default_value": {"long_data": 7}
            },
            {
              "fieldID": 101,
              "name": "tags",
              "data_type": "Array",
              "element_type": "VarChar",
              "nullable": true,
              "is_function_output": true
            },
            {
              "fieldID": 102,
              "name": "dyn",
              "data_type": "JSON",
              "is_dynamic": true
            }
          ]
        }
      },
      "indexes": [],
      "manifest-list": []
    }
    """

    val schema = SnapshotJson.parse(json).toOption.get.collection.schema
    val proto = ProtoCollectionSchema.parseFrom(
      schema.toProtobufBytes
    )

    proto.name shouldBe "test"
    proto.description shouldBe "schema desc"
    proto.autoID shouldBe true
    proto.enableDynamicField shouldBe true
    proto.properties.map(p => p.key -> p.value) should contain(
      "timezone" -> "UTC"
    )

    val id = proto.fields.find(_.name == "id").get
    id.fieldID shouldBe 100L
    id.dataType shouldBe DataType.Int64
    id.isPrimaryKey shouldBe true
    id.isPartitionKey shouldBe true
    id.nullable shouldBe false
    id.getDefaultValue.getLongData shouldBe 7L

    val tags = proto.fields.find(_.name == "tags").get
    tags.dataType shouldBe DataType.Array
    tags.elementType shouldBe DataType.VarChar
    tags.nullable shouldBe true
    tags.isFunctionOutput shouldBe true

    val dyn = proto.fields.find(_.name == "dyn").get
    dyn.dataType shouldBe DataType.JSON
    dyn.isDynamic shouldBe true
  }
}
