package com.zilliz.milvus.storage.manifest

import org.apache.avro.Schema

/** The full Milvus V4 segment record schema. Each consumer owns its parsed
  * schema and chooses the fields it needs.
  */
object SnapshotSegmentSchema {

  // Milvus v3.0.4's internal/snapshotio/snapshot.go defines AvroSchemaV4.
  private val v4SchemaJson =
    """
      |{
      |  "type": "record",
      |  "name": "ManifestEntry",
      |  "fields": [
      |    {"name": "segment_id", "type": "long"},
      |    {"name": "partition_id", "type": "long"},
      |    {"name": "segment_level", "type": "long"},
      |    {"name": "channel_name", "type": "string"},
      |    {"name": "num_of_rows", "type": "long"},
      |    {
      |      "name": "start_position",
      |      "type": {
      |        "type": "record",
      |        "name": "AvroMsgPosition",
      |        "fields": [
      |          {"name": "channel_name", "type": "string"},
      |          {"name": "msg_id", "type": "bytes"},
      |          {"name": "msg_group", "type": "string"},
      |          {"name": "timestamp", "type": "long"}
      |        ]
      |      }
      |    },
      |    {
      |      "name": "dml_position",
      |      "type": "AvroMsgPosition"
      |    },
      |    {"name": "storage_version", "type": "long"},
      |    {"name": "is_sorted", "type": "boolean"},
      |    {"name": "commit_timestamp", "type": "long", "default": 0},
      |    {
      |      "name": "binlog_files",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "type": "record",
      |          "name": "AvroFieldBinlog",
      |          "fields": [
      |            {"name": "field_id", "type": "long"},
      |            {"name": "child_fields", "type": {"type": "array", "items": "long"}, "default": []},
      |            {"name": "format", "type": "string", "default": ""},
      |            {
      |              "name": "binlogs",
      |              "type": {
      |                "type": "array",
      |                "items": {
      |                  "type": "record",
      |                  "name": "AvroBinlog",
      |                  "fields": [
      |                    {"name": "entries_num", "type": "long"},
      |                    {"name": "timestamp_from", "type": "long"},
      |                    {"name": "timestamp_to", "type": "long"},
      |                    {"name": "log_path", "type": "string"},
      |                    {"name": "log_size", "type": "long"},
      |                    {"name": "log_id", "type": "long"},
      |                    {"name": "memory_size", "type": "long"}
      |                  ]
      |                }
      |              }
      |            }
      |          ]
      |        }
      |      }
      |    },
      |    {
      |      "name": "deltalog_files",
      |      "type": {
      |        "type": "array",
      |        "items": "AvroFieldBinlog"
      |      }
      |    },
      |    {
      |      "name": "statslog_files",
      |      "type": {
      |        "type": "array",
      |        "items": "AvroFieldBinlog"
      |      }
      |    },
      |    {
      |      "name": "bm25_statslog_files",
      |      "type": {
      |        "type": "array",
      |        "items": "AvroFieldBinlog"
      |      }
      |    },
      |    {
      |      "name": "index_files",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "type": "record",
      |          "name": "AvroIndexFilePathInfo",
      |          "fields": [
      |            {"name": "segment_id", "type": "long"},
      |            {"name": "field_id", "type": "long"},
      |            {"name": "index_id", "type": "long"},
      |            {"name": "build_id", "type": "long"},
      |            {"name": "index_name", "type": "string"},
      |            {
      |              "name": "index_params",
      |              "type": {
      |                "type": "array",
      |                "items": {
      |                  "type": "record",
      |                  "name": "AvroKeyValuePair",
      |                  "fields": [
      |                    {"name": "key", "type": "string"},
      |                    {"name": "value", "type": "string"}
      |                  ]
      |                }
      |              }
      |            },
      |            {"name": "index_file_paths", "type": {"type": "array", "items": "string"}},
      |            {"name": "serialized_size", "type": "long"},
      |            {"name": "index_version", "type": "long"},
      |            {"name": "num_rows", "type": "long"},
      |            {"name": "current_index_version", "type": "int"},
      |            {"name": "mem_size", "type": "long"},
      |            {"name": "current_scalar_index_version", "type": "int", "default": 0},
      |            {"name": "index_store_path_version", "type": "int", "default": 0}
      |          ]
      |        }
      |      }
      |    },
      |    {
      |      "name": "text_index_files",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "type": "record",
      |          "name": "AvroTextIndexEntry",
      |          "fields": [
      |            {"name": "field_id", "type": "long"},
      |            {
      |              "name": "stats",
      |              "type": {
      |                "type": "record",
      |                "name": "AvroTextIndexStats",
      |                "fields": [
      |                  {"name": "field_id", "type": "long"},
      |                  {"name": "version", "type": "long"},
      |                  {"name": "files", "type": {"type": "array", "items": "string"}},
      |                  {"name": "log_size", "type": "long"},
      |                  {"name": "memory_size", "type": "long"},
      |                  {"name": "build_id", "type": "long"}
      |                ]
      |              }
      |            }
      |          ]
      |        }
      |      }
      |    },
      |    {
      |      "name": "json_key_index_files",
      |      "type": {
      |        "type": "array",
      |        "items": {
      |          "type": "record",
      |          "name": "AvroJsonKeyIndexEntry",
      |          "fields": [
      |            {"name": "field_id", "type": "long"},
      |            {
      |              "name": "stats",
      |              "type": {
      |                "type": "record",
      |                "name": "AvroJSONKeyStats",
      |                "fields": [
      |                  {"name": "field_id", "type": "long"},
      |                  {"name": "version", "type": "long"},
      |                  {"name": "files", "type": {"type": "array", "items": "string"}},
      |                  {"name": "log_size", "type": "long"},
      |                  {"name": "memory_size", "type": "long"},
      |                  {"name": "build_id", "type": "long"},
      |                  {"name": "json_key_stats_data_format", "type": "long"}
      |                ]
      |              }
      |            }
      |          ]
      |        }
      |      }
      |    }
      |  ]
      |}
      |""".stripMargin

  val supportedVersions: Seq[Int] = Seq(4)

  /** Returns an independently owned full schema. Avro schemas are mutable, so a
    * caller must not be able to change another reader or writer's schema.
    */
  def schemaFor(version: Int): Schema = version match {
    case 4 => new Schema.Parser().parse(v4SchemaJson)
    case _ =>
      throw new IllegalArgumentException(
        s"Unsupported Milvus manifest schema version $version"
      )
  }
}
