package com.zilliz.spark.connector.read

import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{
  DeserializationFeature,
  JsonNode,
  ObjectMapper
}
import com.fasterxml.jackson.databind.cfg.{CoercionAction, CoercionInputShape}
import com.fasterxml.jackson.module.scala.{
  DefaultScalaModule,
  ScalaObjectMapper
}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  CollectionSchema => ProtoCollectionSchema,
  DataType,
  FieldSchema,
  FieldState
}

/** Reader for milvus-backup binlog-format backups.
  *
  * A milvus-backup `create` run writes, under the backup directory:
  *   - `meta/full_meta.json` — a proto-JSON `backuppb.BackupInfo` carrying the
  *     collection schema plus every segment's binlog / deltalog layout (mirrors
  *     Milvus's `schemapb.CollectionSchema` and `datapb.SegmentInfo`-lite), and
  *   - `binlogs/insert_log/...` and `binlogs/delta_log/...` — byte-identical
  *     copies of the Milvus storage objects.
  *
  * This object translates the meta into the same runtime objects the snapshot
  * read path consumes (`V2SegmentInfo`), so the backup can be read offline with
  * the existing packed-V2 reader. Two gaps vs. a Milvus snapshot are closed
  * here:
  *   1. milvus-backup persists only `log_size` per binlog, not `entries_num`.
  *      The per-file row count is recovered by reading each parquet footer's
  *      row count ([MilvusParquetFooterReader.readRowCount]). 2. The AVRO
  *      segment-info (and hence the slot -> real field ID mapping) is not
  *      copied by the backup; the real field IDs are recovered from each
  *      parquet file's own schema
  *      ([MilvusParquetFooterReader.readFieldIdsFromSchema]).
  *
  * Only StorageV2 (packed parquet, `storage_version == 2`) segments are
  * supported — the same format the packed-V2 reader handles. StorageV1 is
  * skipped and StorageV3 is rejected.
  */
object BackupMetaReader extends Logging {

  // -------------------------------------------------------------------------
  // Backup meta JSON model (wire keys match backuppb's Go encoding/json tags).
  // -------------------------------------------------------------------------

  /** Top-level `backuppb.BackupInfo`. */
  case class BackupInfo(
      @JsonProperty("id") id: String = "",
      @JsonProperty("name") name: String = "",
      @JsonProperty("format") format: String = "",
      @JsonProperty("milvus_version") milvusVersion: String = "",
      @JsonProperty("collection_backups") collectionBackups: Seq[
        CollectionBackup
      ] = Seq.empty
  ) {
    def isSnapshotFormat: Boolean = format == "snapshot"
  }

  case class CollectionBackup(
      @JsonProperty("collection_id") collectionId: Long = 0L,
      @JsonProperty("collection_name") collectionName: String = "",
      @JsonProperty("db_name") dbName: String = "",
      @JsonProperty("schema") schema: Option[BackupCollectionSchema] = None,
      @JsonProperty("partition_backups") partitionBackups: Seq[
        PartitionBackup
      ] = Seq.empty,
      @JsonProperty("l0_segments") l0Segments: Seq[SegmentBackup] = Seq.empty
  ) {
    def allSegments: Seq[SegmentBackup] =
      partitionBackups.flatMap(_.segmentBackups) ++ l0Segments
  }

  case class PartitionBackup(
      @JsonProperty("partition_id") partitionId: Long = 0L,
      @JsonProperty("partition_name") partitionName: String = "",
      @JsonProperty("segment_backups") segmentBackups: Seq[SegmentBackup] =
        Seq.empty
  )

  /** Lite `datapb.SegmentInfo`. */
  case class SegmentBackup(
      @JsonProperty("segment_id") segmentId: Long = 0L,
      @JsonProperty("collection_id") collectionId: Long = 0L,
      @JsonProperty("partition_id") partitionId: Long = 0L,
      @JsonProperty("num_of_rows") numOfRows: Long = 0L,
      @JsonProperty("binlogs") binlogs: Seq[FieldBinlog] = Seq.empty,
      @JsonProperty("deltalogs") deltalogs: Seq[FieldBinlog] = Seq.empty,
      @JsonProperty("group_id") groupId: Long = 0L,
      @JsonProperty("is_l0") isL0: Boolean = false,
      @JsonProperty("v_channel") vChannel: String = "",
      @JsonProperty("storage_version") storageVersion: Long = 0L
  )

  case class FieldBinlog(
      @JsonProperty("fieldID") fieldId: Long = 0L,
      @JsonProperty("binlogs") binlogs: Seq[Binlog] = Seq.empty
  )

  case class Binlog(
      @JsonProperty("entries_num") entriesNum: Long = 0L,
      @JsonProperty("log_path") logPath: String = "",
      @JsonProperty("log_size") logSize: Long = 0L,
      @JsonProperty("log_id") logId: Long = 0L
  )

  case class BackupKeyValuePair(
      @JsonProperty("key") key: String = "",
      @JsonProperty("value") value: String = ""
  )

  /** `backuppb.FieldSchema` — a mirror of Milvus's `schemapb.FieldSchema`. */
  case class BackupFieldSchema(
      @JsonProperty("fieldID") fieldId: Long = 0L,
      @JsonProperty("name") name: String = "",
      @JsonProperty("is_primary_key") isPrimaryKey: Boolean = false,
      @JsonProperty("description") description: String = "",
      @JsonProperty("data_type") rawDataType: Option[JsonNode] = None,
      @JsonProperty("type_params") typeParams: Seq[BackupKeyValuePair] =
        Seq.empty,
      @JsonProperty("index_params") indexParams: Seq[BackupKeyValuePair] =
        Seq.empty,
      @JsonProperty("autoID") autoId: Boolean = false,
      @JsonProperty("state") rawState: Option[JsonNode] = None,
      @JsonProperty("element_type") rawElementType: Option[JsonNode] = None,
      @JsonProperty("is_dynamic") isDynamic: Boolean = false,
      @JsonProperty("is_partition_key") isPartitionKey: Boolean = false,
      @JsonProperty("nullable") nullable: Boolean = false,
      @JsonProperty("is_function_output") isFunctionOutput: Boolean = false,
      @JsonProperty("default_value_base64") defaultValueBase64: String = ""
  ) {
    def dataType: Int = rawDataType.map(JsonTypeConverter.toInt).getOrElse(0)
    def elementType: Int =
      rawElementType.map(JsonTypeConverter.toInt).getOrElse(0)
    def state: Int = rawState.map(JsonTypeConverter.toInt).getOrElse(0)
  }

  /** `backuppb.CollectionSchema` — a mirror of Milvus's schemapb. */
  case class BackupCollectionSchema(
      @JsonProperty("name") name: String = "",
      @JsonProperty("description") description: String = "",
      @JsonProperty("autoID") autoId: Boolean = false,
      @JsonProperty("fields") fields: Seq[BackupFieldSchema] = Seq.empty,
      @JsonProperty("enable_dynamic_field") enableDynamicField: Boolean = false,
      @JsonProperty("properties") properties: Seq[BackupKeyValuePair] =
        Seq.empty
  )

  // -------------------------------------------------------------------------
  // Parsing
  // -------------------------------------------------------------------------

  private val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m.configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false)
    m.coercionConfigFor(classOf[java.lang.Integer])
      .setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m.coercionConfigFor(classOf[java.lang.Long])
      .setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m.coercionConfigFor(classOf[Int])
      .setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m.coercionConfigFor(classOf[Long])
      .setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m
  }

  /** Absolute path of the `full_meta.json` of a backup directory.
    *
    * Works for both `s3a://bucket/backup/<name>` (or any scheme Hadoop FS
    * understands) and plain local paths.
    */
  def metaPath(backupDir: String): String = {
    val base = Option(backupDir).map(_.trim).getOrElse("")
    if (base.isEmpty) {
      throw new IllegalArgumentException("backup dir must not be empty")
    }
    val stripped = if (base.endsWith("/")) base.stripSuffix("/") else base
    s"$stripped/meta/full_meta.json"
  }

  /** Read and parse the backup's `full_meta.json`. */
  def readMeta(
      hadoopConf: Configuration,
      backupDir: String
  ): Either[Throwable, BackupInfo] = {
    try {
      val metaFilePath = metaPath(backupDir)
      val bytes = V2SegmentLoader.readAllBytes(hadoopConf, metaFilePath)
      val json = new String(bytes, "UTF-8")
      Right(mapper.readValue[BackupInfo](json))
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  /** Parse an in-memory `full_meta.json` string. Exposed for callers that have
    * already read the bytes (e.g. tests) — [[readMeta]] covers the hadoop-path
    * case.
    */
  def parse(json: String): Either[Throwable, BackupInfo] = {
    try {
      Right(mapper.readValue[BackupInfo](json))
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  // -------------------------------------------------------------------------
  // Schema conversion
  // -------------------------------------------------------------------------

  /** Convert a backup `CollectionSchema` into Milvus protobuf
    * `CollectionSchema` bytes — the format the packed-V2 reader expects.
    */
  def toProtobufSchemaBytes(schema: BackupCollectionSchema): Array[Byte] = {
    val userFields =
      schema.fields.filterNot(f => f.name == "RowID" || f.name == "Timestamp")
    val protoFields = userFields.map { field =>
      FieldSchema(
        fieldID = field.fieldId,
        name = field.name,
        description = field.description,
        dataType = DataType.fromValue(field.dataType),
        isPrimaryKey = field.isPrimaryKey,
        isClusteringKey = false,
        typeParams = field.typeParams.map(toProtoKV),
        indexParams = field.indexParams.map(toProtoKV),
        autoID = field.autoId,
        state = FieldState.fromValue(field.state),
        elementType = DataType.fromValue(field.elementType),
        isDynamic = field.isDynamic,
        isPartitionKey = field.isPartitionKey,
        nullable = field.nullable,
        isFunctionOutput = field.isFunctionOutput,
        defaultValue = decodeDefaultValue(field)
      )
    }
    val protoSchema = ProtoCollectionSchema(
      name = schema.name,
      description = schema.description,
      autoID = schema.autoId,
      fields = protoFields,
      enableDynamicField = schema.enableDynamicField,
      properties = schema.properties.map(toProtoKV)
    )
    protoSchema.toByteArray
  }

  private def toProtoKV(kv: BackupKeyValuePair): KeyValuePair =
    KeyValuePair(key = kv.key, value = kv.value)

  /** milvus-backup stores the default value as base64-encoded, proto-marshalled
    * `schemapb.ValueField`. The wire format matches
    * `io.milvus.grpc.schema.ValueField`, so a direct `parseFrom` recovers it.
    * Best-effort: a malformed or absent value degrades to no default.
    */
  private def decodeDefaultValue(
      field: BackupFieldSchema
  ): Option[io.milvus.grpc.schema.ValueField] = {
    val b64 = field.defaultValueBase64
    if (b64 == null || b64.isEmpty) {
      None
    } else {
      scala.util.Try {
        io.milvus.grpc.schema.ValueField.parseFrom(
          java.util.Base64.getDecoder.decode(b64)
        )
      }.toOption
    }
  }

  // -------------------------------------------------------------------------
  // Segment conversion
  // -------------------------------------------------------------------------

  /** Build the runtime `V2SegmentInfo` list for every StorageV2 segment of the
    * backup. L0 delete-only segments keep an empty `columnGroups` so they feed
    * the inherited delete-plan path, exactly like the snapshot reader.
    *
    * @param bucket
    *   Bucket holding the backup's objects (empty for local paths). Bucket-
    *   relative `log_path` values are resolved to `s3a://bucket/...`.
    */
  def toV2Segments(
      info: BackupInfo,
      hadoopConf: Configuration,
      bucket: String,
      applyDeletes: Boolean = true
  ): Either[Throwable, Seq[V2SegmentInfo]] = {
    try {
      if (info.isSnapshotFormat) {
        throw new IllegalStateException(
          s"backup '${info.name}' is in snapshot format; only binlog-format " +
            "backups can be read as a datasource"
        )
      }
      val out = scala.collection.mutable.ArrayBuffer.empty[V2SegmentInfo]
      info.collectionBackups.foreach { coll =>
        coll.allSegments.foreach { seg =>
          buildV2Segment(seg, hadoopConf, bucket, applyDeletes) match {
            case Right(Some(v2)) => out += v2
            case Right(None) => // skipped (legacy storage or L0 w/o deletes)
            case Left(e)     => throw e
          }
        }
      }
      Right(out.toSeq)
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  /** Convert one backup segment into a `V2SegmentInfo` (or skip it).
    *
    * @return
    *   `Right(None)` for StorageV1 segments or (when `applyDeletes = false`) L0
    *   segments; `Right(Some(seg))` for readable StorageV2 segments.
    */
  private[read] def buildV2Segment(
      seg: SegmentBackup,
      hadoopConf: Configuration,
      bucket: String,
      applyDeletes: Boolean
  ): Either[Throwable, Option[V2SegmentInfo]] = {
    if (seg.storageVersion > 2L) {
      Left(
        new IllegalStateException(
          s"backup segment ${seg.segmentId} has storage_version=" +
            s"${seg.storageVersion} (StorageV3+); backup datasource only " +
            "supports StorageV2 packed parquet"
        )
      )
    } else if (seg.storageVersion < 2L) {
      logInfo(
        s"skipping backup segment ${seg.segmentId}: storage_version=" +
          s"${seg.storageVersion} (< 2, not StorageV2)"
      )
      Right(None)
    } else if (seg.isL0 && !applyDeletes) {
      logInfo(
        s"skipping StorageV2 L0 delete-only segment ${seg.segmentId} because " +
          "applyDeletes=false"
      )
      Right(None)
    } else if (seg.binlogs.isEmpty || seg.binlogs.forall(_.binlogs.isEmpty)) {
      logWarning(
        s"backup segment ${seg.segmentId} has no binlogs; emitting as empty " +
          "column-group list"
      )
      Right(
        Some(
          V2SegmentInfo(
            segmentId = seg.segmentId,
            partitionId = seg.partitionId,
            numOfRows = seg.numOfRows,
            storageVersion = seg.storageVersion,
            columnGroups = Seq.empty,
            deltaLogs = toDeltaLogs(seg, bucket)
          )
        )
      )
    } else {
      try {
        val columnGroups = seg.binlogs.map { fieldBinlog =>
          if (fieldBinlog.binlogs.isEmpty) {
            throw new IllegalStateException(
              s"backup segment ${seg.segmentId} has an empty binlogs entry " +
                s"(slot ${fieldBinlog.fieldId}) while other entries are " +
                "populated; cannot recover field ids for this column group"
            )
          }
          val sorted = fieldBinlog.binlogs.sortBy(_.logId)
          val paths =
            sorted.map(b => V2SegmentLoader.resolvePath(b.logPath, bucket))
          val fieldIds = MilvusParquetFooterReader.readFieldIdsFromSchema(
            paths.head,
            hadoopConf
          ) match {
            case Right(ids) => ids
            case Left(err) =>
              throw new RuntimeException(
                s"failed to read field ids from parquet ${paths.head} " +
                  s"(backup segment ${seg.segmentId}, slot " +
                  s"${fieldBinlog.fieldId}): ${err.getMessage}",
                err
              )
          }
          val rowCounts = paths.map { p =>
            MilvusParquetFooterReader.readRowCount(p, hadoopConf) match {
              case Right(n) => n
              case Left(err) =>
                throw new RuntimeException(
                  s"failed to read row count from parquet $p " +
                    s"(backup segment ${seg.segmentId}, slot " +
                    s"${fieldBinlog.fieldId}): ${err.getMessage}",
                  err
                )
            }
          }
          V2ColumnGroup(
            fieldIds = fieldIds,
            filePaths = paths,
            fileRowCounts = rowCounts,
            slotFieldId = fieldBinlog.fieldId
          )
        }
        Right(
          Some(
            V2SegmentInfo(
              segmentId = seg.segmentId,
              partitionId = seg.partitionId,
              numOfRows = seg.numOfRows,
              storageVersion = seg.storageVersion,
              columnGroups = columnGroups,
              deltaLogs = toDeltaLogs(seg, bucket)
            )
          )
        )
      } catch {
        case NonFatal(e) => Left(e)
      }
    }
  }

  private def toDeltaLogs(
      seg: SegmentBackup,
      bucket: String
  ): Seq[V2DeltaLogFile] =
    seg.deltalogs
      .flatMap(_.binlogs)
      .sortBy(_.logId)
      .map(b =>
        V2DeltaLogFile(
          logId = b.logId,
          logPath = V2SegmentLoader.resolvePath(b.logPath, bucket),
          entriesNum = b.entriesNum
        )
      )
}
