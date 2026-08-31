package com.zilliz.spark.connector.read

import java.util.concurrent.{
  Callable,
  ExecutorService,
  Executors,
  ThreadFactory
}
import java.util.concurrent.atomic.AtomicInteger
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
import org.apache.hadoop.fs.FileSystem
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
  *     copies of the Milvus storage objects, keyed at a deterministic layout
  *     derived from the backup dir plus segment IDs (see
  *     [[backupInsertLogPath]] / [[backupDeltaLogPath]]).
  *
  * This object translates the meta into the same runtime objects the snapshot
  * read path consumes (`V2SegmentInfo`), so the backup can be read offline with
  * the existing packed-V2 reader. The meta's `log_path` values are the
  * **original Milvus source keys** — milvus-backup copies each binlog into a
  * separate `DestKey` under the backup dir and records only the source key in
  * the meta — so object paths are always reconstructed from `backupDir` plus
  * the collection/partition/group/segment/field/log IDs, never taken from
  * `log_path`. Three gaps vs. a Milvus snapshot are closed here:
  *   1. milvus-backup persists only `log_size` per binlog, not `entries_num`.
  *      The per-file row count is recovered by reading each parquet footer's
  *      row count ([MilvusParquetFooterReader.readRowCount]), in parallel. 2.
  *      The AVRO segment-info (and hence the slot -> real field ID mapping) is
  *      not copied by the backup; the real field IDs are recovered from the
  *      **head file** of each column group via that file's own parquet schema
  *      ([MilvusParquetFooterReader.readFieldIdsAndRowCount]) — matching
  *      V2SegmentLoader, which assumes all files in a group share the schema.
  *      3. L0 delete-only segments are created by Milvus without a
  *      `StorageVersion` (0/omitted), so they are handled before any
  *      storage-version filtering.
  *
  * Only StorageV2 (packed parquet, `storage_version == 2`) data segments are
  * supported; anything else fails hard rather than returning a partial dataset.
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
    // Strip ALL trailing slashes: `.../b1//` is the same location as `.../b1`
    // but a different S3 key, so a single stripSuffix("/") would miss it.
    val stripped = base.replaceAll("/+$", "")
    s"$stripped/meta/full_meta.json"
  }

  /** Read and parse the backup's `full_meta.json`.
    *
    * The read is bounded ([[MilvusSnapshotReader.readUtf8WithLimit]]) so an
    * oversized or pathological meta fails on the driver instead of being
    * slurped into memory. No process-global cache is kept: the parse must
    * reflect the exact Hadoop configuration (credentials/endpoint) and the
    * current object at that path, and an unbounded cache would retain every
    * backup dir read over the driver's lifetime.
    */
  def readMeta(
      hadoopConf: Configuration,
      backupDir: String,
      maxBytes: Long = MilvusSnapshotReader.MaxSnapshotJsonBytes
  ): Either[Throwable, BackupInfo] = {
    var uri: java.net.URI = null
    var fs: FileSystem = null
    try {
      val metaFilePath = metaPath(backupDir)
      uri = new java.net.URI(metaFilePath)
      fs = FileSystem.get(uri, hadoopConf)
      val in = fs.open(new org.apache.hadoop.fs.Path(uri))
      try {
        val json =
          MilvusSnapshotReader.readUtf8WithLimit(in, metaFilePath, maxBytes)
        Right(mapper.readValue[BackupInfo](json))
      } finally {
        in.close()
      }
    } catch {
      case NonFatal(e) => Left(e)
    } finally {
      Option(uri)
        .flatMap(u => Option(u.getScheme))
        .foreach { scheme =>
          if (
            fs != null && hadoopConf.getBoolean(
              s"fs.$scheme.impl.disable.cache",
              false
            )
          ) {
            fs.close()
          }
        }
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

  /** Serialize a parsed `BackupInfo` back to JSON. Used to thread the meta from
    * table init to the scan planner so it is read/parsed only once.
    */
  def serialize(info: BackupInfo): String = mapper.writeValueAsString(info)

  // -------------------------------------------------------------------------
  // Schema conversion
  // -------------------------------------------------------------------------

  /** Convert a backup `CollectionSchema` into Milvus protobuf
    * `CollectionSchema` bytes — the format the packed-V2 reader expects.
    *
    * Rejects a dynamic collection whose meta lacks the `$meta` field: a default
    * backup only records it when created with etcd access
    * (`--backup_index_extra`), and without it the reader would silently return
    * a null `$meta` column while dropping the real field from the column
    * groups.
    */
  def toProtobufSchemaBytes(schema: BackupCollectionSchema): Array[Byte] = {
    validateDynamicFieldSchema(schema)
    // Drop system fields by field ID (0 = RowID, 1 = Timestamp), never by name:
    // milvus-backup's schema comes from DescribeCollection, which only returns
    // user fields, so a legitimately-named user field (e.g. a PK literally
    // called "RowID") must survive and only true system IDs are removed.
    val userFields =
      schema.fields.filterNot(f => f.fieldId == 0L || f.fieldId == 1L)
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

  /** Validate a backup collection schema before planning. Currently rejects a
    * dynamic collection whose meta has no `$meta` field (a default backup only
    * records it with etcd access) — reading it would silently return a null
    * `$meta` column.
    */
  private[connector] def validateDynamicFieldSchema(
      schema: BackupCollectionSchema
  ): Unit = {
    if (schema.enableDynamicField && !schema.fields.exists(_.name == "$meta")) {
      throw new IllegalArgumentException(
        s"backup collection '${schema.name}' has enable_dynamic_field=true but " +
          "its meta does not record the '$meta' field; re-create the backup " +
          "with etcd access so the dynamic field schema is captured " +
          "(--backup_index_extra requires milvus-backup >= v0.5.13; on " +
          "v0.5.10-v0.5.12 the flag does not capture $meta)"
      )
    }
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

  /** `_allPartitionID` as used by milvus-backup for collection-wide L0
    * segments.
    */
  private val AllPartitionId = -1L

  private val FooterReadThreadCount: Int = {
    val cores = Runtime.getRuntime.availableProcessors()
    math.max(2, math.min(cores, 8))
  }

  /** Bounded, daemon thread pool for segment-level conversion (each segment's
    * footer reads are independent). Only column groups with a single binlog
    * file are supported, so each segment does one footer read per column group
    * and cross-segment parallelism is what keeps a large backup from stalling
    * the driver.
    */
  private val SegmentReadPool: ExecutorService = Executors.newFixedThreadPool(
    FooterReadThreadCount,
    daemonThreadFactory("backup-segment-read")
  )

  private def daemonThreadFactory(namePrefix: String): ThreadFactory =
    new ThreadFactory {
      private val counter = new AtomicInteger(0)
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"$namePrefix-${counter.incrementAndGet()}")
        t.setDaemon(true)
        t
      }
    }

  /** Build the runtime `V2SegmentInfo` list for the segments of the collection
    * identified by `collectionId`. L0 delete-only segments keep an empty
    * `columnGroups` so they feed the inherited delete-plan path, exactly like
    * the snapshot reader.
    *
    * @param backupDir
    *   The full `milvus.backup.dir` (e.g. `s3a://bucket/backup/<name>` or a
    *   local path). Backup object paths are **reconstructed** from this plus
    *   the segment's collection/partition/group/segment/field/log IDs — the
    *   `log_path` values in the meta are the original Milvus source keys and
    *   must never be read directly.
    * @param collectionId
    *   Only the segments of this collection are materialized; a backup holding
    *   several collections must never leak another collection's segments into
    *   the read.
    */
  def toV2Segments(
      info: BackupInfo,
      hadoopConf: Configuration,
      backupDir: String,
      applyDeletes: Boolean = true,
      collectionId: Long
  ): Either[Throwable, Seq[V2SegmentInfo]] = {
    var fs: FileSystem = null
    try {
      if (info.isSnapshotFormat) {
        throw new IllegalStateException(
          s"backup '${info.name}' is in snapshot format; only binlog-format " +
            "backups can be read as a datasource"
        )
      }
      // One FileSystem reused across every segment's footer reads: with
      // fs.s3a.impl.disable.cache=true, per-file FileSystem.get would otherwise
      // construct a whole S3A client + thread pool for each binlog.
      fs = FileSystem.get(new java.net.URI(backupBase(backupDir)), hadoopConf)
      val out = scala.collection.mutable.ArrayBuffer.empty[V2SegmentInfo]
      info.collectionBackups
        .filter(_.collectionId == collectionId)
        .foreach { coll =>
          // Segments are independent, so their footer reads run in parallel on
          // the shared pool; with the common "one file per group" shape each
          // segment still costs a serial head read, so parallelism across
          // segments (not just across a group's tail files) is what keeps a
          // large backup from stalling the driver.
          val futures = coll.allSegments.map { seg =>
            SegmentReadPool.submit(
              new Callable[Either[Throwable, Option[V2SegmentInfo]]] {
                override def call(): Either[Throwable, Option[V2SegmentInfo]] =
                  buildV2SegmentWithFs(seg, fs, backupDir, applyDeletes)
              }
            )
          }
          futures.foreach { f =>
            f.get() match {
              case Right(Some(v2)) => out += v2
              case Right(None)     => // L0 segment skipped (applyDeletes=false)
              case Left(e)         =>
                // One bad segment shouldn't leave a burst of wasted driver-side
                // S3 footer reads running after the read has already failed.
                futures.foreach(_.cancel(true))
                throw e
            }
          }
        }
      Right(out.toSeq)
    } catch {
      case NonFatal(e) => Left(e)
    } finally {
      closeIfNotCached(fs, hadoopConf)
    }
  }

  /** Close a `FileSystem` only when its scheme has the cache disabled (i.e. the
    * instance was created fresh for this read). A cached scheme (e.g. the
    * process-wide `LocalFileSystem` for local paths) must NOT be closed: that
    * evicts it from `FileSystem.CACHE` and fails every other holder with
    * `IOException: Filesystem closed`.
    */
  private def closeIfNotCached(
      fs: FileSystem,
      hadoopConf: Configuration
  ): Unit = {
    try {
      if (fs != null) {
        val scheme = fs.getUri.getScheme
        if (
          scheme != null && hadoopConf
            .getBoolean(s"fs.$scheme.impl.disable.cache", false)
        ) {
          fs.close()
        }
      }
    } catch {
      case NonFatal(_) => // close is best-effort; the read result already won
    }
  }

  /** Convert one backup segment into a `V2SegmentInfo` (or skip it), reusing
    * the caller-supplied `FileSystem` for all footer reads (a backup read opens
    * one FS for all segments, so per-file S3A client construction is avoided).
    *
    * L0 delete-only segments are handled before any storage-version filtering:
    * Milvus creates them without a `StorageVersion` (0/omitted in the meta), so
    * they must not fall into the StorageV1 skip path.
    *
    * @return
    *   `Right(None)` for an L0 segment when `applyDeletes = false`;
    *   `Right(Some(seg))` for readable segments; `Left` for unsupported data
    *   segments (fails hard rather than returning a partial dataset).
    */
  private[read] def buildV2SegmentWithFs(
      seg: SegmentBackup,
      fs: FileSystem,
      backupDir: String,
      applyDeletes: Boolean
  ): Either[Throwable, Option[V2SegmentInfo]] = {
    if (seg.isL0) {
      if (!applyDeletes) {
        logInfo(
          s"skipping L0 delete-only segment ${seg.segmentId} because " +
            "applyDeletes=false"
        )
        Right(None)
      } else {
        Right(Some(emptyColumnGroupSegment(seg, backupDir)))
      }
    } else if (seg.storageVersion != 2L) {
      val storageVersionNote =
        if (seg.storageVersion == 0L) {
          "0/absent means StorageV1 (legacy per-field binlogs)"
        } else {
          s"${seg.storageVersion}"
        }
      Left(
        new IllegalStateException(
          s"backup segment ${seg.segmentId} has storage_version=" +
            s"$storageVersionNote; backup datasource only supports " +
            "StorageV2 (packed parquet) data segments"
        )
      )
    } else if (seg.binlogs.isEmpty || seg.binlogs.forall(_.binlogs.isEmpty)) {
      if (seg.numOfRows == 0L) {
        logWarning(
          s"backup segment ${seg.segmentId} has no binlogs and no rows; " +
            "emitting as empty column-group list"
        )
        Right(Some(emptyColumnGroupSegment(seg, backupDir)))
      } else {
        Left(
          new IllegalStateException(
            s"backup segment ${seg.segmentId} has no binlogs but " +
              s"num_of_rows=${seg.numOfRows}; refusing to silently drop rows"
          )
        )
      }
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
          // Guard: a column group spanning multiple binlog files is rejected.
          // milvus-storage's BuildLoonColumnGroups encodes per-file row counts
          // as group-cumulative ranges, which the packed reader intersects
          // against each file's own zero-based row-group offsets, so file i > 0
          // is silently truncated (zero rows when earlier files are at least as
          // large). Refuse loudly rather than return a short DataFrame; the fix
          // belongs in milvus-storage (per-file, not cumulative, indices).
          if (sorted.size > 1) {
            throw new IllegalStateException(
              s"backup segment ${seg.segmentId} has a column group (slot " +
                s"${fieldBinlog.fieldId}) spanning ${sorted.size} binlog files; " +
                "multi-file column groups are unsupported until " +
                "milvus-storage BuildLoonColumnGroups is fixed (per-file, not " +
                "group-cumulative, row ranges)"
            )
          }
          // Hadoop-qualified paths for the footer reads; the native reader
          // gets bucket-relative keys in `filePaths`.
          val hadoopPaths = sorted.map(b =>
            qualifiedInsertLogPath(backupDir, seg, fieldBinlog.fieldId, b.logId)
          )
          val nativePaths = sorted.map(b =>
            nativeInsertLogPath(backupDir, seg, fieldBinlog.fieldId, b.logId)
          )
          // The (single) file's footer is read once for both the real field IDs
          // (which live in the parquet schema, not the backup meta) and its row
          // count.
          val headInfo = MilvusParquetFooterReader.readFieldIdsAndRowCount(
            fs,
            hadoopPaths.head
          ) match {
            case Right(info) => info
            case Left(err) =>
              throw new RuntimeException(
                s"failed to read footer of parquet ${hadoopPaths.head} " +
                  s"(backup segment ${seg.segmentId}, slot " +
                  s"${fieldBinlog.fieldId}): ${err.getMessage}",
                err
              )
          }
          V2ColumnGroup(
            fieldIds = headInfo.fieldIds,
            filePaths = nativePaths,
            fileRowCounts = Seq(headInfo.rowCount),
            slotFieldId = fieldBinlog.fieldId
          )
        }
        // Driver-side validation to keep the no-partial-read contract: fail a
        // malformed backup here instead of dispatching tasks that die late in
        // the native reader (or silently returning an empty DataFrame).
        if (columnGroups.exists(_.fieldIds.isEmpty)) {
          throw new IllegalStateException(
            s"backup segment ${seg.segmentId} has a column group with no " +
              "field ids; refusing malformed backup"
          )
        }
        val groupTotals = columnGroups.map(_.fileRowCounts.sum)
        if (groupTotals.distinct.size > 1) {
          throw new IllegalStateException(
            s"backup segment ${seg.segmentId} column groups have " +
              s"inconsistent row totals: ${groupTotals.mkString(",")}"
          )
        }
        val totalRows = groupTotals.headOption.getOrElse(0L)
        if (seg.numOfRows > 0L && totalRows <= 0L) {
          throw new IllegalStateException(
            s"backup segment ${seg.segmentId} has num_of_rows=${seg.numOfRows} " +
              "but the parquet footers recovered 0 rows"
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
              deltaLogs = toDeltaLogs(seg, backupDir)
            )
          )
        )
      } catch {
        case NonFatal(e) => Left(e)
      }
    }
  }

  /** [[buildV2SegmentWithFs]] opening a fresh `FileSystem` from the config —
    * for tests / single-segment use. Batch reads should go through
    * [[toV2Segments]], which opens one `FileSystem` for all segments and reuses
    * it across every footer read.
    */
  private[read] def buildV2Segment(
      seg: SegmentBackup,
      hadoopConf: Configuration,
      backupDir: String,
      applyDeletes: Boolean
  ): Either[Throwable, Option[V2SegmentInfo]] = {
    var fs: FileSystem = null
    try {
      fs = FileSystem.get(new java.net.URI(backupBase(backupDir)), hadoopConf)
      buildV2SegmentWithFs(seg, fs, backupDir, applyDeletes)
    } finally {
      closeIfNotCached(fs, hadoopConf)
    }
  }

  private def emptyColumnGroupSegment(
      seg: SegmentBackup,
      backupDir: String
  ): V2SegmentInfo =
    V2SegmentInfo(
      segmentId = seg.segmentId,
      partitionId = seg.partitionId,
      numOfRows = seg.numOfRows,
      storageVersion = seg.storageVersion,
      columnGroups = Seq.empty,
      deltaLogs = toDeltaLogs(seg, backupDir)
    )

  private def toDeltaLogs(
      seg: SegmentBackup,
      backupDir: String
  ): Seq[V2DeltaLogFile] =
    seg.deltalogs
      .flatMap(_.binlogs)
      .sortBy(_.logId)
      .map(b =>
        V2DeltaLogFile(
          logId = b.logId,
          logPath = qualifiedDeltaLogPath(backupDir, seg, b.logId),
          entriesNum = b.entriesNum
        )
      )

  /** Build the delete-only `V2SegmentInfo` list for a collection with no footer
    * reads. Used by the reader factory to compute the shared partition-scoped
    * delete plans independently of partition planning, so delete handling does
    * not depend on Spark evaluating partitions first.
    *
    * The predicate MUST match the planner's `inheritedDeleteSegments`
    * (`columnGroups.isEmpty && deltaLogs.nonEmpty` after
    * `buildV2SegmentWithFs`): every L0 segment, plus a non-L0 StorageV2 segment
    * with no binlogs and zero rows (emitted as an empty column-group segment).
    * Otherwise a partition can be stamped with an inherited-delete marker that
    * has no matching plan entry and silently resolves to
    * `MilvusDeletePlan.empty`.
    */
  private[connector] def deleteOnlySegments(
      info: BackupInfo,
      collectionId: Long,
      backupDir: String
  ): Seq[V2SegmentInfo] =
    info.collectionBackups
      .filter(_.collectionId == collectionId)
      .flatMap(_.allSegments)
      .filter(isDeleteOnlySegment)
      .map(seg => emptyColumnGroupSegment(seg, backupDir))

  private def isDeleteOnlySegment(seg: SegmentBackup): Boolean = {
    val hasDeltas = seg.deltalogs.exists(_.binlogs.nonEmpty)
    val binlogsEmpty =
      seg.binlogs.isEmpty || seg.binlogs.forall(_.binlogs.isEmpty)
    hasDeltas && (seg.isL0 ||
      (seg.storageVersion == 2L && binlogsEmpty && seg.numOfRows == 0L))
  }

  // -------------------------------------------------------------------------
  // Backup path reconstruction
  // -------------------------------------------------------------------------

  /** milvus-backup copies each binlog into a `DestKey` under the backup dir and
    * writes only the source key into `full_meta.json`; the meta's `log_path`
    * values therefore point at the original Milvus storage and must never be
    * opened directly. The backup copies live at a deterministic layout derived
    * from the backup dir plus the segment IDs — mirroring `coll_dml_task.go`'s
    * `insertLogsAttrs` / `deltaLogAttrs`:
    *
    * {{{
    *   insert: {backupDir}/binlogs/insert_log/{coll}/{part}/{group}/{seg}/{field}/{log}
    *   delta:  {backupDir}/binlogs/delta_log/{coll}/{part}/{seg}/{log}            (part == -1)
    *           {backupDir}/binlogs/delta_log/{coll}/{part}/{group}/{seg}/{log}    (part != -1)
    * }}}
    *
    * Two path forms are produced:
    *   - **qualified** (`qualifiedInsertLogPath` / `qualifiedDeltaLogPath`),
    *     e.g. `s3a://bucket/backup/b1/binlogs/...`, consumed by the Hadoop-side
    *     reads (parquet footers, delta logs).
    *   - **native** (`nativeInsertLogPath`), e.g. `backup/b1/binlogs/...`,
    *     consumed by the milvus-storage native packed reader. Its
    *     `FilesystemCache::resolve_config` rejects scheme-qualified URIs (it
    *     would demand `extfs.*` config) and the filesystem proxy prepends
    *     `fs.bucket_name`, so the column-group file keys must be
    *     bucket-relative.
    */
  private def backupBase(backupDir: String): String = {
    Option(backupDir)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.replaceAll("/+$", ""))
      .getOrElse(
        throw new IllegalArgumentException("backup dir must not be empty")
      )
  }

  /** Bucket-relative base of the backup dir: strips `scheme://bucket/` for S3
    * URIs (e.g. `s3a://bucket/backup/b1` -> `backup/b1`); local dirs are
    * returned unchanged. This is the key form the native reader expects.
    */
  private[read] def backupKeyBase(backupDir: String): String = {
    val base = backupBase(backupDir)
    val schemeIdx = base.indexOf("://")
    if (schemeIdx < 0) {
      base
    } else {
      val rest = base.substring(schemeIdx + 3)
      val slash = rest.indexOf('/')
      if (slash < 0) {
        ""
      } else if (slash == 0) {
        // Empty authority (e.g. file:///data/backup/b1): keep the leading
        // slash so both spellings of the same local location agree.
        rest
      } else {
        rest.substring(slash + 1)
      }
    }
  }

  /** Join a path prefix with "/" without introducing a leading slash when the
    * prefix is empty (a backup at the bucket root has an empty bucket-relative
    * prefix, and `/binlogs/...` is a different S3 key than `binlogs/...`).
    */
  private def joinPrefix(prefix: String): String =
    if (prefix.isEmpty) "" else prefix + "/"

  private def insertLogPath(
      prefix: String,
      seg: SegmentBackup,
      slotFieldId: Long,
      logId: Long
  ): String =
    s"${joinPrefix(prefix)}binlogs/insert_log/${seg.collectionId}/" +
      s"${seg.partitionId}/${seg.groupId}/${seg.segmentId}/$slotFieldId/$logId"

  private def deltaLogPath(
      prefix: String,
      seg: SegmentBackup,
      logId: Long
  ): String =
    if (seg.partitionId == AllPartitionId) {
      s"${joinPrefix(prefix)}binlogs/delta_log/${seg.collectionId}/" +
        s"${seg.partitionId}/${seg.segmentId}/$logId"
    } else {
      s"${joinPrefix(prefix)}binlogs/delta_log/${seg.collectionId}/" +
        s"${seg.partitionId}/${seg.groupId}/${seg.segmentId}/$logId"
    }

  /** Hadoop-qualified insert-log path (e.g.
    * `s3a://bucket/backup/b1/binlogs/...`).
    */
  private[read] def qualifiedInsertLogPath(
      backupDir: String,
      seg: SegmentBackup,
      slotFieldId: Long,
      logId: Long
  ): String = insertLogPath(backupBase(backupDir), seg, slotFieldId, logId)

  /** Native-reader bucket-relative insert-log key (e.g.
    * `backup/b1/binlogs/...`).
    */
  private[read] def nativeInsertLogPath(
      backupDir: String,
      seg: SegmentBackup,
      slotFieldId: Long,
      logId: Long
  ): String = insertLogPath(backupKeyBase(backupDir), seg, slotFieldId, logId)

  /** Hadoop-qualified delta-log path. Delta logs only feed the Hadoop-side
    * delete-plan reader, so no native form is produced.
    */
  private[read] def qualifiedDeltaLogPath(
      backupDir: String,
      seg: SegmentBackup,
      logId: Long
  ): String = deltaLogPath(backupBase(backupDir), seg, logId)
}
