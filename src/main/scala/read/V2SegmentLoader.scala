package com.zilliz.spark.connector.read

import java.io.ByteArrayOutputStream
import java.net.URI
import scala.util.control.NonFatal

import com.zilliz.spark.connector.MilvusStoragePath
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging

/** High-level loader for StorageV2 (non-manifest packed parquet) segments.
  *
  * Given the list of per-segment AVRO paths from
  * `SnapshotMetadata.manifestList` and the S3 bucket where those files live,
  * this object:
  *
  *   1. Fetches each AVRO via Hadoop FS. 2. Decodes with
  *      [[MilvusSegmentManifestReader]]. 3. Skips entries whose
  *      `storage_version != 2` (V1/V3 are handled elsewhere). 4. For each V2
  *      entry, reads exactly one parquet footer's `group_field_id_list`
  *      kv-metadata to recover the segment's column-group layout
  *      ([[MilvusParquetFooterReader]]). 5. Calls
  *      `MilvusSegmentManifestReader.toV2SegmentInfo` to join the two.
  *
  * The resulting `Seq[V2SegmentInfo]` is the runtime view consumed by
  * `MilvusPackedV2InputPartition` / `MilvusPackedV2PartitionReader`.
  *
  * Path resolution: AVRO and parquet paths that Milvus writes are
  * bucket-relative (`files/snapshots/...`). When `bucket` is non-empty we
  * prefix `{storageScheme}://{bucket}/`. The default remains `s3a` for the
  * connector DataSource and tools; Alibaba callers must explicitly pass
  * `storageScheme = "oss"` with a matching Hadoop OSS configuration. Explicit
  * `s3://` and `s3a://` paths are aliases rewritten to the requested scheme.
  */
object V2SegmentLoader extends Logging {

  /** Load all V2 segments referenced by an AVRO manifest list.
    *
    * @param manifestPaths
    *   Bucket-relative (or fully-qualified) paths as they appear in
    *   `SnapshotMetadata.manifestList`.
    * @param bucket
    *   S3 bucket that holds both the AVRO files and the segment parquet files.
    *   Empty string is accepted for unit-test / local-FS usage.
    * @param hadoopConf
    *   Pre-configured Hadoop `Configuration` (per-bucket S3A creds/endpoint
    *   already set by the caller).
    * @return
    *   `Right(segments)` on success; `Left(firstError)` on the first
    *   unrecoverable failure.
    */
  def loadV2Segments(
      manifestPaths: Seq[String],
      bucket: String,
      hadoopConf: Configuration,
      manifestSchemaVersion: Int = 1,
      applyDeletes: Boolean = true,
      storageScheme: String = "s3a",
      endpoint: String = ""
  ): Either[Throwable, Seq[V2SegmentInfo]] = {
    try {
      val out = scala.collection.mutable.ArrayBuffer.empty[V2SegmentInfo]
      manifestPaths.foreach { rawPath =>
        val avroPath =
          resolvePath(rawPath, bucket, storageScheme, endpoint)
        val avroBytes = readAllBytes(hadoopConf, avroPath)
        val entry =
          MilvusSegmentManifestReader
            .parse(avroBytes, manifestSchemaVersion) match {
            case Right(e) => e
            case Left(err) =>
              throw new RuntimeException(
                s"failed to decode segment manifest $avroPath: ${err.getMessage}",
                err
              )
          }
        buildV2SegmentInfoFromEntry(
          entry,
          bucket,
          hadoopConf,
          applyDeletes,
          storageScheme,
          endpoint
        ) match {
          case Right(Some(seg)) => out += seg
          case Right(None)      => // skipped (storage version != 2)
          case Left(err)        => throw err
        }
      }
      Right(out.toSeq)
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  /** Convert one parsed AVRO entry into a runtime `V2SegmentInfo`. Extracted
    * for unit-testability — it needs only Hadoop FS, so local parquet files + a
    * hand-built `AvroManifestEntry` cover the full behavior matrix without
    * minio/S3.
    *
    * @return
    *   `Right(Some(seg))` for a StorageV2 entry (including "all-empty" which
    *   emits a segment with no column groups); `Right(None)` when the entry is
    *   not StorageV2 and should be skipped; `Left(err)` with segment/slot
    *   context on any unrecoverable failure.
    */
  private[read] def buildV2SegmentInfoFromEntry(
      entry: AvroManifestEntry,
      bucket: String,
      hadoopConf: Configuration,
      applyDeletes: Boolean = true,
      storageScheme: String = "s3a",
      endpoint: String = ""
  ): Either[Throwable, Option[V2SegmentInfo]] = {
    val resolvedEntry = resolveEntryPaths(entry, bucket, storageScheme, endpoint)
    val isL0 = resolvedEntry.segmentLevel == 1L

    if (resolvedEntry.storageVersion != 2L) {
      logInfo(
        s"skipping segment ${resolvedEntry.segmentId}: storage_version=${resolvedEntry.storageVersion} " +
          s"(!= 2); V2SegmentLoader only handles StorageV2"
      )
      Right(None)
    } else if (isL0 && !applyDeletes) {
      logInfo(
        s"skipping StorageV2 L0 delete-only segment ${resolvedEntry.segmentId} because applyDeletes=false"
      )
      Right(None)
    } else if (
      resolvedEntry.binlogFiles.isEmpty ||
      resolvedEntry.binlogFiles.forall(_.binlogs.isEmpty)
    ) {
      logWarning(
        s"segment ${resolvedEntry.segmentId} has no binlog files with entries; " +
          s"emitting as empty column-group list"
      )
      Right(
        Some(
          V2SegmentInfo(
            segmentId = resolvedEntry.segmentId,
            partitionId = resolvedEntry.partitionId,
            numOfRows = resolvedEntry.numOfRows,
            storageVersion = resolvedEntry.storageVersion,
            columnGroups = Seq.empty,
            deltaLogs = resolvedEntry.deltaLogFiles
              .flatMap(_.binlogs)
              .sortBy(_.logId)
              .map(log =>
                V2DeltaLogFile(
                  logId = log.logId,
                  logPath = log.logPath,
                  entriesNum = log.entriesNum
                )
              )
          )
        )
      )
    } else {
      try {
        // Per-entry field-id recovery: each V2 parquet file holds exactly
        // one column group, and its schema's top-level columns ARE that
        // group's field IDs. Reading per entry (rather than reusing a
        // single footer's segment-level `group_field_id_list`) is required
        // because a segment that has been backfilled contains parquets
        // from multiple write sessions, each advertising only its own
        // session's groups — see MilvusParquetFooterReader.readFieldIdsFromSchema.
        val groupFieldIdListPerEntry: Seq[Seq[Long]] =
          resolvedEntry.binlogFiles.map { afb =>
            if (afb.binlogs.isEmpty) {
              // The top-level guard above only rejects the all-empty case.
              // A partial-empty entry alongside populated ones points at a
              // corrupt manifest (we have no parquet to recover field ids
              // from, and the downstream V2ColumnGroup would be a silent
              // empty shell). Fail loudly with slot/segment context.
              throw new IllegalStateException(
                s"segment ${resolvedEntry.segmentId} has an empty binlog_files entry " +
                  s"(slot ${afb.slotFieldId}) while other entries are populated; " +
                  "cannot recover field ids for this column group — refusing to " +
                  "emit an empty V2ColumnGroup from a partial manifest"
              )
            }
            val samplePath = afb.binlogs.head.logPath
            MilvusParquetFooterReader
              .readFieldIdsFromSchema(samplePath, hadoopConf) match {
              case Right(ids) => ids
              case Left(err) =>
                throw new RuntimeException(
                  s"failed to read field ids from parquet $samplePath " +
                    s"(segment ${resolvedEntry.segmentId}, slot ${afb.slotFieldId}): " +
                    err.getMessage,
                  err
                )
            }
          }
        MilvusSegmentManifestReader.toV2SegmentInfo(
          resolvedEntry,
          groupFieldIdListPerEntry
        ) match {
          case Right(seg) => Right(Some(seg))
          case Left(err) =>
            Left(
              new RuntimeException(
                s"failed to build V2SegmentInfo for segment ${resolvedEntry.segmentId}: " +
                  err.getMessage,
                err
              )
            )
        }
      } catch {
        case NonFatal(e) => Left(e)
      }
    }
  }

  private def resolveEntryPaths(
      entry: AvroManifestEntry,
      bucket: String,
      storageScheme: String,
      endpoint: String = ""
  ): AvroManifestEntry = {
    def resolveFieldBinlogs(
        fieldBinlogs: Seq[AvroFieldBinlogEntry]
    ): Seq[AvroFieldBinlogEntry] =
      fieldBinlogs.map(fieldBinlog =>
        fieldBinlog.copy(binlogs =
          fieldBinlog.binlogs.map(log =>
            log.copy(
              logPath = resolvePath(log.logPath, bucket, storageScheme, endpoint)
            )
          )
        )
      )

    entry.copy(
      binlogFiles = resolveFieldBinlogs(entry.binlogFiles),
      deltaLogFiles = resolveFieldBinlogs(entry.deltaLogFiles)
    )
  }

  /** Prefix `bucket` when `path` has no scheme; pass through s3a:// / s3://.
    *
    * `endpoint` is the configured storage endpoint (e.g. `fs.address` /
    * `s3Endpoint`); it lets [[MilvusStoragePath.toStandardS3Path]] recognize
    * port-less endpoint-prefixed Milvus-format URIs. `storageScheme` is
    * intentionally explicit for non-S3 providers because the generic connector
    * read path does not yet derive a provider from its public options.
    */
  def resolvePath(
      path: String,
      bucket: String,
      storageScheme: String = "s3a",
      endpoint: String = ""
  ): String = {
    val scheme = storageScheme.stripSuffix("://")
    if (path == null) path
    else if (path.startsWith("s3a://") || path.startsWith("s3://")) {
      // Canonicalize Milvus-format s3://<address>/<bucket>/<key> to
      // s3a://bucket/key, then re-apply the target storage scheme (e.g. oss
      // for Aliyun).
      val canonical = MilvusStoragePath.toStandardS3Path(
        path,
        endpoint = endpoint,
        configuredBucket = bucket
      )
      if (scheme == "s3a") canonical
      else scheme + "://" + canonical.stripPrefix("s3a://")
    } else if (path.contains("://")) path
    else if (bucket != null && bucket.nonEmpty)
      s"$scheme://$bucket/${path.stripPrefix("/")}"
    else path
  }

  private[read] def readAllBytes(
      conf: Configuration,
      fullyQualifiedPath: String
  ): Array[Byte] = {
    var uri: URI = null
    var fs: FileSystem = null
    try {
      uri = new URI(fullyQualifiedPath)
      fs = FileSystem.get(uri, conf)
      val in = fs.open(new Path(uri))
      try {
        val out = new ByteArrayOutputStream()
        val buf = new Array[Byte](8192)
        var n = in.read(buf)
        while (n >= 0) {
          out.write(buf, 0, n)
          n = in.read(buf)
        }
        out.toByteArray
      } finally {
        in.close()
      }
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(
          s"failed to read bytes from $fullyQualifiedPath: ${e.getMessage}",
          e
        )
    } finally {
      Option(uri).flatMap(uri => Option(uri.getScheme)).foreach { scheme =>
        if (
          fs != null && conf.getBoolean(s"fs.$scheme.impl.disable.cache", false)
        ) {
          fs.close()
        }
      }
    }
  }
}
