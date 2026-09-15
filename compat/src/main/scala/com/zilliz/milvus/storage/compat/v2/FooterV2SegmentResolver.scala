package com.zilliz.milvus.storage.compat.v2

import scala.util.control.NonFatal

import com.zilliz.milvus.storage.compat.ParquetFooterReader
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.manifest.{
  AvroFieldBinlogEntry,
  AvroManifestEntry
}
import com.zilliz.milvus.storage.manifest.SegmentManifestReader
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.DeltaLogFile
import com.zilliz.milvus.storage.snapshot.V2SegmentInfo

/** High-level loader for StorageV2 (non-manifest packed parquet) segments.
  *
  * Given the list of per-segment AVRO paths from `SnapshotJson.manifestList`
  * and the S3 bucket where those files live, this object:
  *
  *   1. Fetches each AVRO via Hadoop FS. 2. Decodes with
  *      [[SegmentManifestReader]]. 3. Skips entries whose `storage_version !=
  *      2` (V1/V3 are handled elsewhere). 4. For each V2 entry, reads exactly
  *      one parquet footer's `group_field_id_list` kv-metadata to recover the
  *      segment's column-group layout ([[ParquetFooterReader]]). 5. Calls
  *      `SegmentManifestReader.toV2SegmentInfo` to join the two.
  *
  * The resulting `Seq[V2SegmentInfo]` is the runtime view consumed by
  * `MilvusV2InputPartition` / `MilvusV2PartitionReader`.
  *
  * Path resolution: AVRO and parquet paths that Milvus writes are
  * bucket-relative (`files/snapshots/...`). When `bucket` is non-empty we
  * prefix `{storageScheme}://{bucket}/`. The default remains `s3a` for the
  * connector DataSource and tools; Alibaba callers must explicitly pass
  * `storageScheme = "oss"` with a matching Hadoop OSS configuration. Explicit
  * `s3://` and `s3a://` paths are aliases rewritten to the requested scheme.
  */
object FooterV2SegmentResolver extends com.zilliz.milvus.storage.Logging {

  /** Load all V2 segments referenced by an AVRO manifest list.
    *
    * @param manifestPaths
    *   Bucket-relative (or fully-qualified) paths as they appear in
    *   `SnapshotJson.manifestList`.
    * @param bucket
    *   S3 bucket that holds both the AVRO files and the segment parquet files.
    *   Empty string is accepted for unit-test / local-FS usage.
    * @param store
    *   Bound to the bucket and credentials these paths need.
    * @return
    *   `Right(segments)` on success; `Left(firstError)` on the first
    *   unrecoverable failure.
    */
  def loadV2Segments(
      manifestPaths: Seq[String],
      bucket: String,
      store: ObjectStore,
      manifestSchemaVersion: Int = 1,
      applyDeletes: Boolean = true,
      storageScheme: String = "s3a"
  ): Either[Throwable, Seq[V2SegmentInfo]] = {
    try {
      val out = scala.collection.mutable.ArrayBuffer.empty[V2SegmentInfo]
      manifestPaths.foreach { rawPath =>
        val avroPath = StoragePath.parse(rawPath, bucket).key
        val avroBytes = store.readAll(avroPath)
        val entry =
          SegmentManifestReader
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
          store,
          applyDeletes,
          storageScheme
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
  def buildV2SegmentInfoFromEntry(
      entry: AvroManifestEntry,
      bucket: String,
      store: ObjectStore,
      applyDeletes: Boolean = true,
      storageScheme: String = "s3a"
  ): Either[Throwable, Option[V2SegmentInfo]] = {
    val resolvedEntry = resolveEntryPaths(entry, bucket, storageScheme)
    val isL0 = resolvedEntry.segmentLevel == 1L

    if (resolvedEntry.storageVersion != 2L) {
      logInfo(
        s"skipping segment ${resolvedEntry.segmentId}: storage_version=${resolvedEntry.storageVersion} " +
          s"(!= 2); FooterV2SegmentResolver only handles StorageV2"
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
                DeltaLogFile(
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
        // session's groups — see ParquetFooterReader.readFieldIdsFromSchema.
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
            // The entry's paths were resolved to full URIs above for the
            // executors; the store is bound to the bucket and takes the key.
            ParquetFooterReader
              .readFieldIdsFromSchema(
                StoragePath.parse(samplePath, bucket).key,
                store
              ) match {
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
        SegmentManifestReader.toV2SegmentInfo(
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
      storageScheme: String
  ): AvroManifestEntry = {
    def resolveFieldBinlogs(
        fieldBinlogs: Seq[AvroFieldBinlogEntry]
    ): Seq[AvroFieldBinlogEntry] =
      fieldBinlogs.map(fieldBinlog =>
        fieldBinlog.copy(binlogs =
          fieldBinlog.binlogs.map(log =>
            log.copy(logPath =
              StoragePath.parse(log.logPath, bucket).uri(storageScheme)
            )
          )
        )
      )

    entry.copy(
      binlogFiles = resolveFieldBinlogs(entry.binlogFiles),
      deltaLogFiles = resolveFieldBinlogs(entry.deltaLogFiles)
    )
  }
}
