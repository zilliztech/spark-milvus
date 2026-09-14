package com.zilliz.spark.connector.scan

import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.storage.compat.backup.BackupMetaReader
import com.zilliz.milvus.storage.delete.MilvusDeltaLogReader
import com.zilliz.spark.connector.options.{BackupSelection, StorageOptions}
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.CollectionSchema

/** Backup mode: a milvus-backup binlog-format export is read offline. Its
  * `full_meta.json` is translated into the same `V2SegmentInfo` the snapshot
  * path uses, so delete planning and the packed-V2 reader are shared.
  */
private[scan] final class BackupPlanner(
    ctx: ScanContext,
    preParsedBackupMeta: Option[BackupMetaReader.BackupInfo]
) extends PartitionPlanner(ctx) {

  /** Plan input partitions from a milvus-backup binlog-format export (offline,
    * no Milvus client connection).
    *
    * `[[MilvusOption.BackupDir]]` points at the backup directory (the parent of
    * `meta/full_meta.json`). The meta is translated by [[BackupMetaReader]]
    * into the same `V2SegmentInfo` objects the snapshot path uses, then handed
    * to the shared [[buildSnapshotPartitions]] so delete planning, extra
    * columns, vector projection and the packed-V2 reader are all reused.
    */
  def plan(): Array[InputPartition] = {
    val backupDir = MilvusOption.backupDir(options).getOrElse {
      throw new IllegalArgumentException(
        s"${MilvusOption.BackupDir} is not set"
      )
    }
    logInfo(
      s"Using backup mode for partition planning (no Milvus client connection): $backupDir"
    )

    val hadoopConf = ctx.hadoopConf(backupDir)
    // Prefer the meta already parsed at table init (threaded directly, so it is
    // neither re-serialized nor shipped to executors); fall back to a fresh
    // read otherwise (e.g. a direct scan without table init).
    val meta = preParsedBackupMeta.getOrElse {
      BackupMetaReader.readMeta(
        StorageOptions.storeFor(
          hadoopConf,
          StorageOptions.snapshotBucket(backupDir).getOrElse(""),
          milvusOption.options
        ),
        backupDir,
        StorageOptions.backupMaxJsonBytes(options)
      ) match {
        case Right(m) => m
        case Left(err) =>
          throw new IllegalArgumentException(
            s"Failed to parse backup meta at ${BackupMetaReader
                .metaPath(backupDir)}: ${err.getMessage}",
            err
          )
      }
    }
    if (meta.isSnapshotFormat) {
      throw new IllegalArgumentException(
        s"Backup '${meta.name}' is in snapshot format; only binlog-format " +
          "backups can be read as a datasource"
      )
    }
    val coll = BackupSelection.resolveBackupCollection(
      meta,
      milvusOption.databaseName,
      milvusOption.collectionName
    ) match {
      case Right(c) => c
      case Left(msg) =>
        throw new IllegalArgumentException(msg)
    }
    if (
      milvusOption.partitionName.nonEmpty || milvusOption.partitionID.nonEmpty ||
      milvusOption.segmentID.nonEmpty
    ) {
      throw new IllegalArgumentException(
        s"Backup datasource does not support " +
          s"'${MilvusOption.MilvusPartitionName}' / " +
          s"'${MilvusOption.MilvusPartitionID}' / " +
          s"'${MilvusOption.MilvusSegmentID}' selectors yet; read the whole " +
          "backup and filter in Spark"
      )
    }
    val schemaBytes = coll.schema
      .map(BackupMetaReader.toProtobufSchemaBytes)
      .getOrElse {
        throw new IllegalArgumentException(
          s"Backup '${meta.name}' meta has no schema for collection " +
            s"'${coll.collectionName}'; cannot plan a read"
        )
      }
    // Driver-side validation only: a backup read requires a primary key for
    // delete handling; the reader factory re-derives it independently.
    CollectionSchema
      .parseFrom(schemaBytes)
      .fields
      .find(_.isPrimaryKey)
      .getOrElse {
        throw new IllegalArgumentException(
          s"Backup '${meta.name}' collection '${coll.collectionName}' schema " +
            "has no primary key; cannot plan a read"
        )
      }
    val applyDeletes = MilvusOption.readApplyDeletes(options)
    // Object paths are reconstructed from `backupDir` plus segment IDs inside
    // BackupMetaReader; the meta's `log_path` values are source keys and are
    // never opened. The native packed reader receives bucket-relative keys, so
    // its `fs.bucket_name` must be canonicalized to the backup URI's bucket.
    val canonicalBucket = StorageOptions.snapshotBucket(backupDir)
    if (canonicalBucket.isEmpty) {
      // The JNI packed reader requires S3: with a local/file:// dir it would
      // fall back to a-bucket/localhost:9000 and fail on every task, so reject
      // here rather than after footer planning and partition dispatch.
      throw new IllegalArgumentException(
        s"${MilvusOption.BackupDir} must be an S3 URI (s3a://...) for reads; " +
          s"got '$backupDir'. Local or file:// backup dirs are only supported " +
          "by the meta/footer mapping layer, not the native packed reader"
      )
    }
    val v2Segments = BackupMetaReader.toV2Segments(
      meta,
      StorageOptions.storeFor(
        hadoopConf,
        StorageOptions.snapshotBucket(backupDir).getOrElse(""),
        milvusOption.options
      ),
      backupDir,
      applyDeletes,
      coll.collectionId
    ) match {
      case Right(segs) => segs
      case Left(err) =>
        throw new IllegalStateException(
          s"Failed to load StorageV2 segments from backup: ${err.getMessage}",
          err
        )
    }
    // L0 delete-only segments alone must not satisfy the guard: they carry no
    // column groups and would be filtered out at partition planning, so a
    // delete-only backup would silently read zero rows.
    if (!v2Segments.exists(_.columnGroups.nonEmpty)) {
      throw new IllegalArgumentException(
        s"Backup '${meta.name}' collection '${coll.collectionName}' has no " +
          "packed-parquet (StorageV2) data segments to read (only delete-only " +
          "segments). This connector requires Milvus 2.6+ with Storage V2; " +
          "ensure the collection has been flushed and contains data."
      )
    }

    val v2DeletePlans = DeletePlanning.loadV2DeletePlans(
      ctx,
      v2Segments,
      schemaBytes,
      snapshotBucket = canonicalBucket,
      hadoopConf,
      errorContext = "backup"
    )

    val inheritedDeleteSegments = v2Segments.filter(seg =>
      seg.columnGroups.isEmpty && seg.deltaLogs.nonEmpty
    )
    // With inlineInheritedDeletePlans=false, buildSnapshotPartitions only
    // consults the KEYS of this map (inheritedDeletePlanPartitionMarker does
    // `.contains`); the full partition-scoped delete plans are loaded once, by
    // createReaderFactory. Passing empty placeholder plans keeps the marker
    // semantics without downloading and PK-decoding the entire L0 delete set
    // twice on the driver.
    val inheritedDeletePlansByPartition =
      if (!applyDeletes || inheritedDeleteSegments.isEmpty) {
        Map.empty[Long, com.zilliz.milvus.storage.delete.MilvusDeletePlan]
      } else {
        inheritedDeleteSegments
          .map(seg =>
            seg.partitionId ->
              com.zilliz.milvus.storage.delete.MilvusDeletePlan.empty
          )
          .toMap
      }

    // The default inlineInheritedDeletePlans = false is what backup needs:
    // partitions carry a partition-scoped marker instead of a copy of the L0
    // delete plan, and the reader factory resolves the shared plan from a
    // single map — otherwise a delete-heavy backup materializes the L0 delete
    // map once per segment (O(S×D)).
    SnapshotPartitions.build(
      ctx,
      manifestList = Seq.empty,
      defaultPartitionId = "0",
      schemaBytes = schemaBytes,
      v2Segments = v2Segments,
      v2DeletePlans = v2DeletePlans,
      inheritedDeletePlansByPartition = inheritedDeletePlansByPartition,
      forceCanonicalBucket = canonicalBucket
    )
  }

  /** Compute the shared partition-scoped L0 delete plans for a backup read from
    * the parsed meta, independent of partition planning. Falls back to a fresh
    * meta read when table init did not parse one (e.g. its read failed while
    * the planner's succeeded), and fails loudly if that re-read also fails —
    * otherwise a partition-scoped marker would silently resolve to an empty
    * plan and deleted rows would come back as live.
    */
  def inheritedDeletePlans()
      : Map[Long, com.zilliz.milvus.storage.delete.MilvusDeletePlan] = {
    val backupDir = MilvusOption.backupDir(options).getOrElse {
      return Map.empty
    }
    val hadoopConf = ctx.hadoopConf(backupDir)
    val meta = preParsedBackupMeta.getOrElse {
      BackupMetaReader.readMeta(
        StorageOptions.storeFor(
          hadoopConf,
          StorageOptions.snapshotBucket(backupDir).getOrElse(""),
          milvusOption.options
        ),
        backupDir,
        StorageOptions.backupMaxJsonBytes(options)
      ) match {
        case Right(m) => m
        case Left(err) =>
          throw new IllegalStateException(
            s"Failed to re-read backup meta at ${BackupMetaReader
                .metaPath(backupDir)} to resolve inherited delete plans: ${err.getMessage}",
            err
          )
      }
    }
    val coll = BackupSelection
      .resolveBackupCollection(
        meta,
        milvusOption.databaseName,
        milvusOption.collectionName
      )
      .getOrElse { return Map.empty }
    val schemaBytes = coll.schema
      .map(BackupMetaReader.toProtobufSchemaBytes)
      .getOrElse { return Map.empty }
    val pkField = CollectionSchema
      .parseFrom(schemaBytes)
      .fields
      .find(_.isPrimaryKey)
      .getOrElse { return Map.empty }
    val deleteOnlySegments =
      BackupMetaReader.deleteOnlySegments(meta, coll.collectionId, backupDir)
    if (deleteOnlySegments.isEmpty) {
      Map.empty
    } else {
      MilvusDeltaLogReader.loadPartitionScopedDeletePlans(
        deleteOnlySegments,
        pkField,
        StorageOptions.snapshotBucket(backupDir).getOrElse(""),
        StorageOptions.storeFor(
          hadoopConf,
          StorageOptions.snapshotBucket(backupDir).getOrElse(""),
          milvusOption.options
        )
      ) match {
        case Right(plans) => plans
        case Left(err) =>
          throw new IllegalStateException(
            s"Failed to load inherited StorageV2 delete logs for reader factory: ${err.getMessage}",
            err
          )
      }
    }
  }
}
