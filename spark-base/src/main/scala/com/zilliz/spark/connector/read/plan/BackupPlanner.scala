package com.zilliz.spark.connector.read.plan

import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.storage.compat.backup.BackupMetaReader
import com.zilliz.milvus.storage.delete.{DeletePlan, DeltaLogReader}
import com.zilliz.milvus.storage.snapshot.{
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin
}
import com.zilliz.spark.connector.options.{
  BackupSelection,
  MilvusOption,
  StorageOptions
}

/** Backup mode: a milvus-backup binlog-format export is read offline. Its
  * `full_meta.json` is translated into the same `Segment` the snapshot path
  * uses and becomes a [[Snapshot]], so delete planning and the packed-V2 reader
  * are shared.
  */
private[read] final class BackupPlanner(
    ctx: ScanContext,
    preParsedBackupMeta: Option[BackupMetaReader.BackupInfo]
) extends PartitionPlanner(ctx) {

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

    val snapshot = snapshotFor(backupDir, hadoopConf, canonicalBucket)
    val v2DeletePlans = DeletePlanning.loadV2DeletePlans(
      ctx,
      snapshot,
      canonicalBucket,
      hadoopConf,
      errorContext = "backup"
    )
    // With inlineInheritedDeletePlans = false, buildSnapshotPartitions only
    // consults the KEYS of this map (inheritedDeletePlanPartitionMarker does
    // `.contains`); the full partition-scoped delete plans are loaded once, by
    // createReaderFactory. Passing empty placeholder plans keeps the marker
    // semantics without downloading and PK-decoding the entire L0 delete set
    // twice on the driver.
    val inheritedDeletePlansByPartition =
      if (!MilvusOption.readApplyDeletes(options)) Map.empty[Long, DeletePlan]
      else
        snapshot.deleteOnlySegments
          .map(seg => seg.partitionId -> DeletePlan.empty)
          .toMap
    SnapshotPartitions.build(
      ctx,
      snapshot,
      v2DeletePlans = v2DeletePlans,
      inheritedDeletePlansByPartition = inheritedDeletePlansByPartition,
      forceCanonicalBucket = canonicalBucket
    )
  }

  /** The export as a [[Snapshot]]: the selected collection's V2 segments,
    * schema and partitions. Fails loudly on a snapshot-format backup, a missing
    * schema, a missing primary key or an export with no data segment.
    */
  private def snapshotFor(
      backupDir: String,
      hadoopConf: org.apache.hadoop.conf.Configuration,
      canonicalBucket: Option[String]
  ): Snapshot = {
    val meta = readMeta(backupDir, hadoopConf, "Failed to parse backup meta")
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
      case Right(c)  => c
      case Left(msg) => throw new IllegalArgumentException(msg)
    }
    val schemaBytes = coll.schema
      .map(BackupMetaReader.toProtobufSchemaBytes)
      .getOrElse {
        throw new IllegalArgumentException(
          s"Backup '${meta.name}' meta has no schema for collection " +
            s"'${coll.collectionName}'; cannot plan a read"
        )
      }
    val v2Segments = BackupMetaReader.toV2Segments(
      meta,
      StorageOptions.storeFor(
        hadoopConf,
        canonicalBucket.getOrElse(""),
        milvusOption.options
      ),
      backupDir,
      MilvusOption.readApplyDeletes(options),
      coll.collectionId
    ) match {
      case Right(segs) => segs
      case Left(err) =>
        throw new IllegalStateException(
          s"Failed to load StorageV2 segments from backup: ${err.getMessage}",
          err
        )
    }
    val snapshot = SnapshotCatalog.fromLists(
      name = meta.name,
      collectionId = coll.collectionId,
      createdAt = None,
      partitionIds = v2Segments.map(_.partitionId).distinct,
      schemaBytes = schemaBytes,
      v3Items = Seq.empty,
      v2Segments = v2Segments,
      bucket = canonicalBucket.getOrElse(""),
      origin = SnapshotOrigin.Backup(backupDir)
    ) match {
      case Right(s) => s
      case Left(e) =>
        throw new IllegalArgumentException(
          s"Backup '${meta.name}' is not readable: ${e.getMessage}",
          e
        )
    }
    // Driver-side validation only: a backup read requires a primary key for
    // delete handling; the reader factory re-derives it independently.
    if (snapshot.primaryKeyField.isEmpty) {
      throw new IllegalArgumentException(
        s"Backup '${meta.name}' collection '${coll.collectionName}' schema " +
          "has no primary key; cannot plan a read"
      )
    }
    // L0 delete-only segments alone must not satisfy the guard: they carry no
    // column groups and would be filtered out at partition planning, so a
    // delete-only backup would silently read zero rows.
    if (snapshot.dataSegments.isEmpty) {
      throw new IllegalArgumentException(
        s"Backup '${meta.name}' collection '${coll.collectionName}' has no " +
          "packed-parquet (StorageV2) data segments to read (only delete-only " +
          "segments). This connector requires Milvus 2.6+ with Storage V2; " +
          "ensure the collection has been flushed and contains data."
      )
    }
    snapshot
  }

  /** `wrap` decides the exception class: a bad `milvus.backup.dir` is an
    * argument error at planning, a re-read failing under the reader factory is
    * a state error.
    */
  private def readMeta(
      backupDir: String,
      hadoopConf: org.apache.hadoop.conf.Configuration,
      failure: String,
      wrap: (String, Throwable) => RuntimeException =
        new IllegalArgumentException(_, _)
  ): BackupMetaReader.BackupInfo =
    preParsedBackupMeta.getOrElse {
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
          throw wrap(
            s"$failure at ${BackupMetaReader.metaPath(backupDir)}: ${err.getMessage}",
            err
          )
      }
    }

  /** The shared partition-scoped L0 delete plans for a backup read, from the
    * parsed meta, independent of partition planning. Falls back to a fresh meta
    * read when table init did not parse one, and fails loudly if that re-read
    * also fails: otherwise a partition-scoped marker would silently resolve to
    * an empty plan and deleted rows would come back as live.
    */
  def inheritedDeletePlans(): Map[Long, DeletePlan] = {
    val backupDir = MilvusOption.backupDir(options).getOrElse {
      return Map.empty
    }
    val hadoopConf = ctx.hadoopConf(backupDir)
    val meta = readMeta(
      backupDir,
      hadoopConf,
      "Failed to re-read backup meta to resolve inherited delete plans",
      wrap = new IllegalStateException(_, _)
    )
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
    val pkField = io.milvus.grpc.schema.CollectionSchema
      .parseFrom(schemaBytes)
      .fields
      .find(_.isPrimaryKey)
      .getOrElse { return Map.empty }
    val deleteOnlySegments =
      BackupMetaReader.deleteOnlySegments(meta, coll.collectionId, backupDir)
    if (deleteOnlySegments.isEmpty) Map.empty
    else {
      val bucket = StorageOptions.snapshotBucket(backupDir).getOrElse("")
      DeltaLogReader.loadPartitionScopedDeletePlans(
        deleteOnlySegments,
        pkField,
        bucket,
        StorageOptions.storeFor(hadoopConf, bucket, milvusOption.options)
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
