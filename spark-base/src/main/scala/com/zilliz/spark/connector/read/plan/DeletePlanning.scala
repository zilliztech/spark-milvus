package com.zilliz.spark.connector.read.plan

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import com.zilliz.milvus.storage.delete.{MilvusDeletePlan, MilvusDeltaLogReader}
import com.zilliz.milvus.storage.manifest.MilvusStorageV3ManifestReader
import com.zilliz.milvus.storage.snapshot.{
  DeleteFiles,
  Segment,
  SegmentLayout,
  Snapshot,
  V2SegmentInfo
}
import com.zilliz.spark.connector.options.{MilvusOption, StorageOptions}

/** Delete plans for the segments of a [[Snapshot]], resolved on the driver.
  *
  * V2 packed segments list their delta logs in the snapshot
  * (`DeleteFiles.Listed`); V3 segments carry them in the manifest at the read
  * version, which is resolved here too (`DeleteFiles.InManifest`). Delete-only
  * segments become partition-scoped plans that every data segment of the same
  * partition inherits.
  */
object DeletePlanning extends Logging {

  final case class V3DeletePlanning(
      deletePlans: Map[Long, MilvusDeletePlan],
      readVersions: Map[Long, Long]
  )

  object V3DeletePlanning {
    val empty: V3DeletePlanning = V3DeletePlanning(Map.empty, Map.empty)
  }

  private def store(ctx: ScanContext, conf: Configuration, bucket: Option[String]) =
    StorageOptions.storeFor(conf, bucket.getOrElse(""), ctx.milvusOption.options)

  private def primaryKey(snapshot: Snapshot) =
    snapshot.primaryKeyField.getOrElse(
      throw new IllegalArgumentException("No primary key field found in schema")
    )

  /** Own delete plans of the V2 data segments, keyed by segment id. */
  private[read] def loadV2DeletePlans(
      ctx: ScanContext,
      snapshot: Snapshot,
      snapshotBucket: Option[String],
      hadoopConf: Configuration,
      errorContext: String
  ): Map[Long, MilvusDeletePlan] = {
    val applyDeletes = MilvusOption.readApplyDeletes(ctx.options)
    val dataSegments = snapshot.v2Segments.filter(s =>
      s.hasData && s.deletes.isInstanceOf[DeleteFiles.Listed]
    )
    if (!applyDeletes || dataSegments.isEmpty) Map.empty
    else {
      val pkField = primaryKey(snapshot)
      dataSegments.map { seg =>
        val files = seg.deletes.asInstanceOf[DeleteFiles.Listed].files
        MilvusDeltaLogReader.loadDeletePlan(
          files,
          pkField,
          snapshotBucket.getOrElse(""),
          store(ctx, hadoopConf, snapshotBucket)
        ) match {
          case Right(plan) => seg.id -> plan
          case Left(err) =>
            throw new IllegalStateException(
              s"Failed to load StorageV2 delete logs from $errorContext for segment ${seg.id}: ${err.getMessage}",
              err
            )
        }
      }.toMap
    }
  }

  /** Read versions and delete plans of the V3 segments. A segment whose
    * manifest version is not pinned gets the latest one resolved here, so every
    * task reads the same version.
    */
  private[read] def loadV3DeletePlanning(
      ctx: ScanContext,
      snapshot: Snapshot,
      snapshotBucket: Option[String],
      hadoopConf: Configuration,
      errorContext: String
  ): V3DeletePlanning = {
    val applyDeletes = MilvusOption.readApplyDeletes(ctx.options)
    val v3 = snapshot.v3Segments
    if (!applyDeletes || v3.isEmpty) return V3DeletePlanning.empty
    val pkField = snapshot.primaryKeyField
    if (pkField.isEmpty) return V3DeletePlanning.empty

    val entries = v3.flatMap { seg =>
      val (basePath, requestedReadVersion) = seg.layout match {
        case SegmentLayout.Manifest(path, version) => (path, version)
        case SegmentLayout.ColumnGroups(_) =>
          throw new IllegalStateException(
            s"segment ${seg.id} is storage_version 3 but carries a column-group layout"
          )
      }
      if (seg.id == 0L) None
      else {
        val readVersion: Long =
          if (requestedReadVersion > 0L) requestedReadVersion
          else
            MilvusStorageV3ManifestReader.latestManifestVersion(
              basePath,
              snapshotBucket.getOrElse(""),
              store(ctx, hadoopConf, snapshotBucket)
            ) match {
              case Right(version) => version
              case Left(err) =>
                throw new IllegalStateException(
                  s"Failed to resolve latest StorageV3 manifest version from $errorContext for segment ${seg.id}: ${err.getMessage}",
                  err
                )
            }
        if (readVersion <= 0L) None
        else {
          val deltaLogs = MilvusStorageV3ManifestReader.loadDeltaLogs(
            basePath,
            readVersion,
            snapshotBucket.getOrElse(""),
            store(ctx, hadoopConf, snapshotBucket)
          ) match {
            case Right(logs) => logs
            case Left(err) =>
              throw new IllegalStateException(
                s"Failed to load StorageV3 manifest delete logs from $errorContext for segment ${seg.id}: ${err.getMessage}",
                err
              )
          }
          val deletePlan: Option[MilvusDeletePlan] =
            if (deltaLogs.isEmpty) None
            else
              MilvusDeltaLogReader.loadDeletePlan(
                deltaLogs,
                pkField.get,
                snapshotBucket.getOrElse(""),
                store(ctx, hadoopConf, snapshotBucket)
              ) match {
                case Right(plan) => Some(plan)
                case Left(err) =>
                  throw new IllegalStateException(
                    s"Failed to decode StorageV3 manifest delete logs from $errorContext for segment ${seg.id}: ${err.getMessage}",
                    err
                  )
              }
          Some((seg.id, readVersion, deletePlan))
        }
      }
    }
    V3DeletePlanning(
      deletePlans = entries.flatMap { case (segmentId, _, deletePlan) =>
        deletePlan.map(segmentId -> _)
      }.toMap,
      readVersions = entries.map { case (segmentId, readVersion, _) =>
        segmentId -> readVersion
      }.toMap
    )
  }

  /** Partition-scoped plans from the delete-only (L0) segments, keyed by
    * partition id. Empty when deletes are off or there are none.
    */
  private[read] def loadInheritedDeletePlans(
      ctx: ScanContext,
      snapshot: Snapshot,
      snapshotBucket: Option[String],
      hadoopConf: Configuration,
      errorContext: String
  ): Map[Long, MilvusDeletePlan] = {
    val applyDeletes = MilvusOption.readApplyDeletes(ctx.options)
    val deleteOnly = snapshot.deleteOnlySegments
    if (!applyDeletes || deleteOnly.isEmpty) Map.empty
    else {
      MilvusDeltaLogReader.loadPartitionScopedDeletePlans(
        deleteOnly.map(asV2Info),
        primaryKey(snapshot),
        snapshotBucket.getOrElse(""),
        store(ctx, hadoopConf, snapshotBucket)
      ) match {
        case Right(plans) => plans
        case Left(err) =>
          throw new IllegalStateException(
            s"Failed to load inherited StorageV2 delete logs from $errorContext: ${err.getMessage}",
            err
          )
      }
    }
  }

  /** The delta-log reader still takes the V2 record type; a delete-only
    * segment maps onto it without loss.
    */
  private def asV2Info(seg: Segment): V2SegmentInfo =
    V2SegmentInfo(
      segmentId = seg.id,
      partitionId = seg.partitionId,
      numOfRows = seg.rows.getOrElse(0L),
      storageVersion = 2L,
      columnGroups = Seq.empty,
      deltaLogs = seg.deletes match {
        case DeleteFiles.Listed(files) => files
        case _                         => Seq.empty
      }
    )
}
