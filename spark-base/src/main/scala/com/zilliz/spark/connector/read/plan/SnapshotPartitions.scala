package com.zilliz.spark.connector.read.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.delete.{MilvusDeletePlan, MilvusDeltaLogReader}
import com.zilliz.milvus.storage.read.plan.{DeleteSource, InputSpec}
import com.zilliz.milvus.storage.snapshot.{SegmentLayout, Snapshot}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.read.{
  MilvusPackedV2InputPartition,
  MilvusStorageV3InputPartition
}

/** The common tail of every planning entry point: a [[Snapshot]] plus its
  * delete plans becomes one `InputPartition` per data segment.
  *
  * This is the part that becomes `core.read.plan`; the planners in this
  * package only differ in where the snapshot comes from.
  */
object SnapshotPartitions extends Logging {

  private[read] def build(
      ctx: ScanContext,
      snapshot: Snapshot,
      v3DeletePlans: Map[Long, MilvusDeletePlan] = Map.empty,
      v3ReadVersions: Map[Long, Long] = Map.empty,
      v2DeletePlans: Map[Long, MilvusDeletePlan] = Map.empty,
      inheritedDeletePlansByPartition: Map[Long, MilvusDeletePlan] = Map.empty,
      inlineInheritedDeletePlans: Boolean = false,
      forceCanonicalBucket: Option[String] = None
  ): Array[InputPartition] = {
    val milvusOption = ctx.milvusOption
    val canonicalMilvusOption = forceCanonicalBucket
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(bucket =>
        milvusOption.copy(
          options = milvusOption.options ++
            Map(StorageProperties.BucketName -> bucket)
        )
      )
      .getOrElse(milvusOption)

    // Parsed once for the whole plan rather than per partition: a bad storage
    // configuration should fail planning, not every task.
    //
    // Lazy because the two layouts use different configurations and a plan
    // rarely contains both. Backup planning reaches here with a bucket derived
    // from `milvus.backup.dir` and no `fs.bucket_name`, produces column-group
    // partitions only, and would otherwise fail validating the manifest-line
    // configuration that no partition ends up using.
    lazy val storageProperties = StorageProperties.from(milvusOption.options)
    lazy val canonicalStorageProperties =
      StorageProperties.from(canonicalMilvusOption.options)
    val applyDeletes = MilvusOption.readApplyDeletes(ctx.options)
    val schemaBytes = snapshot.schemaBytes
    def deleteSourceFor(plan: MilvusDeletePlan): DeleteSource =
      if (!applyDeletes || plan.isEmpty) DeleteSource.None
      else DeleteSource.Materialized(plan)

    val v3Partitions = snapshot.v3Segments.map { seg =>
      val (basePath, parsedReadVersion) = seg.layout match {
        case SegmentLayout.Manifest(path, version) => (path, version)
        case SegmentLayout.ColumnGroups(_) =>
          throw new IllegalStateException(
            s"segment ${seg.id} is storage_version 3 but carries a column-group layout"
          )
      }
      val readVersion = v3ReadVersions.getOrElse(seg.id, parsedReadVersion)
      logInfo(
        s"Creating partition with manifestPath=$basePath, partitionID=${seg.partitionId}, segmentID=${seg.id}, readVersion=$readVersion"
      )
      val inheritedDeletePlan = MilvusDeltaLogReader.effectiveInheritedDeletePlan(
        seg.partitionId,
        inheritedDeletePlansByPartition
      )
      val ownDeletePlan =
        v3DeletePlans.getOrElse(seg.id, MilvusDeletePlan.empty)
      val deletePlan =
        MilvusDeletePlan.union(inheritedDeletePlan, ownDeletePlan)
      MilvusStorageV3InputPartition(
        InputSpec(
          segmentId = seg.id,
          partitionId = seg.partitionId,
          layout = SegmentLayout.Manifest(basePath, readVersion),
          schemaBytes = schemaBytes,
          properties = storageProperties,
          deletes = deleteSourceFor(deletePlan)
        ),
        seg.partitionId.toString,
        milvusOption,
        ctx.vectorSearchConfig.map(_.topK),
        ctx.vectorSearchConfig.map(_.queryVector),
        ctx.vectorSearchConfig.map(_.metricType),
        ctx.vectorSearchConfig.map(_.vectorColumn)
      ): InputPartition
    }

    if (v3Partitions.nonEmpty) {
      logInfo(
        s"Created ${v3Partitions.size} V3 partitions from snapshot manifests"
      )
    }

    val packedV2Partitions = snapshot.v2Segments.filter(_.hasData).map { seg =>
      val groups = seg.layout match {
        case SegmentLayout.ColumnGroups(gs) => gs
        case SegmentLayout.Manifest(_, _) =>
          throw new IllegalStateException(
            s"segment ${seg.id} is storage_version 2 but carries a manifest layout"
          )
      }
      val ownDeletePlan =
        v2DeletePlans.getOrElse(seg.id, MilvusDeletePlan.empty)
      val inheritedDeletePlan =
        if (inlineInheritedDeletePlans)
          MilvusDeltaLogReader.effectiveInheritedDeletePlan(
            seg.partitionId,
            inheritedDeletePlansByPartition
          )
        else MilvusDeletePlan.empty
      val deletePlan =
        MilvusDeletePlan.union(inheritedDeletePlan, ownDeletePlan)
      MilvusPackedV2InputPartition(
        InputSpec(
          segmentId = seg.id,
          partitionId = seg.partitionId,
          layout = SegmentLayout.ColumnGroups(groups),
          schemaBytes = schemaBytes,
          properties = canonicalStorageProperties,
          deletes = deleteSourceFor(deletePlan)
        ),
        canonicalMilvusOption,
        inheritedDeletePlanPartitionId =
          if (inlineInheritedDeletePlans) None
          else
            MilvusDeltaLogReader.inheritedDeletePlanPartitionMarker(
              seg.partitionId,
              inheritedDeletePlansByPartition
            )
      ): InputPartition
    }

    val skippedDeleteOnlySegments = snapshot.v2Segments.count(!_.hasData)
    if (skippedDeleteOnlySegments > 0) {
      logInfo(
        s"Skipped $skippedDeleteOnlySegments StorageV2 delete-only segment(s) during snapshot partition planning"
      )
    }
    if (packedV2Partitions.nonEmpty) {
      logInfo(
        s"Created ${packedV2Partitions.size} packed-V2 partition(s) from snapshot metadata"
      )
    }

    (v3Partitions ++ packedV2Partitions).toArray
  }
}
