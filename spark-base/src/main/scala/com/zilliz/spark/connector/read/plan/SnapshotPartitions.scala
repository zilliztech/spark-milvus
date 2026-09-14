package com.zilliz.spark.connector.read.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.delete.MilvusDeltaLogReader
import com.zilliz.milvus.storage.read.plan.{
  DeleteSource,
  InputSpec,
  SegmentLayout
}
import com.zilliz.milvus.storage.snapshot.{
  MilvusSnapshotReader,
  StorageV2ManifestItem,
  V2SegmentInfo
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.read.{MilvusPackedV2InputPartition, MilvusStorageV3InputPartition}

/** The common tail of every planning entry point: a segment list plus its
  * delete plans becomes one `InputPartition` per segment.
  *
  * This is the part that becomes `core.read.plan`; the four planners in this
  * package only differ in where the segment list comes from.
  */
object SnapshotPartitions extends Logging {
  private[read] def segmentIdForManifestItem(
      item: StorageV2ManifestItem,
      basePath: String
  ): Long = {
    if (item.segmentID != 0L) {
      item.segmentID
    } else {
      val pathParts = basePath.split("/").filter(_.nonEmpty)
      if (pathParts.nonEmpty) {
        try pathParts.last.toLong
        catch { case _: NumberFormatException => 0L }
      } else 0L
    }
  }

  private[read] def build(
      ctx: ScanContext,
      manifestList: Seq[StorageV2ManifestItem],
      defaultPartitionId: String,
      schemaBytes: Array[Byte],
      v3DeletePlans: Map[
        Long,
        com.zilliz.milvus.storage.delete.MilvusDeletePlan
      ] = Map.empty,
      v3ReadVersions: Map[Long, Long] = Map.empty,
      v2Segments: Seq[V2SegmentInfo],
      v2DeletePlans: Map[
        Long,
        com.zilliz.milvus.storage.delete.MilvusDeletePlan
      ],
      inheritedDeletePlansByPartition: Map[
        Long,
        com.zilliz.milvus.storage.delete.MilvusDeletePlan
      ] = Map.empty,
      inlineInheritedDeletePlans: Boolean = false,
      forceCanonicalBucket: Option[String] = None
  ): Array[InputPartition] = {
    val canonicalMilvusOption = forceCanonicalBucket
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(bucket =>
        ctx.milvusOption.copy(
          options = ctx.milvusOption.options ++
            Map(StorageProperties.BucketName -> bucket)
        )
      )
      .getOrElse(ctx.milvusOption)

    // Parsed once for the whole plan rather than per partition: a bad storage
    // configuration should fail planning, not every task.
    //
    // Lazy because the two layouts use different configurations and a plan
    // rarely contains both. Backup planning reaches here with a bucket derived
    // from `milvus.backup.dir` and no `fs.bucket_name`, produces column-group
    // partitions only, and would otherwise fail validating the manifest-line
    // configuration that no partition ends up using.
    lazy val storageProperties =
      StorageProperties.from(ctx.milvusOption.options)
    lazy val canonicalStorageProperties =
      StorageProperties.from(canonicalMilvusOption.options)
    val applyDeletes = MilvusOption.readApplyDeletes(ctx.options)
    def deleteSourceFor(
        plan: com.zilliz.milvus.storage.delete.MilvusDeletePlan
    ): DeleteSource =
      if (!applyDeletes || plan.isEmpty) DeleteSource.None
      else DeleteSource.Materialized(plan)

    val v3Partitions = manifestList.map { item =>
      val (basePath, parsedReadVersion) =
        MilvusSnapshotReader.parseManifestContent(item.manifest) match {
          case Right(content) => (content.basePath, content.ver.toLong)
          case Left(_)        => (item.manifest, -1L)
        }

      val pathParts = basePath.split("/").filter(_.nonEmpty)
      val segmentID = segmentIdForManifestItem(item, basePath)
      val readVersion = v3ReadVersions.getOrElse(segmentID, parsedReadVersion)
      val insertLogIdx = pathParts.lastIndexOf("insert_log")
      val partitionId =
        if (insertLogIdx >= 0 && pathParts.length > insertLogIdx + 3) {
          pathParts(insertLogIdx + 2)
        } else {
          logWarning(
            s"Manifest path '$basePath' does not match expected insert_log/{collectionID}/{partitionID}/{segmentID} layout; using fallback partitionID=$defaultPartitionId"
          )
          defaultPartitionId
        }
      logInfo(
        s"Creating partition with manifestPath=$basePath, partitionID=$partitionId, segmentID=$segmentID, readVersion=$readVersion"
      )
      val partitionIdLong =
        try partitionId.toLong
        catch { case _: NumberFormatException => Long.MinValue }
      val inheritedDeletePlan =
        if (partitionIdLong == Long.MinValue) {
          com.zilliz.milvus.storage.delete.MilvusDeletePlan.empty
        } else {
          MilvusDeltaLogReader.effectiveInheritedDeletePlan(
            partitionIdLong,
            inheritedDeletePlansByPartition
          )
        }
      val ownDeletePlan = v3DeletePlans.getOrElse(
        segmentID,
        com.zilliz.milvus.storage.delete.MilvusDeletePlan.empty
      )
      val deletePlan = com.zilliz.milvus.storage.delete.MilvusDeletePlan.union(
        inheritedDeletePlan,
        ownDeletePlan
      )
      MilvusStorageV3InputPartition(
        InputSpec(
          segmentId = segmentID,
          partitionId = partitionIdLong,
          layout = SegmentLayout.Manifest(basePath, readVersion),
          schemaBytes = schemaBytes,
          properties = storageProperties,
          deletes = deleteSourceFor(deletePlan)
        ),
        partitionId,
        ctx.milvusOption,
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

    // Dedup column groups by slot so a field carried by both an old multi-field
    // group and a newer single-field group (add-field + backfill) is read from
    // the newest owner, not whichever the native reader picks. This changes
    // which parquet a field is read from on every read path, so log loudly
    // whenever it actually re-attributes a field — a silent mis-attribute would
    // otherwise surface as stale/null values with no explanation.
    val packedV2Segments = v2Segments.map { seg =>
      val deduped = seg.dedupColumnGroupsBySlot
      if (deduped.columnGroups != seg.columnGroups) {
        logWarning(
          s"V2 slot dedup re-attributed fields for segment ${seg.segmentId}: " +
            "overlapping fields are now read from their max-slot group. This " +
            "assumes slot ids grow with write time; if they do not, the read " +
            "may return an older group's values."
        )
      }
      deduped
    }
    val packedV2Partitions = packedV2Segments
      .filter(_.columnGroups.nonEmpty)
      .map { seg =>
        val ownDeletePlan = v2DeletePlans.getOrElse(
          seg.segmentId,
          com.zilliz.milvus.storage.delete.MilvusDeletePlan.empty
        )
        val inheritedDeletePlan =
          if (inlineInheritedDeletePlans) {
            MilvusDeltaLogReader.effectiveInheritedDeletePlan(
              seg.partitionId,
              inheritedDeletePlansByPartition
            )
          } else {
            com.zilliz.milvus.storage.delete.MilvusDeletePlan.empty
          }
        val deletePlan =
          com.zilliz.milvus.storage.delete.MilvusDeletePlan.union(
            inheritedDeletePlan,
            ownDeletePlan
          )
        MilvusPackedV2InputPartition(
          InputSpec(
            segmentId = seg.segmentId,
            partitionId = seg.partitionId,
            layout = SegmentLayout.ColumnGroups(seg.columnGroups),
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

    val skippedDeleteOnlySegments =
      v2Segments.count(_.columnGroups.isEmpty)
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
