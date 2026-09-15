package com.zilliz.spark.connector.read.plan

import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.{Snapshot, SnapshotCatalog}
import com.zilliz.spark.connector.options.{
  MilvusOption,
  StorageOptions,
  V2SegmentResolvers
}

/** Client mode: the collection is named, the service answers which
  * collection id that is, and the snapshot comes from the snapshot directory
  * on object storage through [[SnapshotCatalog]]. Nothing is created on the
  * service: a read needs a snapshot that already exists, made by Milvus or by
  * `CALL create_snapshot` (capability A1).
  *
  * `milvus.client.snapshot.name` picks a snapshot by name; without it the
  * latest one is read. `milvus.partition.name`, `milvus.partition.id` and
  * `milvus.segment.id` narrow the segment list (R16).
  */
private[read] final class ClientSnapshotPlanner(ctx: ScanContext)
    extends PartitionPlanner(ctx) {

  def plan(client: MilvusClient): Array[InputPartition] = {
    val snapshot = select(client)
    val bucket = Some(StorageOptions.resolveConnectorS3Bucket(milvusOption.options))
    val hadoopConf = ctx.hadoopConf("")
    val v3 = DeletePlanning.loadV3DeletePlanning(
      ctx,
      snapshot,
      bucket,
      hadoopConf,
      errorContext = s"snapshot ${snapshot.name}"
    )
    val v2 = DeletePlanning.loadV2DeletePlans(
      ctx,
      snapshot,
      bucket,
      hadoopConf,
      errorContext = s"snapshot ${snapshot.name}"
    )
    val inherited = DeletePlanning.loadInheritedDeletePlans(
      ctx,
      snapshot,
      bucket,
      hadoopConf,
      errorContext = s"snapshot ${snapshot.name}"
    )
    SnapshotPartitions.build(
      ctx,
      snapshot,
      v3DeletePlans = v3.deletePlans,
      v3ReadVersions = v3.readVersions,
      v2DeletePlans = v2,
      inheritedDeletePlansByPartition = inherited,
      inlineInheritedDeletePlans = true
    )
  }

  /** The snapshot this read is about, narrowed to the selected partitions and
    * segments.
    */
  private[read] def select(client: MilvusClient): Snapshot = {
    val collectionInfo = client
      .getCollectionInfo(milvusOption.databaseName, milvusOption.collectionName)
      .getOrElse(
        throw new IllegalArgumentException(
          s"Collection ${milvusOption.collectionName} not found"
        )
      )
    val catalog = ClientSnapshotPlanner.catalog(ctx)
    val rootPath =
      milvusOption.options.getOrElse(StorageProperties.RootPath, "files")
    val snapshot = Option(options.get(MilvusOption.ClientSnapshotName))
      .map(_.trim)
      .filter(_.nonEmpty) match {
      case Some(name) => catalog.byName(rootPath, collectionInfo.collectionID, name)
      case None       => catalog.latest(rootPath, collectionInfo.collectionID)
    }
    logInfo(
      s"Reading snapshot ${snapshot.name} of collection ${milvusOption.collectionName} " +
        s"(${snapshot.segments.size} segments) from ${snapshot.origin}"
    )
    SegmentSelection.narrow(snapshot, milvusOption, partitionIdByName = name =>
      client
        .getPartitionID(milvusOption.databaseName, milvusOption.collectionName, name)
        .getOrElse(
          throw new IllegalArgumentException(
            s"Partition '$name' not found in collection ${milvusOption.collectionName}"
          )
        )
    )
  }
}

object ClientSnapshotPlanner {

  /** A catalog bound to the connector's bucket, reading through the driver's
    * object store, materializing V2 segments through compat.
    */
  private[read] def catalog(ctx: ScanContext): SnapshotCatalog = {
    val bucket = StorageOptions.resolveConnectorS3Bucket(ctx.milvusOption.options)
    val store = StorageOptions.storeFor(
      ctx.hadoopConf(""),
      bucket,
      ctx.milvusOption.options
    )
    new SnapshotCatalog(
      store,
      bucket,
      V2SegmentResolvers.footer(MilvusOption.readApplyDeletes(ctx.options)),
      StorageOptions.backupMaxJsonBytes(ctx.options)
    )
  }
}

/** R16: the partition and segment selectors applied to a snapshot. A
  * selector that matches nothing is an error, not an empty read.
  */
private[read] object SegmentSelection {

  def narrow(
      snapshot: Snapshot,
      milvusOption: MilvusOption,
      partitionIdByName: String => Long
  ): Snapshot = {
    val partitionFilter: Option[Long] =
      if (milvusOption.partitionID.trim.nonEmpty) {
        Some(parseId(MilvusOption.MilvusPartitionID, milvusOption.partitionID))
      } else if (milvusOption.partitionName.trim.nonEmpty) {
        Some(partitionIdByName(milvusOption.partitionName.trim))
      } else None
    val segmentFilter: Option[Long] =
      if (milvusOption.segmentID.trim.nonEmpty)
        Some(parseId(MilvusOption.MilvusSegmentID, milvusOption.segmentID))
      else None
    if (partitionFilter.isEmpty && segmentFilter.isEmpty) return snapshot

    val byPartition = partitionFilter match {
      case Some(p) => snapshot.segments.filter(_.partitionId == p)
      case None    => snapshot.segments
    }
    // A segment selector keeps the partition's delete-only segments: their
    // deletes apply to the selected segment too.
    val selected = segmentFilter match {
      case Some(s) =>
        val data = byPartition.filter(seg => seg.hasData && seg.id == s)
        if (data.isEmpty) {
          throw new IllegalArgumentException(
            s"Segment $s not found in snapshot ${snapshot.name}" +
              partitionFilter.map(p => s" partition $p").getOrElse("")
          )
        }
        val partitions = data.map(_.partitionId).toSet
        data ++ byPartition.filter(seg =>
          !seg.hasData && partitions.contains(seg.partitionId)
        )
      case None =>
        if (byPartition.isEmpty) {
          throw new IllegalArgumentException(
            s"Partition ${partitionFilter.get} has no segments in snapshot ${snapshot.name}"
          )
        }
        byPartition
    }
    snapshot.copy(
      partitionIds = partitionFilter.map(Seq(_)).getOrElse(snapshot.partitionIds),
      segments = selected
    )
  }

  private def parseId(key: String, raw: String): Long =
    try raw.trim.toLong
    catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"Option '$key' must be a numeric id, got '$raw'"
        )
    }
}
