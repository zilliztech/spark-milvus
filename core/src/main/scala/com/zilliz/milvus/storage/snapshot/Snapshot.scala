package com.zilliz.milvus.storage.snapshot

import io.milvus.grpc.schema.{CollectionSchema => ProtoSchema, FieldSchema}

/** One fixed view of a collection: what a read plans against.
  *
  * Every read entry point produces one of these and nothing downstream looks at
  * the entry point's own format again. It stays on the driver; the
  * per-partition projection that reaches executors is
  * `core.read.plan.SegmentReadTask`.
  *
  * Fields a source cannot supply are `Option` or carry an explicit state, so a
  * missing value is visible to the planner instead of defaulting to "none".
  * `docs/design/architecture/snapshot.html` is the design.
  *
  * @param bucket
  *   the bucket every path in `segments` is a key of; the native reader is
  *   rooted at it. Empty when the segments live on a local filesystem.
  */
final case class Snapshot(
    name: String,
    collectionId: Long,
    createdAt: Option[Long],
    schema: ProtoSchema,
    partitionIds: Seq[Long],
    segments: Seq[Segment],
    origin: SnapshotOrigin,
    bucket: String
) {

  /** The schema as the bytes every `SegmentReadTask` carries. */
  def schemaBytes: Array[Byte] = schema.toByteArray

  def primaryKeyField: Option[FieldSchema] = schema.fields.find(_.isPrimaryKey)

  /** Segments that hold rows and become partitions. */
  def dataSegments: Seq[Segment] = segments.filter(_.hasData)

  /** Segments that carry only deletes (Milvus L0 segments): their delete files
    * apply to every data segment of the same partition.
    */
  def deleteOnlySegments: Seq[Segment] =
    segments.filter(s => !s.hasData && s.deletes != DeleteFiles.Empty)

  def v3Segments: Seq[Segment] = segments.filter(_.storageVersion == 3)
  def v2Segments: Seq[Segment] = segments.filter(_.storageVersion == 2)

  /** The selected partitions and/or data segments (capability R16).
    *
    * Selection preserves snapshot order. Every requested id must exist after
    * the other selector is applied; an accidental empty read is therefore a
    * planning error. Selecting data segments also retains the delete-only
    * segments of their partitions, because those deletes still apply.
    */
  def narrow(
      selectedPartitionIds: Seq[Long],
      selectedSegmentIds: Seq[Long]
  ): Snapshot = {
    val partitionSelection = selectedPartitionIds.distinct
    val segmentSelection = selectedSegmentIds.distinct
    if (partitionSelection.isEmpty && segmentSelection.isEmpty) return this

    val availablePartitionIds =
      (partitionIds ++ segments.filter(_.hasData).map(_.partitionId)).distinct
    val missingPartitions =
      partitionSelection.filterNot(availablePartitionIds.contains)
    if (missingPartitions.nonEmpty) {
      throw new IllegalArgumentException(
        s"Partition id(s) ${missingPartitions.mkString(", ")} not found in snapshot $name"
      )
    }

    val partitionSet = partitionSelection.toSet
    val withinPartitions =
      if (partitionSet.isEmpty) segments
      else
        segments.filter(segment =>
          partitionSet.contains(segment.partitionId) ||
            (!segment.hasData && segment.partitionId == -1L)
        )

    val selected =
      if (segmentSelection.isEmpty) withinPartitions
      else {
        val segmentSet = segmentSelection.toSet
        val data = withinPartitions.filter(segment =>
          segment.hasData && segmentSet.contains(segment.id)
        )
        val found = data.iterator.map(_.id).toSet
        val missing = segmentSelection.filterNot(found.contains)
        if (missing.nonEmpty) {
          val partitionContext =
            if (partitionSelection.isEmpty) ""
            else s" in partition id(s) ${partitionSelection.mkString(", ")}"
          throw new IllegalArgumentException(
            s"Segment id(s) ${missing.mkString(", ")} not found$partitionContext in snapshot $name"
          )
        }
        val dataPartitions = data.iterator.map(_.partitionId).toSet
        withinPartitions.filter(segment =>
          (segment.hasData && segmentSet.contains(segment.id)) ||
            (!segment.hasData &&
              (segment.partitionId == -1L || dataPartitions.contains(
                segment.partitionId
              )))
        )
      }

    val retainedPartitionSet =
      selected.iterator.filter(_.hasData).map(_.partitionId).toSet
    val orderedPartitionIds =
      (partitionIds ++ selected.map(_.partitionId)).distinct.filter(
        retainedPartitionSet.contains
      )
    copy(partitionIds = orderedPartitionIds, segments = selected)
  }

  /** Single-id compatibility entry point. */
  def narrow(partitionId: Option[Long], segmentId: Option[Long]): Snapshot =
    narrow(partitionId.toSeq, segmentId.toSeq)
}

/** Which entry point produced a `Snapshot`; for errors and logs. */
sealed trait SnapshotOrigin

object SnapshotOrigin {

  /** Read from a snapshot JSON at `location` in the snapshot directory. */
  final case class Catalog(location: String) extends SnapshotOrigin

  /** Translated from a milvus-backup export at `dir`. */
  final case class Backup(dir: String) extends SnapshotOrigin

  /** Deserialized from the 1.x option strings. Goes with capability K2. */
  case object Options extends SnapshotOrigin
}
