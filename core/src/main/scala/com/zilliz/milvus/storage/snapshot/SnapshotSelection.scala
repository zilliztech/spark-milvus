package com.zilliz.milvus.storage.snapshot

/** Partition selection using only partition identities and whether a segment
  * carries data; no layout is interpreted.
  */
object SnapshotSelection {
  final case class Segment(partitionId: Long, hasData: Boolean)

  /** Positions refer to the input sequence, so callers retain its order even
    * when descriptors repeat within a partition.
    */
  final case class Selection(
      segmentPositions: Set[Int],
      partitionIds: Seq[Long]
  )

  def select(
      name: String,
      availablePartitionIds: Seq[Long],
      segments: Seq[Segment],
      selectedPartitionIds: Seq[Long]
  ): Selection = {
    val partitionSelection = selectedPartitionIds.distinct
    if (partitionSelection.isEmpty)
      return Selection(segments.indices.toSet, availablePartitionIds)

    val knownPartitions =
      (availablePartitionIds ++ segments
        .filter(_.hasData)
        .map(_.partitionId)).distinct
    val missingPartitions =
      partitionSelection.filterNot(knownPartitions.contains)
    if (missingPartitions.nonEmpty) {
      throw new IllegalArgumentException(
        s"Partition id(s) ${missingPartitions.mkString(", ")} not found in snapshot $name"
      )
    }

    val partitionSet = partitionSelection.toSet
    val selected = segments.zipWithIndex.filter { case (segment, _) =>
      partitionSet.contains(segment.partitionId) ||
      (!segment.hasData && segment.partitionId == -1L)
    }

    val retainedPartitionSet =
      selected.iterator.map(_._1).filter(_.hasData).map(_.partitionId).toSet
    val orderedPartitionIds =
      (availablePartitionIds ++ selected.map(_._1.partitionId)).distinct.filter(
        retainedPartitionSet.contains
      )
    Selection(selected.iterator.map(_._2).toSet, orderedPartitionIds)
  }
}
