package com.zilliz.milvus.storage.snapshot

/** Partition and segment selection shared by input formats. Only identities and
  * whether a segment carries data are needed; no layout is interpreted.
  */
object SnapshotSelection {
  final case class Segment(id: Long, partitionId: Long, hasData: Boolean)

  /** Positions refer to the input sequence, so callers retain its order even
    * when descriptors happen to repeat an id in different partitions.
    */
  final case class Selection(
      segmentPositions: Set[Int],
      partitionIds: Seq[Long]
  )

  def select(
      name: String,
      availablePartitionIds: Seq[Long],
      segments: Seq[Segment],
      selectedPartitionIds: Seq[Long],
      selectedSegmentIds: Seq[Long]
  ): Selection = {
    val partitionSelection = selectedPartitionIds.distinct
    val segmentSelection = selectedSegmentIds.distinct
    if (partitionSelection.isEmpty && segmentSelection.isEmpty)
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
    val withinPartitions = segments.zipWithIndex.filter { case (segment, _) =>
      partitionSet.isEmpty || partitionSet.contains(segment.partitionId) ||
      (!segment.hasData && segment.partitionId == -1L)
    }
    val selected =
      if (segmentSelection.isEmpty) withinPartitions
      else {
        val segmentSet = segmentSelection.toSet
        val data = withinPartitions.filter { case (segment, _) =>
          segment.hasData && segmentSet.contains(segment.id)
        }
        val found = data.iterator.map(_._1.id).toSet
        val missing = segmentSelection.filterNot(found.contains)
        if (missing.nonEmpty) {
          val partitionContext =
            if (partitionSelection.isEmpty) ""
            else s" in partition id(s) ${partitionSelection.mkString(", ")}"
          throw new IllegalArgumentException(
            s"Segment id(s) ${missing.mkString(", ")} not found$partitionContext in snapshot $name"
          )
        }
        val dataPartitions = data.iterator.map(_._1.partitionId).toSet
        withinPartitions.filter { case (segment, _) =>
          (segment.hasData && segmentSet.contains(segment.id)) ||
          (!segment.hasData &&
            (segment.partitionId == -1L || dataPartitions.contains(
              segment.partitionId
            )))
        }
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
