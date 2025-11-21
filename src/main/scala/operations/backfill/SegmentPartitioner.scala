package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.Partitioner

/**
 * Custom partitioner that ensures each segment goes to exactly one partition
 *
 * Maps each segment_id to a unique partition index via exact lookup table,
 * eliminating hash collisions entirely. This guarantees:
 * - Each partition contains exactly one segment's data
 * - Each segment's data is in exactly one partition
 *
 * @param segmentIds Array of all segment IDs in the collection
 */
class SegmentPartitioner(segmentIds: Array[Long]) extends Partitioner {

  // Create exact mapping: segment_id -> partition_index
  // Sorted to ensure deterministic partition assignment
  private val segmentToIndex: Map[Long, Int] = segmentIds.sorted.zipWithIndex.toMap

  override def numPartitions: Int = segmentIds.length

  override def getPartition(key: Any): Int = {
    val segmentId = key.asInstanceOf[Long]
    segmentToIndex.getOrElse(segmentId,
      throw new IllegalArgumentException(s"Unknown segment ID: $segmentId. Known segments: ${segmentIds.mkString(", ")}"))
  }

  override def equals(other: Any): Boolean = other match {
    case h: SegmentPartitioner => h.segmentToIndex == segmentToIndex
    case _ => false
  }

  override def hashCode(): Int = segmentToIndex.hashCode()
}
