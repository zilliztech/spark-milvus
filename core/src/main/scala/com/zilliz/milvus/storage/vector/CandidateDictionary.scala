package com.zilliz.milvus.storage.vector

import java.util.Arrays

/** One search's global segment ordinals, ordered by signed Long segment id.
  * Only encoding and final address decoding need this dictionary; merge state
  * and candidate records carry no dictionary reference.
  */
final class CandidateDictionary private (
    private val ids: Array[Long]
) extends Serializable {
  def size: Int = ids.length

  def ordinal(segmentId: Long): Int = {
    val found = Arrays.binarySearch(ids, segmentId)
    require(
      found >= 0,
      s"Segment $segmentId is absent from the candidate dictionary"
    )
    found
  }

  private def index(ordinal: Long): Int = {
    require(
      ordinal >= 0L && ordinal < size.toLong,
      s"Candidate ordinal $ordinal is outside a dictionary of $size segments"
    )
    ordinal.toInt
  }

  def segmentId(ordinal: Long): Long = ids(index(ordinal))
}

object CandidateDictionary {

  /** Build one order for every task in the search, independently of task
    * assignment. Duplicate identities occupy one ordinal.
    */
  def apply(segmentIds: Seq[Long]): CandidateDictionary =
    new CandidateDictionary(segmentIds.distinct.sorted.toArray)
}
