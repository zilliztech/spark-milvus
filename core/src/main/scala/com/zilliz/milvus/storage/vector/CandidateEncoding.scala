package com.zilliz.milvus.storage.vector

import java.nio.ByteBuffer
import scala.collection.immutable.LongMap

/** Encoding facts for one task, built once and shared read-only by its packing
  * threads. Encoding retains only this task's segment addresses, not the global
  * dictionary used to construct them.
  */
final class CandidateEncoding private (
    private val segments: LongMap[Int]
) extends Serializable {
  private[vector] def write(
      buffer: ByteBuffer,
      at: Int,
      segmentId: Long,
      rowOffset: Long,
      score: Float
  ): Unit = {
    val ordinal = segments.getOrElse(
      segmentId,
      throw new IllegalArgumentException(
        s"Segment $segmentId is absent from this task's candidate encoding"
      )
    )
    require(
      rowOffset >= 0L && rowOffset < CandidateBytes.MaxRows,
      s"Candidate row $rowOffset in segment $segmentId is outside UInt32 addresses"
    )
    buffer.putInt(at, ordinal)
    buffer.putInt(at + 4, rowOffset.toInt)
    buffer.putFloat(at + 8, score)
    ()
  }
}

object CandidateEncoding {
  def apply(
      dictionary: CandidateDictionary,
      segmentIds: Seq[Long]
  ): CandidateEncoding = {
    val segments = segmentIds.foldLeft(LongMap.empty[Int]) { (local, id) =>
      local.updated(id, dictionary.ordinal(id))
    }
    new CandidateEncoding(segments)
  }
}
