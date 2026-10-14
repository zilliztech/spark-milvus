package com.zilliz.milvus.storage.index

import java.nio.ByteBuffer
import java.util.Arrays

import com.zilliz.milvus.storage.schema.MetricType

/** Builds sorted byte inputs for merge tests independently of TopKMerger. */
object CandidateBytesFixture {

  def pack(candidates: Seq[Candidate], metric: MetricType): Array[Byte] = {
    val sorted = candidates.toArray
    Arrays.sort(sorted, Candidate.ranking(metric))
    if (sorted.isEmpty) return CandidateBytes.Empty
    val packed = new Array[Byte](sorted.length * CandidateBytes.Width)
    val buffer = ByteBuffer.wrap(packed)
    var at = 0
    while (at < sorted.length) {
      val candidate = sorted(at)
      val offset = at * CandidateBytes.Width
      buffer.putLong(offset, candidate.segmentId)
      buffer.putLong(offset + 8, candidate.rowOffset)
      buffer.putDouble(offset + 16, candidate.score)
      at += 1
    }
    packed
  }
}
