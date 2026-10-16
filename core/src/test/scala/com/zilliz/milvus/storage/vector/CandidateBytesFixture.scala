package com.zilliz.milvus.storage.vector

import java.nio.ByteBuffer
import java.util.Arrays

import com.zilliz.milvus.storage.schema.MetricType

/** Builds sorted byte inputs for merge tests independently of TopKMerger. */
object CandidateBytesFixture {

  def pack(
      candidates: Seq[Candidate],
      metric: MetricType,
      encoding: CandidateEncoding
  ): Array[Byte] = {
    val sorted = candidates.toArray
    Arrays.sort(sorted, Candidate.ranking(metric))
    if (sorted.isEmpty) return CandidateBytes.Empty
    val packed = new Array[Byte](sorted.length * CandidateBytes.Width)
    val buffer = ByteBuffer.wrap(packed)
    var at = 0
    while (at < sorted.length) {
      val candidate = sorted(at)
      encoding.write(
        buffer,
        at * CandidateBytes.Width,
        candidate.segmentId,
        candidate.rowOffset,
        candidate.score
      )
      at += 1
    }
    packed
  }
}
