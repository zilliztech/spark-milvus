package com.zilliz.spark.connector.read

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

import com.zilliz.milvus.storage.index.{Candidate, TopKMerger}

/** One candidate as the merge stage reads it: the address of a row and what it
  * scored.
  */
private[read] case class SearchCandidate(
    segmentId: Long,
    rowOffset: Long,
    score: Double
)

/** One query's answer: its best k rows, best first. */
private[read] case class SearchHit(
    rank: Int,
    score: Double,
    segmentId: Long,
    rowOffset: Long
)

private[read] case class SearchHits(hits: Array[SearchHit])

/** Merges the candidates of one query into its global top-k.
  *
  * The buffer is the same bounded heap the first stage merges with, so adding
  * candidates in any order or merging two buffers gives the same answer, and
  * the memory one group of queries needs is what
  * `milvus.search.group.max.bytes` sizes (vector-search.html sections 2.1 and
  * 2.6).
  */
private[read] class TopKAggregator(k: Int, metric: String)
    extends Aggregator[SearchCandidate, TopKMerger, SearchHits] {

  override def zero: TopKMerger = new TopKMerger(1, k, metric)

  override def reduce(
      buffer: TopKMerger,
      candidate: SearchCandidate
  ): TopKMerger = {
    buffer.add(
      Candidate(0, candidate.segmentId, candidate.rowOffset, candidate.score)
    )
    buffer
  }

  override def merge(left: TopKMerger, right: TopKMerger): TopKMerger =
    left.merge(right)

  override def finish(buffer: TopKMerger): SearchHits = SearchHits(
    buffer
      .results(0)
      .iterator
      .zipWithIndex
      .map { case (candidate, index) =>
        SearchHit(
          index + 1,
          candidate.score,
          candidate.segmentId,
          candidate.rowOffset
        )
      }
      .toArray
  )

  override def bufferEncoder: Encoder[TopKMerger] =
    Encoders.javaSerialization[TopKMerger]

  override def outputEncoder: Encoder[SearchHits] = Encoders.product[SearchHits]
}
