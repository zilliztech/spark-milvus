package com.zilliz.milvus.storage.index

import java.lang.{Double => JavaDouble, Long => JavaLong}
import scala.collection.mutable

/** One candidate row for one query: where it is and what it scored.
  *
  * `score` is the metric score Knowhere returned, widened to a Double and
  * otherwise unchanged (docs/design/architecture/vector-search.html section
  * 2.6).
  */
final case class Candidate(
    query: Int,
    segmentId: Long,
    rowOffset: Long,
    score: Double
)

object Candidate {

  /** Metrics whose smaller score is the better one. */
  private val SmallerIsBetter = Set("L2", "HAMMING", "JACCARD")

  private val Metrics = SmallerIsBetter ++ Set("IP", "COSINE")

  def metricRanks(metric: String): Boolean = Metrics.contains(metric)

  /** Best first: by score, then by segment id, then by row offset. The two
    * tie-breakers make the result of a search independent of the order its
    * candidates arrived in.
    */
  def ranking(metric: String): Ordering[Candidate] = {
    require(metricRanks(metric), s"Unsupported vector search metric: $metric")
    val better = if (SmallerIsBetter.contains(metric)) 1 else -1
    (left: Candidate, right: Candidate) => {
      val score = JavaDouble.compare(left.score, right.score) * better
      if (score != 0) score
      else {
        val segment = JavaLong.compare(left.segmentId, right.segmentId)
        if (segment != 0) segment
        else JavaLong.compare(left.rowOffset, right.rowOffset)
      }
    }
  }
}

/** Keeps the best k candidates of every query and nothing else.
  *
  * A stage-one task merges its segments into one of these, and the Spark
  * aggregation merges the tasks the same way: adding candidates in any order,
  * or merging two mergers, gives the same result. Memory is bounded by `queries
  * * k` candidates, which is what `milvus.search.group.max.bytes` sizes a query
  * group against.
  */
final class TopKMerger(val queries: Int, val k: Int, val metric: String)
    extends Serializable {
  require(queries >= 0, s"Query count must not be negative: $queries")
  require(k > 0, s"topK must be positive: $k")

  private val ranking = Candidate.ranking(metric)

  /** A priority queue hands back its greatest element, and the greatest under
    * `ranking` is the worst candidate, which is the one a better one evicts.
    */
  private val heaps =
    Array.fill(queries)(mutable.PriorityQueue.empty[Candidate](ranking))

  def add(candidate: Candidate): Unit = {
    require(
      candidate.query >= 0 && candidate.query < queries,
      s"Candidate names query ${candidate.query} of $queries"
    )
    val heap = heaps(candidate.query)
    if (heap.size < k) heap.enqueue(candidate)
    else if (ranking.lt(candidate, heap.head)) {
      heap.dequeue()
      heap.enqueue(candidate)
    }
  }

  def addAll(candidates: IterableOnce[Candidate]): this.type = {
    candidates.iterator.foreach(add)
    this
  }

  /** Takes over `other`'s candidates. Both mergers must rank the same way. */
  def merge(other: TopKMerger): this.type = {
    require(
      other.queries == queries && other.k == k && other.metric == metric,
      "Merged searches must have the same query count, topK and metric"
    )
    other.heaps.foreach(_.foreach(add))
    this
  }

  /** Best first; at most k for this query. */
  def results(query: Int): Vector[Candidate] = {
    require(
      query >= 0 && query < queries,
      s"Query $query is outside the $queries queries"
    )
    heaps(query).toVector.sorted(ranking)
  }

  /** Every query's results, queries in order and each query's best first. This
    * is what a stage-one task gives Spark.
    */
  def candidates: Vector[Candidate] =
    (0 until queries).iterator.flatMap(results).toVector

  def size: Int = heaps.iterator.map(_.size).sum
}
