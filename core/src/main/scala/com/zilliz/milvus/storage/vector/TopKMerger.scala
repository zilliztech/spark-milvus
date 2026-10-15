package com.zilliz.milvus.storage.vector

import java.lang.{Double => JavaDouble, Long => JavaLong}

import com.zilliz.milvus.storage.schema.MetricType

/** One candidate row for one query: where it is and what it scored.
  *
  * `score` is the metric score Knowhere returned, widened to a Double and
  * otherwise unchanged
  * (docs/design/projects/spark-milvus/architecture/vector-search.html section
  * 2.6). [[TopKMerger]] does not hold these: it holds the three numbers in
  * primitive arrays, and builds a `Candidate` only where a caller asks for one.
  */
final case class Candidate(
    query: Int,
    segmentId: Long,
    rowOffset: Long,
    score: Double
)

object Candidate {

  /** Best first: by score, then by segment id, then by row offset. The two
    * tie-breakers make the result of a search independent of the order its
    * candidates arrived in.
    */
  def ranking(metric: MetricType): Ordering[Candidate] = {
    val better = if (metric.smallerIsBetter) 1 else -1
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
  * A stage-one task merges its segments into one of these, and adding
  * candidates in any order gives the same result. Memory is bounded by `queries
  * * k` candidates, which is what `milvus.search.group.max.bytes` sizes a query
  * group against.
  *
  * Each query's k slots are a binary heap whose root is the worst candidate
  * kept, laid out in three primitive arrays rather than `Candidate` objects in
  * a `mutable.PriorityQueue`. A JFR profile of one P3 chunk (250,000 queries
  * over 74 segments on eight executors) put 46% of an executor's Java samples
  * in that queue and its `Ordering` and made `Candidate` the most allocated
  * class in the JVM; on primitives the same heap costs one comparison to reject
  * and `log k` swaps to keep, and allocates nothing. A sorted run was tried in
  * its place and measured 19 s slower per task: a task's first segment fills
  * every query's run from empty, and inserting 100 candidates into a sorted
  * array one at a time is an insertion sort, whatever the array holds. The heap
  * sorts only when the caller requests [[sort]], and only for the k it kept.
  * [[results]] reads the current order without ending collection; [[sort]]
  * finalizes one query before its ordered output, and [[takePacked]] requires
  * that explicit step
  * (docs/design/projects/spark-milvus/architecture/topk-merger.html).
  */
final class TopKMerger(val queries: Int, val k: Int, val metric: MetricType)
    extends Serializable {
  require(queries >= 0, s"Query count must not be negative: $queries")
  require(k > 0, s"topK must be positive: $k")
  require(
    queries.toLong * k.toLong <= Int.MaxValue.toLong,
    s"$queries queries by topK $k is more candidates than one heap holds"
  )

  /** +1 when the smaller score is the better one, so that a positive comparison
    * always means "left is worse than right".
    */
  private val better: Int = if (metric.smallerIsBetter) 1 else -1

  // Query q owns slots [q * k, q * k + sizes(q)). While collecting, its root
  // is the worst; after sort finalizes the query, its slots are best first.
  private val scores = new Array[Double](queries * k)
  private val segments = new Array[Long](queries * k)
  private val offsets = new Array[Long](queries * k)
  private val sizes = new Array[Int](queries)
  private val sorted = new Array[Boolean](queries)

  /** Negative when the candidate at `slot` is better than the given one, zero
    * when they are the same place with the same score, positive when worse.
    */
  private def compareAt(
      slot: Int,
      segmentId: Long,
      rowOffset: Long,
      score: Double
  ): Int = {
    val byScore = JavaDouble.compare(scores(slot), score) * better
    if (byScore != 0) byScore
    else {
      val bySegment = JavaLong.compare(segments(slot), segmentId)
      if (bySegment != 0) bySegment
      else JavaLong.compare(offsets(slot), rowOffset)
    }
  }

  /** Positive when the candidate at `left` is worse than the one at `right`. */
  private def compareSlots(left: Int, right: Int): Int =
    compareAt(left, segments(right), offsets(right), scores(right))

  private def swap(left: Int, right: Int): Unit = {
    val score = scores(left)
    scores(left) = scores(right)
    scores(right) = score
    val segment = segments(left)
    segments(left) = segments(right)
    segments(right) = segment
    val offset = offsets(left)
    offsets(left) = offsets(right)
    offsets(right) = offset
  }

  /** Moves the candidate at `base + at` up while it is worse than its parent.
    */
  private def siftUp(base: Int, at: Int): Unit = {
    var child = at
    while (child > 0) {
      val parent = (child - 1) >>> 1
      if (compareSlots(base + child, base + parent) > 0) {
        swap(base + child, base + parent)
        child = parent
      } else return
    }
  }

  /** Moves the candidate at `base + at` down while a child is worse than it. */
  private def siftDown(base: Int, at: Int, size: Int): Unit = {
    var parent = at
    while (true) {
      val left = 2 * parent + 1
      val right = left + 1
      var worst = parent
      if (left < size && compareSlots(base + left, base + worst) > 0)
        worst = left
      if (right < size && compareSlots(base + right, base + worst) > 0)
        worst = right
      if (worst == parent) return
      swap(base + parent, base + worst)
      parent = worst
    }
  }

  def add(candidate: Candidate): Unit = add(
    candidate.query,
    candidate.segmentId,
    candidate.rowOffset,
    candidate.score
  )

  /** Keeps this candidate when it is one of the query's best k.
    *
    * A full heap rejects on one comparison against its root, which is what most
    * candidates of most segments do once a query has seen one segment; one that
    * is kept replaces the root and sinks.
    */
  def add(query: Int, segmentId: Long, rowOffset: Long, score: Double): Unit = {
    require(
      query >= 0 && query < queries,
      s"Candidate names query $query of $queries"
    )
    if (sorted(query))
      throw new IllegalStateException(
        s"Query $query has been sorted and no longer accepts candidates"
      )
    val base = query * k
    val size = sizes(query)
    if (size < k) {
      val at = base + size
      scores(at) = score
      segments(at) = segmentId
      offsets(at) = rowOffset
      sizes(query) = size + 1
      siftUp(base, size)
    } else if (compareAt(base, segmentId, rowOffset, score) > 0) {
      scores(base) = score
      segments(base) = segmentId
      offsets(base) = rowOffset
      siftDown(base, 0, k)
    }
  }

  /** True when a candidate scoring this cannot be kept for this query, whatever
    * its place. The place only breaks ties, so a strictly worse score is
    * rejected whoever holds it; a caller with a segment's whole answer for a
    * query in hand asks this once per candidate before it looks anything up.
    */
  def rejectsScore(query: Int, score: Double): Boolean = {
    require(
      query >= 0 && query < queries,
      s"Query $query is outside the $queries queries"
    )
    sizes(query) == k &&
    JavaDouble.compare(scores(worstSlot(query)), score) * better < 0
  }

  private var scanned = 0L

  /** The rows exact scans have offered this merger so far, every query alike:
    * once the next batch is merged, about k times its rows over these and its
    * own are among the best k ([[ExactScan.nominated]]).
    */
  def scannedRows: Long = scanned

  def countScanned(rows: Int): Unit = {
    require(rows >= 0, s"A batch of $rows rows")
    scanned += rows
  }

  /** True once the query keeps k candidates: a candidate is then kept only when
    * it beats [[worstScore]].
    */
  def isFull(query: Int): Boolean = {
    require(
      query >= 0 && query < queries,
      s"Query $query is outside the $queries queries"
    )
    sizes(query) == k
  }

  /** The score of the worst candidate a full query keeps. */
  def worstScore(query: Int): Double = {
    require(isFull(query), s"Query $query keeps fewer than $k candidates")
    scores(worstSlot(query))
  }

  private def worstSlot(query: Int): Int =
    query * k + (if (sorted(query)) sizes(query) - 1 else 0)

  def addAll(candidates: Iterable[Candidate]): this.type = {
    candidates.iterator.foreach(add)
    this
  }

  /** Orders the query's slots best first, in place. The heap's root is its
    * worst, so taking the root out to the end `size` times leaves the slots
    * sorted from best to worst. Only the first explicit sort calls this, since
    * the heap is gone afterwards.
    */
  private def sortInPlace(base: Int, size: Int): Unit = {
    var end = size
    while (end > 1) {
      end -= 1
      swap(base, base + end)
      siftDown(base, 0, end)
    }
  }

  /** Sorts a query best first in place and ends its collection, once. All
    * writers of this query must have finished. Repeated calls keep the same
    * order without sorting again; even an empty query stops accepting adds.
    */
  def sort(query: Int): this.type = {
    val size = querySize(query)
    if (!sorted(query)) {
      if (size > 1) sortInPlace(query * k, size)
      sorted(query) = true
    }
    this
  }

  private def querySize(query: Int): Int = {
    require(
      query >= 0 && query < queries,
      s"Query $query is outside the $queries queries"
    )
    sizes(query)
  }

  /** An independent snapshot of this query's candidates in their current order.
    *
    * This neither sorts nor ends collection. Before [[sort]], the order is the
    * heap's and further additions are allowed; afterwards it is best first.
    * Later additions or sorting do not change the returned objects.
    */
  def results(query: Int): Vector[Candidate] = {
    val size = querySize(query)
    val base = query * k
    val built = Vector.newBuilder[Candidate]
    var slot = 0
    while (slot < size) {
      built += Candidate(
        query,
        segments(base + slot),
        offsets(base + slot),
        scores(base + slot)
      )
      slot += 1
    }
    built.result()
  }

  /** Encodes this query best first after the caller has explicitly [[sort]]ed
    * it, including when it is empty. Refuses an unsorted query without changing
    * its state: the merge stage requires ordered bytes. Repeated reads encode
    * the same candidates without sorting; each nonempty output owns its bytes.
    */
  def takePacked(query: Int): Array[Byte] = {
    val size = querySize(query)
    if (!sorted(query))
      throw new IllegalStateException(
        s"Query $query must be sorted before packing"
      )
    if (size == 0) CandidateBytes.Empty
    else CandidateBytes.writeColumns(segments, offsets, scores, query * k, size)
  }

  /** Where every kept candidate is, query by query, in no particular order:
    * what a caller holding something for each kept place, such as a DataFrame
    * input's rows, checks what it still needs against. Allocates nothing.
    */
  def foreachKept(each: (Long, Long) => Unit): Unit = {
    var query = 0
    while (query < queries) {
      val base = query * k
      val until = base + sizes(query)
      var slot = base
      while (slot < until) {
        each(segments(slot), offsets(slot))
        slot += 1
      }
      query += 1
    }
  }

  def size: Int = {
    var total = 0
    var query = 0
    while (query < queries) {
      total += sizes(query)
      query += 1
    }
    total
  }
}
