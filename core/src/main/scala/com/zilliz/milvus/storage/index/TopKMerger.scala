package com.zilliz.milvus.storage.index

import java.lang.{Double => JavaDouble, Long => JavaLong}

/** One candidate row for one query: where it is and what it scored.
  *
  * `score` is the metric score Knowhere returned, widened to a Double and
  * otherwise unchanged (docs/design/architecture/vector-search.html section
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

  /** Metrics whose smaller score is the better one. */
  private val SmallerIsBetter = Set("L2", "HAMMING", "JACCARD")

  private val Metrics = SmallerIsBetter ++ Set("IP", "COSINE")

  def metricRanks(metric: String): Boolean = Metrics.contains(metric)

  /** Whether this metric's better score is its smaller one. What
    * [[CandidateBytes]] needs to rank packed candidates without unpacking them
    * into objects first, and what [[TopKMerger]] needs to rank three primitives
    * without building one.
    */
  def smallerIsBetter(metric: String): Boolean = {
    require(metricRanks(metric), s"Unsupported vector search metric: $metric")
    SmallerIsBetter.contains(metric)
  }

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
  * pays the sort once, in [[takePacked]], and only for the k it kept
  * (docs/design/architecture/vector-search.html section 2.6).
  */
final class TopKMerger(val queries: Int, val k: Int, val metric: String)
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
  private val better: Int = if (Candidate.smallerIsBetter(metric)) 1 else -1

  // Query q owns slots [q * k, q * k + sizes(q)); slot q * k is its worst.
  private val scores = new Array[Double](queries * k)
  private val segments = new Array[Long](queries * k)
  private val offsets = new Array[Long](queries * k)
  private val sizes = new Array[Int](queries)

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
    JavaDouble.compare(scores(query * k), score) * better < 0
  }

  def addAll(candidates: Iterable[Candidate]): this.type = {
    candidates.iterator.foreach(add)
    this
  }

  /** Takes over `other`'s candidates. Both mergers must rank the same way. */
  def merge(other: TopKMerger): this.type = {
    require(
      other.queries == queries && other.k == k && other.metric == metric,
      "Merged searches must have the same query count, topK and metric"
    )
    var query = 0
    while (query < queries) {
      val base = query * k
      val size = other.sizes(query)
      var slot = 0
      while (slot < size) {
        val from = base + slot
        add(
          query,
          other.segments(from),
          other.offsets(from),
          other.scores(from)
        )
        slot += 1
      }
      query += 1
    }
    this
  }

  /** Orders the query's slots best first, in place. The heap's root is its
    * worst, so taking the root out to the end `size` times leaves the slots
    * sorted from best to worst; the heap is gone afterwards, which is why only
    * [[takePacked]] and [[results]] call this.
    */
  private def sortInPlace(base: Int, size: Int): Unit = {
    var end = size
    while (end > 1) {
      end -= 1
      swap(base, base + end)
      siftDown(base, 0, end)
    }
  }

  /** Best first; at most k for this query. Leaves the query as it was. */
  def results(query: Int): Vector[Candidate] = {
    require(
      query >= 0 && query < queries,
      s"Query $query is outside the $queries queries"
    )
    val base = query * k
    val size = sizes(query)
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
    built.result().sorted(Candidate.ranking(metric))
  }

  /** One query's results packed into bytes, best first, releasing the query's
    * heap as it reads it. This is what a stage-one task gives Spark.
    *
    * Releasing each heap keeps what a task holds at the `queries * k *
    * CandidateBytes` it planned against
    * (docs/design/architecture/vector-search.html section 2.1). Reading the
    * same query twice gives nothing the second time.
    */
  def takePacked(query: Int): Array[Byte] = {
    require(
      query >= 0 && query < queries,
      s"Query $query is outside the $queries queries"
    )
    val size = sizes(query)
    if (size == 0) return CandidateBytes.Empty
    val base = query * k
    sortInPlace(base, size)
    val packed =
      CandidateBytes.writeColumns(segments, offsets, scores, base, size)
    sizes(query) = 0
    packed
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
