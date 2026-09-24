package com.zilliz.milvus.storage.index

import scala.collection.mutable

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.milvus.storage.snapshot.SegmentIndexes

/** Splits one search into the tasks of its first stage.
  *
  * A task takes one segment set and a range of query groups and answers every
  * group of the range on every segment of the set. A group is sized so that its
  * query matrix and its top-k fit `milvus.search.group.max.bytes`.
  *
  * How many sets there are, and which side a task keeps in memory, follow from
  * how many tasks the search runs at once and the two budgets of one task: its
  * queries' budget on the heap and its segments' budget off it
  * (docs/design/architecture/vector-search.html section 2.1, search-resources
  * .html section 3.3). A task that can keep its queries reads every segment of
  * its set once, one at a time, so there are as many sets as tasks run at once:
  * each set is then read once and the query set is taken once per set, which is
  * the least either costs. A task that cannot keeps its segment set and the
  * query groups pass through it, so a set holds no more than the segment budget
  * and there are more sets when the segments need them.
  *
  * An exact search with fewer sets than tasks run at once cuts its queries into
  * contiguous ranges, one task per (set, range): the batched distance entry
  * runs single-threaded on its task, so the tasks are the search's parallelism.
  * Each range reads its set again, so no range holds fewer than
  * [[MinRangeQueries]] queries; when the groups the byte limit cuts are fewer
  * than the ranges, the query set is cut into one group per range instead. An
  * index search keeps one range, since a range would load the set's indexes
  * again (decisions 28 and 32).
  */
object SearchPlan {

  /** What one candidate is planned to cost a task while the search runs.
    *
    * A planning constant. The figure dates from when a candidate was a
    * `Candidate` object in the query's own priority queue: on a 64-bit JVM with
    * compressed ordinary object pointers a 12-byte header over an `Int`, two
    * `Long`s and a `Double`, 40 bytes once aligned, plus the queue's reference
    * to it. [[TopKMerger]] now lays each query's k slots out in three primitive
    * arrays (score, segment, row offset), 24 bytes per candidate and no object;
    * the constant keeps the old figure as headroom, so a task planned against a
    * budget stays under it. Counting the fields alone once said 28 and a task
    * planned against a budget it then overran.
    *
    * Packing a query's answer for the shuffle costs `k * CandidateBytes.Width`
    * on top, and [[TopKMerger.takePacked]] releases each query's heap as it
    * packs it, so a task's peak stays at what this counts rather than holding
    * both forms of every candidate at once.
    */
  val CandidateBytes: Int = 40 + 8

  /** What a query's top-k is planned to keep besides its candidates. The figure
    * dates from the priority queue and the backing array of sixteen references
    * it started with; [[TopKMerger]] now keeps only a per-query size in a
    * shared `Int` array, and the constant stays as headroom.
    */
  val TopKQueueBytes: Int = 128

  /** A query's id, which a task keeps to name its candidates. */
  val QueryIdBytes: Int = 8

  /** The side of the search a task keeps in memory while the other side passes
    * through it once.
    */
  sealed trait Resident
  object Resident {

    /** The task's queries stay: each segment of its set is opened once,
      * searched for every query group and closed before the next one opens.
      */
    case object Queries extends Resident

    /** The task's segment set stays: the query groups arrive one at a time and
      * each is searched on the whole set.
      */
    case object Segments extends Resident
  }

  /** What one segment costs, off the heap, the task that searches it.
    *
    * @param kept
    *   the bytes the segment holds while a task keeps it -- its loaded index,
    *   or its vector batches -- or None when nothing recorded its size and the
    *   plan estimates
    * @param loading
    *   the bytes held beside `kept` while a segment loaded whole is loading, or
    *   None when unknown, and the plan then counts `kept` again
    * @param whole
    *   true when the segment is searched only once it is loaded whole, as an
    *   index is: searching it costs `kept` and, while it loads, `loading` too.
    *   False when it is scanned a block at a time, as vectors are
    * @param blockBytes
    *   what a scan holds at once, when `whole` is false
    */
  final case class Footprint(
      kept: Option[Long],
      loading: Option[Long],
      whole: Boolean,
      blockBytes: Long
  ) {
    require(
      kept.forall(_ >= 0L) && loading.forall(_ >= 0L) && blockBytes >= 0L,
      s"A footprint is never negative: $this"
    )
  }

  object Footprint {

    /** Copies of a persisted index's bytes that a loaded index holds: the copy
      * Knowhere's C API keeps of the BinarySet for as long as the index lives
      * (knowhere `src/c_api/c_api.cc`, `IndexResource.deserialized_data`), and
      * the index deserialized from it. Measured 1.99 and 2.04 on
      * perf_laion_31m's Cardinal indexes (search-resources.html section 3.3).
      */
    val IndexKeptCopies: Int = 2

    /** Copies held beside those while an index loads: the BinarySet the
      * connector read the files into, released once Knowhere has deserialized.
      */
    val IndexLoadingCopies: Int = 1

    /** A persisted index of `bytes`, as the snapshot recorded it. */
    def index(bytes: Long): Footprint = Footprint(
      Some(IndexKeptCopies.toLong * bytes),
      Some(IndexLoadingCopies.toLong * bytes),
      true,
      0L
    )

    /** A persisted index whose size nothing recorded. */
    val unsizedIndex: Footprint = Footprint(None, None, true, 0L)

    /** Vectors scanned `blockBytes` at a time, kept as the batches they arrive
      * in.
      */
    def vectors(
        task: SegmentReadTask,
        layout: VectorLayout,
        blockBytes: Long
    ): Footprint = Footprint(
      knownRows(task).map(_ * layout.rowBytes.toLong),
      Some(0L),
      false,
      blockBytes
    )
  }

  /** What one task may keep: segment data off the heap and queries on it. */
  final case class Budget(segmentBytes: Long, queryBytes: Long) {
    require(
      segmentBytes > 0L && queryBytes >= 0L,
      s"A task's budget must be positive: $this"
    )
  }

  /** What one task of the plan needs under either order, which the driver logs
    * beside the budget each was compared to.
    */
  final case class Needs(
      queriesHeap: Long,
      queriesOffHeap: Long,
      segmentsOffHeap: Long
  )

  /** A slice of the query set, by position in the query matrix. */
  final case class QueryGroup(firstQuery: Int, queries: Int) {
    require(firstQuery >= 0, s"A group starts at $firstQuery")
    require(queries > 0, s"A group holds $queries queries")
    def untilQuery: Int = firstQuery + queries
  }

  /** The first stage: one task per (segment set, query range), every group of a
    * range answered on every set. The two sides stay apart because the query
    * groups reach a task two ways — inside a broadcast variable, or with the
    * shuffle — while a task is built from its segment set and its range's index
    * (section 2.1). `ranges` holds group indices, contiguous and in order, and
    * covers every group once.
    *
    * `resident` is the side a task keeps. When it keeps its segments,
    * `capacity` is the most a set may keep and `kept` says, set by set, what
    * the plan knows the set keeps, so a task whose set is known to be over
    * streams from the start.
    */
  final case class Plan(
      sets: Seq[Seq[SegmentReadTask]],
      groups: Seq[QueryGroup],
      estimated: Seq[Long] = Seq.empty,
      ranges: Seq[Range] = Seq.empty,
      resident: Resident = Resident.Segments,
      capacity: Long = Long.MaxValue,
      kept: Seq[Option[Long]] = Seq.empty,
      needs: Option[Needs] = None
  ) {
    require(
      ranges.isEmpty || ranges.flatten == groups.indices,
      s"Query ranges ${ranges.mkString(", ")} do not cover ${groups.size} groups in order"
    )

    /** The ranges the tasks take: all groups in one when none were cut. */
    def queryRanges: Seq[Range] =
      if (ranges.isEmpty) Seq(groups.indices) else ranges

    def tasks: Int = sets.size * queryRanges.size
    def isEmpty: Boolean = sets.isEmpty
  }

  /** The share of a task's budget an unknown-sized segment is planned at. Half:
    * two such segments fill a set, one is never alone in a set for nothing, and
    * the task that reads the set streams instead of holding when the segments
    * turn out larger (docs/design/architecture/search-resources.html section
    * 3.3).
    */
  val UnknownSegmentShare: Double = 0.5

  /** What one query costs a task: its row in the query matrix and its bounded
    * top-k.
    */
  def bytesPerQuery(layout: VectorLayout, k: Int): Long = {
    require(k > 0, s"topK must be positive: $k")
    layout.rowBytes.toLong + k.toLong * CandidateBytes
  }

  /** How many queries fit one group: enough for their query matrix and their
    * bounded top-k to stay inside `groupMaxBytes`.
    */
  def queriesPerGroup(
      layout: VectorLayout,
      k: Int,
      groupMaxBytes: Long
  ): Int = {
    require(
      groupMaxBytes > 0,
      s"The group limit must be positive: $groupMaxBytes"
    )
    val perQuery = bytesPerQuery(layout, k)
    require(
      perQuery <= groupMaxBytes,
      s"One query needs $perQuery bytes for its ${layout.dimension} dimensions and $k candidates, over the group limit of $groupMaxBytes"
    )
    math.min(groupMaxBytes / perQuery, Int.MaxValue.toLong).toInt
  }

  def groups(
      queries: Int,
      layout: VectorLayout,
      k: Int,
      groupMaxBytes: Long
  ): Seq[QueryGroup] = {
    require(queries > 0, s"A search needs at least one query: $queries")
    val size = queriesPerGroup(layout, k, groupMaxBytes)
    (0 until queries by size).map(first =>
      QueryGroup(first, math.min(size, queries - first))
    )
  }

  /** Splits the segments into the sets one task each reads.
    *
    * A set keeps at most `capacity` bytes, as `size` counts them; there are at
    * least as many sets as `slots`, so every task that runs at once has work,
    * unless the search has fewer segments than that. Segments go in largest
    * first and each one joins the lightest set that still has room, which
    * spreads equal segments one per set, and a segment that fits no set opens
    * one of its own. Segments keep their order inside a set.
    */
  def segmentSets(
      tasks: Seq[SegmentReadTask],
      slots: Int,
      capacity: Long,
      size: SegmentReadTask => Long
  ): Seq[Seq[SegmentReadTask]] = {
    require(slots > 0, s"A search needs at least one task slot: $slots")
    require(capacity > 0, s"A set's capacity must be positive: $capacity")
    if (tasks.isEmpty) return Seq.empty
    val sizes = tasks.map(task => task.segmentId -> size(task)).toMap
    val total = sizes.values.sum
    val needed =
      math.max(1L, total / capacity + (if (total % capacity == 0L) 0L else 1L))
    val start =
      math.min(tasks.size.toLong, math.max(slots.toLong, needed)).toInt
    val filled = mutable.ArrayBuffer.fill(start)(0L)
    val members = mutable.ArrayBuffer.fill(start)(
      mutable.ArrayBuffer.empty[SegmentReadTask]
    )
    tasks.sortBy(task => (-sizes(task.segmentId), task.segmentId)).foreach {
      task =>
        val bytes = sizes(task.segmentId)
        var lightest = 0
        var index = 1
        while (index < filled.size) {
          val lighter = filled(index) < filled(lightest)
          val sameAndShorter = filled(index) == filled(lightest) &&
            members(index).size < members(lightest).size
          if (lighter || sameAndShorter) lightest = index
          index += 1
        }
        val full = members(lightest).nonEmpty &&
          filled(lightest) + bytes > capacity
        if (full) {
          filled += bytes
          members += mutable.ArrayBuffer(task)
        } else {
          members(lightest) += task
          filled(lightest) += bytes
        }
    }
    val order = tasks.zipWithIndex.map { case (task, index) =>
      task.segmentId -> index
    }.toMap
    members.iterator
      .filter(_.nonEmpty)
      .map(_.sortBy(task => order(task.segmentId)).toSeq)
      .toSeq
  }

  /** A segment's vectors in bytes, or `budget × UnknownSegmentShare` when its
    * row count is unknown: the size exact search plans a set by.
    */
  def vectorBytes(
      layout: VectorLayout,
      budget: Long
  ): SegmentReadTask => Long = {
    val unknown = unknownSegmentBytes(layout, budget)
    task => knownRows(task).map(_ * layout.rowBytes.toLong).getOrElse(unknown)
  }

  /** The rows a segment is known to hold: from the snapshot, from its column
    * groups, or from the row count its persisted index was built over. None
    * when nothing recorded it, which is a V3 segment listed by manifest alone.
    */
  def knownRows(task: SegmentReadTask): Option[Long] =
    task.expectedRows.orElse(task.indexes match {
      case SegmentIndexes.Available(indexes) =>
        indexes.map(_.rowCount).filter(_ > 0L).reduceOption(_ max _)
      case _ => None
    })

  /** The bytes of vectors a set is known to hold, or None when any of its
    * segments has no row count. What a task compares to its budget before it
    * reads: a set known to be larger streams from the start.
    */
  def knownBytes(
      set: Seq[SegmentReadTask],
      layout: VectorLayout
  ): Option[Long] = {
    val rows = set.map(knownRows)
    if (rows.forall(_.isDefined)) Some(rows.flatten.sum * layout.rowBytes)
    else None
  }

  private def unknownSegmentBytes(
      layout: VectorLayout,
      budget: Long
  ): Long =
    math.max(layout.rowBytes.toLong, (budget * UnknownSegmentShare).toLong)

  /** The fewest queries a range of an exact search holds.
    *
    * A range reads its whole segment set, and computing a block takes the time
    * reading it does once a range holds about 2,300 queries: a float32 row is
    * 4d bytes read at the 110 MB/s one task reads (decision 31) against 2d
    * floating-point operations a query at the 125 GFLOP/s of one core in the
    * batched entry (decision 27), whatever the dimension d. A range under that
    * waits on its reads, so another range only reads the set once more; rounded
    * down to a power of two (decision 32).
    */
  val MinRangeQueries: Int = 2048

  /** How many ranges an exact search cuts `queries` queries into on `sets`
    * sets: as many as it takes for the sets to fill `slots` slots, but no range
    * under [[MinRangeQueries]] queries, and at least one. One when `split` is
    * false.
    */
  def rangeCount(queries: Int, sets: Int, slots: Int, split: Boolean): Int = {
    require(queries > 0, s"A search needs at least one query: $queries")
    require(sets > 0, s"A search needs at least one segment set: $sets")
    require(slots > 0, s"A search needs at least one task slot: $slots")
    if (!split) 1
    else
      math.max(
        1,
        math.min((slots + sets - 1) / sets, queries / MinRangeQueries)
      )
  }

  /** Cuts `groups` query groups into `ranges` contiguous ranges, as even as the
    * counts allow with the longer ranges first.
    */
  def queryRanges(groups: Int, ranges: Int): Seq[Range] = {
    require(groups > 0, s"A search needs at least one query group: $groups")
    require(
      ranges > 0 && ranges <= groups,
      s"$ranges ranges cannot each take a group of $groups"
    )
    val base = groups / ranges
    val longer = groups % ranges
    (0 until ranges)
      .scanLeft(0) { (start, index) =>
        start + base + (if (index < longer) 1 else 0)
      }
      .sliding(2)
      .map { case Seq(start, end) => start until end }
      .toSeq
  }

  /** The query set cut into `count` contiguous groups, as even as the counts
    * allow with the longer groups first: how an exact search gets a group for
    * each of its ranges when the byte limit cuts fewer. Each group is smaller
    * than the limit's, because the limit's groups were fewer.
    */
  def evenGroups(queries: Int, count: Int): Seq[QueryGroup] = {
    require(queries > 0, s"A search needs at least one query: $queries")
    require(
      count > 0 && count <= queries,
      s"$queries queries cannot fill $count groups"
    )
    val base = queries / count
    val longer = queries % count
    (0 until count)
      .scanLeft(0) { (start, index) =>
        start + base + (if (index < longer) 1 else 0)
      }
      .sliding(2)
      .map { case Seq(start, end) => QueryGroup(start, end - start) }
      .toSeq
  }

  /** The first-stage tasks of one search: the segment sets, the query groups,
    * the ranges of groups each task answers on its set, and the side a task
    * keeps (docs/design/architecture/vector-search.html section 2.1).
    *
    * @param concurrency
    *   how many first-stage tasks run at once in the whole search
    * @param budget
    *   what one of those tasks may keep
    * @param footprint
    *   what one segment costs the task that searches it
    * @param shuffled
    *   true when the query groups reach a task from the shuffle, so a task that
    *   keeps its queries has one group's bytes on its heap as it arrives; a
    *   broadcast set is on the heap once for the executor
    * @param splitQueries
    *   true for an exact search, whose tasks are its parallelism
    */
  def of(
      tasks: Seq[SegmentReadTask],
      layout: VectorLayout,
      concurrency: Int,
      queries: Int,
      k: Int,
      groupMaxBytes: Long,
      budget: Budget,
      footprint: SegmentReadTask => Footprint,
      shuffled: Boolean,
      splitQueries: Boolean = false
  ): Plan = {
    require(concurrency > 0, s"A search runs at least one task: $concurrency")
    val limited = groups(queries, layout, k, groupMaxBytes)
    if (tasks.isEmpty) return Plan(Seq.empty, limited)
    val prints = tasks.map(task => task.segmentId -> footprint(task)).toMap
    val unknown = unknownSegmentBytes(layout, budget.segmentBytes)
    def kept(task: SegmentReadTask): Long =
      prints(task.segmentId).kept.getOrElse(unknown)
    def loading(task: SegmentReadTask): Long = {
      val print = prints(task.segmentId)
      if (print.whole) print.loading.getOrElse(kept(task)) else 0L
    }
    def searching(task: SegmentReadTask): Long = {
      val print = prints(task.segmentId)
      if (print.whole) kept(task) + loading(task) else print.blockBytes
    }
    def knownKept(set: Seq[SegmentReadTask]): Option[Long] = {
      val bytes = set.map(task => prints(task.segmentId).kept)
      if (bytes.forall(_.isDefined)) Some(bytes.flatten.sum) else None
    }
    // The groups and ranges for `sets` sets. When the byte limit cuts fewer
    // groups than the ranges, the query set is cut into one group per range,
    // each smaller than the limit's (decision 32).
    def cut(sets: Int): (Seq[QueryGroup], Seq[Range]) = {
      val count = rangeCount(queries, sets, concurrency, splitQueries)
      val cutGroups =
        if (limited.size >= count) limited else evenGroups(queries, count)
      (cutGroups, queryRanges(cutGroups.size, count))
    }
    val rowBytes = layout.rowBytes.toLong
    val estimated =
      tasks.filter(task => prints(task.segmentId).kept.isEmpty).map(_.segmentId)

    // Keeping the queries reads each segment once, so a set for every task
    // that runs at once is the fewest sets the search can have.
    val streamedSets = segmentSets(tasks, concurrency, Long.MaxValue, kept)
    val (streamedGroups, streamedRanges) = cut(streamedSets.size)
    val rangeQueries = streamedRanges
      .map(_.map(index => streamedGroups(index).queries.toLong).sum)
      .max
    val perQuery =
      QueryIdBytes.toLong + k.toLong * CandidateBytes + TopKQueueBytes
    val arriving =
      if (shuffled)
        streamedGroups.map(_.queries.toLong).max *
          (QueryIdBytes.toLong + rowBytes)
      else 0L
    val queriesHeap = rangeQueries * perQuery + arriving
    val queriesOffHeap = rangeQueries * rowBytes + tasks.map(searching).max

    // Keeping the segments holds a set, the largest load beside it and the
    // matrix of the group being searched. The byte limit's largest group
    // bounds any group a cut for more ranges makes.
    val extra =
      tasks.map(loading).max + limited.map(_.queries.toLong).max * rowBytes
    val capacity = math.max(rowBytes, budget.segmentBytes - extra)
    val keptSets = segmentSets(tasks, concurrency, capacity, kept)
    val segmentsOffHeap = keptSets.map(_.map(kept).sum).max + extra

    val needs = Needs(queriesHeap, queriesOffHeap, segmentsOffHeap)
    if (
      queriesHeap <= budget.queryBytes && queriesOffHeap <= budget.segmentBytes
    )
      Plan(
        streamedSets,
        streamedGroups,
        estimated,
        streamedRanges,
        Resident.Queries,
        Long.MaxValue,
        streamedSets.map(knownKept),
        Some(needs)
      )
    else {
      val (keptGroups, keptRanges) = cut(keptSets.size)
      Plan(
        keptSets,
        keptGroups,
        estimated,
        keptRanges,
        Resident.Segments,
        capacity,
        keptSets.map(knownKept),
        Some(needs)
      )
    }
  }

  /** How many bytes of vectors one task keeps at once: its whole segment set,
    * which the planner sized to fit, or `vectorsMaxBytes` when one segment is
    * larger than that on its own. A segment is read once either way; what a
    * task keeps is what the query groups after the first one run on
    * (docs/design/architecture/vector-search.html section 2.3).
    */
  def retainedBytes(
      set: Seq[SegmentReadTask],
      layout: VectorLayout,
      vectorsMaxBytes: Long
  ): Long = {
    require(
      vectorsMaxBytes > 0,
      s"The retained vector limit must be positive: $vectorsMaxBytes"
    )
    val bytes = set.map(vectorBytes(layout, vectorsMaxBytes)).sum
    math.min(math.max(bytes, layout.rowBytes.toLong), vectorsMaxBytes)
  }
}
