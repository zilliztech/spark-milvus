package com.zilliz.milvus.storage.index

import scala.collection.mutable

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.VectorLayout

/** Splits one search into the tasks of its first stage.
  *
  * A task takes one segment set and one query group, so its memory is one batch
  * or one segment's index plus that group's top-k, and what it sends on is
  * bounded by "segment sets × queries × k" candidates
  * (docs/design/architecture/vector-search.html sections 1.1 and 2.1).
  */
object SearchPlan {

  /** What one candidate costs in the aggregation buffer: the query, the
    * segment, the row offset and the score.
    */
  val CandidateBytes: Int = 4 + 8 + 8 + 8

  /** A slice of the query set, by position in the query matrix. */
  final case class QueryGroup(firstQuery: Int, queries: Int) {
    require(firstQuery >= 0, s"A group starts at $firstQuery")
    require(queries > 0, s"A group holds $queries queries")
    def untilQuery: Int = firstQuery + queries
  }

  /** One first-stage task: the segments it reads and the queries it answers. */
  final case class Task(segments: Seq[SegmentReadTask], group: QueryGroup)

  /** How many queries fit one group: enough for their bounded top-k to stay
    * inside `groupMaxBytes`.
    */
  def queriesPerGroup(k: Int, groupMaxBytes: Long): Int = {
    require(k > 0, s"topK must be positive: $k")
    require(
      groupMaxBytes > 0,
      s"The group limit must be positive: $groupMaxBytes"
    )
    val perQuery = k.toLong * CandidateBytes
    require(
      perQuery <= groupMaxBytes,
      s"A single query's $k candidates need $perQuery bytes, over the group limit of $groupMaxBytes"
    )
    math.min(groupMaxBytes / perQuery, Int.MaxValue.toLong).toInt
  }

  def groups(queries: Int, k: Int, groupMaxBytes: Long): Seq[QueryGroup] = {
    require(queries > 0, s"A search needs at least one query: $queries")
    val size = queriesPerGroup(k, groupMaxBytes)
    (0 until queries by size).map(first =>
      QueryGroup(first, math.min(size, queries - first))
    )
  }

  /** Balances the segments over `sets` by the bytes their vector column holds,
    * largest first, so that every set reads about as much as the others.
    * Segments of equal size, which includes segments whose row count the
    * snapshot did not give, spread one per set. Segments keep their order
    * inside a set.
    */
  def segmentSets(
      tasks: Seq[SegmentReadTask],
      layout: VectorLayout,
      sets: Int
  ): Seq[Seq[SegmentReadTask]] = {
    require(sets > 0, s"A search needs at least one segment set: $sets")
    if (tasks.isEmpty) return Seq.empty
    val count = math.min(sets, tasks.size)
    val bytes = tasks.map(task =>
      task -> task.snapshotRows.getOrElse(0L) * layout.rowBytes.toLong
    )
    val filled = Array.fill(count)(0L)
    val members = Array.fill(count)(mutable.ArrayBuffer.empty[SegmentReadTask])
    bytes.sortBy { case (task, size) => (-size, task.segmentId) }.foreach {
      case (task, size) =>
        var lightest = 0
        var index = 1
        while (index < count) {
          val lighter = filled(index) < filled(lightest)
          val sameAndShorter = filled(index) == filled(lightest) &&
            members(index).size < members(lightest).size
          if (lighter || sameAndShorter) lightest = index
          index += 1
        }
        members(lightest) += task
        filled(lightest) += size
    }
    val order = tasks.zipWithIndex.map { case (task, index) =>
      task.segmentId -> index
    }.toMap
    members.iterator
      .filter(_.nonEmpty)
      .map(_.sortBy(task => order(task.segmentId)).toSeq)
      .toSeq
  }

  /** Every (segment set, query group) pair, segment sets in their planned order
    * and groups in query order.
    */
  def of(
      tasks: Seq[SegmentReadTask],
      layout: VectorLayout,
      executors: Int,
      queries: Int,
      k: Int,
      groupMaxBytes: Long
  ): Seq[Task] = {
    val sets = segmentSets(tasks, layout, executors)
    val slices = groups(queries, k, groupMaxBytes)
    for {
      set <- sets
      group <- slices
    } yield Task(set, group)
  }
}
