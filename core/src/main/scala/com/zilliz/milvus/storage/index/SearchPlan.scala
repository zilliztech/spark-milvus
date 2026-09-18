package com.zilliz.milvus.storage.index

import scala.collection.mutable

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.VectorLayout

/** Splits one search into the tasks of its first stage.
  *
  * A task takes one segment set and answers every query group on it. The set is
  * sized so that its vectors fit `milvus.search.vectors.max.bytes`, which is
  * what lets a task read its segments once and run group after group on what it
  * keeps; a group is sized so that its query matrix and its top-k fit
  * `milvus.search.group.max.bytes`. Task memory is therefore one set of vectors
  * and one group, and what a task sends on is bounded by "query groups ×
  * queries × k" candidates (docs/design/architecture/vector-search.html
  * sections 1.1 and 2.1).
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

  /** The first stage: one task per segment set, every group answered on every
    * set. The two sides stay apart because the query groups reach a task two
    * ways — inside a broadcast variable, or with the shuffle — while a task is
    * built from its segment set alone (section 2.1).
    */
  final case class Plan(
      sets: Seq[Seq[SegmentReadTask]],
      groups: Seq[QueryGroup]
  ) {
    def tasks: Int = sets.size
    def isEmpty: Boolean = sets.isEmpty
  }

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
    * A set holds at most `vectorsMaxBytes` of vectors, so a task can keep what
    * it read and answer one query group after another on it; there are at least
    * as many sets as executors, so every executor has work. Segments go in
    * largest first and each one joins the lightest set that still has room,
    * which spreads equal segments — including the ones whose row count the
    * snapshot did not give — one per set. Segments keep their order inside a
    * set.
    */
  def segmentSets(
      tasks: Seq[SegmentReadTask],
      layout: VectorLayout,
      executors: Int,
      vectorsMaxBytes: Long
  ): Seq[Seq[SegmentReadTask]] = {
    require(executors > 0, s"A search needs at least one executor: $executors")
    require(
      vectorsMaxBytes > 0,
      s"The retained vector limit must be positive: $vectorsMaxBytes"
    )
    if (tasks.isEmpty) return Seq.empty
    val sizes =
      tasks.map(task => task.segmentId -> segmentBytes(task, layout)).toMap
    val total = sizes.values.sum
    val needed = math.max(1L, (total + vectorsMaxBytes - 1L) / vectorsMaxBytes)
    val start =
      math.min(tasks.size.toLong, math.max(executors.toLong, needed)).toInt
    val filled = mutable.ArrayBuffer.fill(start)(0L)
    val members = mutable.ArrayBuffer.fill(start)(
      mutable.ArrayBuffer.empty[SegmentReadTask]
    )
    tasks.sortBy(task => (-sizes(task.segmentId), task.segmentId)).foreach {
      task =>
        val size = sizes(task.segmentId)
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
          filled(lightest) + size > vectorsMaxBytes
        if (full) {
          filled += size
          members += mutable.ArrayBuffer(task)
        } else {
          members(lightest) += task
          filled(lightest) += size
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

  private def segmentBytes(
      task: SegmentReadTask,
      layout: VectorLayout
  ): Long = task.snapshotRows.getOrElse(0L) * layout.rowBytes.toLong

  /** The first-stage tasks of one search: the segment sets and the query groups
    * they each answer.
    */
  def of(
      tasks: Seq[SegmentReadTask],
      layout: VectorLayout,
      executors: Int,
      queries: Int,
      k: Int,
      groupMaxBytes: Long,
      vectorsMaxBytes: Long
  ): Plan = Plan(
    segmentSets(tasks, layout, executors, vectorsMaxBytes),
    groups(queries, layout, k, groupMaxBytes)
  )

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
    val bytes = set.map(segmentBytes(_, layout)).sum
    math.min(math.max(bytes, layout.rowBytes.toLong), vectorsMaxBytes)
  }
}
