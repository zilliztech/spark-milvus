package com.zilliz.spark.connector.read

import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{
  DoubleType,
  LongType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext

import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.milvus.storage.index.{
  QueryMatrix,
  SearchPlan,
  SegmentSearch,
  TopKMerger
}
import com.zilliz.milvus.storage.read.exec.{
  RowExclusions,
  SegmentIndexHandle,
  SegmentVectors
}
import com.zilliz.milvus.storage.read.exec.ReadMetrics
import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.SearchResources
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.CollectionSchema

/** One first-stage task: one segment set, every query group of its range, the
  * candidates they produce.
  *
  * The task opens what the Milvus format side offers for each of its segments —
  * the vector batches of an exact scan, or the index handle of an index probe —
  * and the computation in `core.index` searches it. The plan chose which side
  * the task keeps (docs/design/architecture/vector-search.html section 2.1).
  * Keeping its queries, the task takes every group of its range first and reads
  * each segment once, one at a time, searching every group on it; that is also
  * what a task with one group does. Keeping its segments, it reads the set into
  * memory and the groups arrive one after another. Either way the segments are
  * read once, whichever way the query set was delivered; a kept set larger than
  * the task's budget is read again for every group instead
  * (search-resources.html section 3.3).
  */
private[read] object SegmentSetSearch extends Logging {

  private val chosenBatches =
    new java.util.concurrent.ConcurrentHashMap[(Int, Option[Long]), Long]()

  /** The exact scan's base block for this JVM: the option when the call set it,
    * otherwise the default (docs/design/architecture/search-resources.html
    * section 3.2). Logged once per JVM for each distinct request.
    */
  private[read] def exactScanBatch(spec: Spec): Long =
    chosenBatches.computeIfAbsent(
      (spec.slots, spec.batchMaxBytes),
      _ => {
        val choice = SearchResources.exactScanBatch(spec.batchMaxBytes)
        logInfo(s"exact scan batch: ${choice.reason}")
        choice.bytes
      }
    )

  /** What every task of one search needs and the driver already knows.
    * `keptMaxBytes` is the most a task that keeps its segment set may keep, and
    * `slots` the search tasks that run at once on one executor.
    */
  final case class Spec(
      vectorColumn: String,
      layout: VectorLayout,
      fieldId: Long,
      nullable: Boolean,
      k: Int,
      metric: String,
      mode: String,
      filter: Option[String],
      parameters: Map[String, String],
      allowUnindexed: Boolean,
      keptMaxBytes: Long,
      arrowMaxBytes: Long,
      slots: Int,
      batchMaxBytes: Option[Long],
      resident: SearchPlan.Resident = SearchPlan.Resident.Segments
  ) extends Serializable

  /** What one task sends on: at most k candidates for each of its queries. */
  val CandidateSchema: StructType = StructType(
    Seq(
      StructField("query_id", LongType, nullable = false),
      StructField("segment_id", LongType, nullable = false),
      StructField("row_offset", LongType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    )
  )

  /** @param plannedBytes
    *   what the plan knows the set keeps, when it knows every segment's size; a
    *   task keeping its segments streams from the start when this is over its
    *   budget
    */
  def run(
      set: Seq[MilvusInputPartition],
      spec: Spec,
      groups: Iterator[SearchQueries.Group],
      groupCount: Int,
      plannedBytes: Option[Long],
      metrics: SearchMetrics
  ): Iterator[Row] = {
    require(set.nonEmpty, "A first-stage task has no segments")
    require(groupCount > 0, s"A task answers $groupCount query groups")
    val segments = set.map(_.task.segmentId)
    val partitions =
      set.map(partition => partition.task.segmentId -> partition).toMap
    val allocator = ArrowAllocator.forSearchTask(
      TaskContext.get().partitionId(),
      spec.arrowMaxBytes
    )
    // Every step goes to the accumulators as it finishes. A task of this
    // stage runs for as long as its vectors take, and Spark carries a running
    // task's accumulators on the executor heartbeat, so a total added at the
    // end is a total nobody can see while it matters.
    def stepped(step: SegmentSearch.Progress): Unit = {
      if (step.nativeCalls != 0)
        metrics.knowhereCalls.add(step.nativeCalls.toLong)
      if (step.nativeNanos != 0L) metrics.knowhereNanos.add(step.nativeNanos)
      if (step.compared != 0L) metrics.comparedPairs.add(step.compared)
      if (step.segments != 0) metrics.segmentSearches.add(step.segments.toLong)
    }
    def open(segmentId: Long): SegmentSearch.Source =
      source(partitions(segmentId), spec, allocator.allocator, metrics)
    // One group over the set, reading every segment as it goes: what a task
    // keeping a set too large to hold does for each group.
    def streamed(group: SearchQueries.Group): Seq[Row] = {
      val (merger, counters) = searching(group, spec, allocator.allocator) {
        queries =>
          SegmentSearch.run(
            segments,
            open,
            queries,
            spec.k,
            spec.metric,
            spec.parameters,
            allocator.allocator,
            stepped
          )
      }
      report(metrics, counters, merger.size)
      logInfo(
        s"Search task: segments=${counters.segments}, groups=$groupCount, " +
          s"queries=${group.queries}, candidates=${merger.size}, streamed"
      )
      candidates(merger, group.ids)
    }
    if (groupCount == 1 || spec.resident == SearchPlan.Resident.Queries) {
      try
        keepingQueries(
          segments,
          open,
          groups,
          groupCount,
          spec,
          allocator.allocator,
          metrics,
          stepped
        )
      finally allocator.close()
    } else {
      val held = holdOrStream(plannedBytes, spec, segments, open, metrics)
      Option(TaskContext.get()).foreach(
        _.addTaskCompletionListener[Unit] { _ =>
          try held.foreach(_.close())
          finally allocator.close()
        }
      )
      held match {
        case None => groups.flatMap(streamed)
        case Some(kept) =>
          groups.flatMap { group =>
            val (merger, counters) =
              searching(group, spec, allocator.allocator) { queries =>
                kept.search(
                  queries,
                  spec.k,
                  spec.metric,
                  spec.parameters,
                  allocator.allocator,
                  stepped
                )
              }
            report(metrics, counters, merger.size)
            logInfo(
              s"Search task: segments=${counters.segments}, groups=$groupCount, " +
                s"queries=${group.queries}, candidates=${merger.size}, held"
            )
            candidates(merger, group.ids)
          }
      }
    }
  }

  /** The task's queries stay and each segment is read once.
    *
    * Every group of the range is taken before the first segment opens: its
    * vectors are copied into a query matrix as it arrives and its heap bytes
    * are let go, so what the task keeps on the heap is ids and top-k. The
    * candidates go out group by group, each group's merger released once its
    * rows are out (docs/design/architecture/vector-search.html section 2.1).
    */
  private def keepingQueries(
      segments: Seq[Long],
      open: Long => SegmentSearch.Source,
      groups: Iterator[SearchQueries.Group],
      groupCount: Int,
      spec: Spec,
      allocator: BufferAllocator,
      metrics: SearchMetrics,
      stepped: SegmentSearch.Progress => Unit
  ): Iterator[Row] = {
    val ids = new Array[Array[Long]](groupCount)
    val matrices = new Array[QueryMatrix](groupCount)
    var taken = 0
    try {
      groups.foreach { group =>
        require(
          taken < groupCount,
          s"A task answering $groupCount query groups was given more"
        )
        ids(taken) = group.ids
        matrices(taken) = QueryMatrix.ofPacked(
          group.vectors,
          group.firstQuery,
          group.queries,
          spec.layout,
          allocator
        )
        taken += 1
      }
      require(
        taken == groupCount,
        s"A task answering $groupCount query groups was given $taken"
      )
      val (mergers, counters) = SegmentSearch.runGroups(
        segments,
        open,
        matrices.toSeq,
        spec.k,
        spec.metric,
        spec.parameters,
        allocator,
        stepped
      )
      val kept = mergers.toArray
      report(metrics, counters, kept.iterator.map(_.size).sum)
      logInfo(
        s"Search task: segments=${counters.segments}, groups=$groupCount, " +
          s"queries=${ids.iterator.map(_.length).sum}, " +
          s"candidates=${kept.iterator.map(_.size).sum}, queries kept"
      )
      Iterator.range(0, groupCount).flatMap { index =>
        val rows = candidates(kept(index), ids(index))
        kept(index) = null
        ids(index) = null
        rows
      }
    } finally matrices.iterator.filter(_ != null).foreach(_.close())
  }

  /** The segment set read into memory for the groups to share, or None when it
    * does not fit the task's budget and each group reads it again instead.
    *
    * A set whose every size the plan knew is decided before anything is read. A
    * set with an estimated segment is read until it either fits or goes over;
    * what was read by then is released and read again per group, which is the
    * cost of an unrecorded size, not a failure
    * (docs/design/architecture/search-resources.html section 3.3).
    */
  private def holdOrStream(
      plannedBytes: Option[Long],
      spec: Spec,
      segments: Seq[Long],
      open: Long => SegmentSearch.Source,
      metrics: SearchMetrics
  ): Option[SegmentSearch.Held] =
    plannedBytes match {
      case Some(bytes) if bytes > spec.keptMaxBytes =>
        logInfo(
          s"Segment set streamed: $bytes bytes over the ${spec.keptMaxBytes} " +
            s"a task keeps; every query group reads the ${segments.size} segments again"
        )
        None
      case _ =>
        SegmentSearch.hold(
          segments,
          open,
          spec.keptMaxBytes,
          spec.layout
        ) match {
          case Right(held) =>
            read(metrics, held.read)
            Some(held)
          case Left(overflow) =>
            read(metrics, overflow.read)
            None
        }
    }

  private def report(
      metrics: SearchMetrics,
      counters: SegmentSearch.Counters,
      candidates: Int
  ): Unit = {
    metrics.candidates.add(candidates.toLong)
    read(metrics, counters.read)
  }

  private def read(metrics: SearchMetrics, of: ReadMetrics): Unit = {
    metrics.readBytes.add(of.arrowBytes)
    metrics.readNanos.add(of.jniNanos)
  }

  private def searching[A](
      group: SearchQueries.Group,
      spec: Spec,
      allocator: BufferAllocator
  )(search: QueryMatrix => A): A = {
    val queries = QueryMatrix.ofPacked(
      group.vectors,
      group.firstQuery,
      group.queries,
      spec.layout,
      allocator
    )
    try search(queries)
    finally queries.close()
  }

  /** The candidates of one group, named by query id rather than by position in
    * the group.
    */
  private def candidates(merger: TopKMerger, ids: Array[Long]): Seq[Row] =
    merger.candidates.map(candidate =>
      Row(
        ids(candidate.query),
        candidate.segmentId,
        candidate.rowOffset,
        candidate.score
      )
    )

  /** What this segment offers the search: its vectors, or its index. */
  private def source(
      partition: MilvusInputPartition,
      spec: Spec,
      allocator: BufferAllocator,
      metrics: SearchMetrics
  ): SegmentSearch.Source = {
    val task = partition.task
    val binding = ColumnBinding(partition, StructType(Seq.empty))
    val collection = CollectionSchema.parseFrom(task.schemaBytes)
    val exclusions = RowExclusions.of(
      task,
      collection,
      spec.filter.map(PlanParser.parse),
      binding.columnNameFor
    )
    val selected =
      if (spec.mode != "index") None
      else
        SegmentIndexHandle.select(
          task,
          spec.fieldId,
          spec.metric,
          spec.allowUnindexed
        )
    selected match {
      case Some(descriptor) =>
        val started = System.nanoTime()
        val excluded = exclusions.bitmap(
          task,
          binding.arrowSchema,
          descriptor.rowCount,
          allocator,
          read(metrics, _)
        )
        metrics.bitmapNanos.add(System.nanoTime() - started)
        val handle = SegmentIndexHandle.open(
          task,
          descriptor,
          spec.layout,
          spec.nullable
        )
        metrics.indexBytes.add(handle.bytes)
        metrics.indexLoadNanos.add(handle.loadNanos)
        SegmentSearch.Index(task.segmentId, handle, excluded)
      case None =>
        // The block one brute-force call scans is chosen on this executor,
        // for this machine's caches and this executor's concurrency.
        val batch = exactScanBatch(spec)
        SegmentSearch.Exact(
          task.segmentId,
          SegmentVectors.open(
            task.copy(limits = task.limits.copy(batchMaxBytes = batch)),
            binding.arrowSchema,
            binding.columnNameFor,
            binding.arrowColumnFor(spec.vectorColumn),
            spec.layout,
            exclusions,
            allocator
          )
        )
    }
  }
}
