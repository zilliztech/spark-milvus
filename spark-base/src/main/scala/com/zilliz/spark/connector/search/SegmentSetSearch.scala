package com.zilliz.spark.connector.search

import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.TaskContext

import com.zilliz.milvus.storage.expr.{FilterExpr, PredicateExpr}
import com.zilliz.milvus.storage.read.exec.{
  RowExclusions,
  SegmentIndexHandle,
  SegmentVectors
}
import com.zilliz.milvus.storage.schema.{MetricType, VectorLayout}
import com.zilliz.milvus.storage.vector.{
  QueryMatrix,
  RankingFunction,
  SearchThreads,
  SegmentSearch,
  TopKMerger,
  VectorBatch
}
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.read.{ColumnBinding, MilvusInputPartition}
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.CollectionSchema

/** One first-stage task: one segment set, every query group of its range, the
  * candidates they produce.
  *
  * The task opens what the Milvus format side offers for each of its segments —
  * vector batches for exact scan, or an index handle for indexed search — and
  * the computation in `core.vector` searches it. The plan chose which side the
  * task keeps (docs/design/architecture/vector-search.html section 2.1).
  * Keeping its queries, the task takes every group of its range first and reads
  * each segment once, one at a time, searching every group on it; that is also
  * what a task with one group does. Keeping its segments, it reads the set into
  * memory and the groups arrive one after another. Either way the segments are
  * read once, whichever way the query set was delivered; a kept set larger than
  * the task's budget is read again for every group instead
  * (search-resources.html section 3.3).
  */
private[search] object SegmentSetSearch extends Logging {

  /** The search semantics and Arrow read limit shared by every task. Runtime
    * execution choices arrive separately in [[SearchExecutionPlan.Task]].
    */
  final case class Request(
      vectorColumn: String,
      layout: VectorLayout,
      fieldId: Long,
      nullable: Boolean,
      k: Int,
      metric: MetricType,
      mode: SearchMode,
      filter: Option[FilterExpr],
      parameters: Map[String, String],
      arrowMaxBytes: Long,
      // What Spark pushed into the scan the search reads; it excludes rows
      // before the top k, with SQL three-valued logic.
      predicate: Option[PredicateExpr] = None,
      // The function a nearest-by join ranks by, for the pairs Knowhere does
      // not score as it does (docs/design/architecture/dataframe-api.html
      // section 2); without one Knowhere scores every pair.
      function: Option[RankingFunction] = None
  ) extends Serializable

  /** Executes the chosen traversal. Retention attempts still account actual
    * loaded bytes and fall back to streaming when they exceed the capacity.
    */
  def run(
      set: Seq[MilvusInputPartition],
      request: Request,
      execution: SearchExecutionPlan.Task,
      groups: Iterator[SearchQueries.Group],
      groupCount: Int,
      metrics: SearchMetrics
  ): Iterator[(Long, Array[Byte])] = {
    require(set.nonEmpty, "A first-stage task has no segments")
    require(groupCount > 0, s"A task answers $groupCount query groups")
    val segments = set.map(_.task.segmentId)
    val partitions =
      set.map(partition => partition.task.segmentId -> partition).toMap
    val allocator = ArrowAllocator.forSearchTask(
      TaskContext.get().partitionId(),
      request.arrowMaxBytes
    )
    def open(segmentId: Long): SegmentSearch.Source =
      source(
        partitions(segmentId),
        request,
        allocator.allocator,
        metrics,
        execution.batchMaxBytes
      )
    if (execution.retention == SearchExecutionPlan.Retention.Queries) {
      try
        keepingQueries(
          segments,
          open,
          groups,
          groupCount,
          request,
          allocator.allocator,
          metrics,
          execution
        )
      finally allocator.close()
    } else {
      val held = holdOrStream(execution, request, segments, open, metrics)
      Option(TaskContext.get()).foreach(
        _.addTaskCompletionListener[Unit] { _ =>
          try held.foreach(_.close())
          finally allocator.close()
        }
      )
      // A group searches the set held in memory or, when it did not fit,
      // reads every segment again as it goes.
      val search: QueryMatrix => (TopKMerger, SegmentSearch.Counters) =
        held match {
          case Some(kept) =>
            kept.search(
              _,
              request.k,
              request.metric,
              request.parameters,
              allocator.allocator,
              metrics.record,
              execution.collectThreads
            )
          case None =>
            SegmentSearch.run(
              segments,
              open,
              _,
              request.k,
              request.metric,
              request.parameters,
              allocator.allocator,
              metrics.record,
              request.function
            )
        }
      val how = if (held.isEmpty) "streamed" else "held"
      groups.flatMap { group =>
        val (merger, counters) =
          searching(group, request, allocator.allocator)(search)
        report(metrics, counters, merger.size)
        logInfo(
          s"Search task: segments=${counters.segments}, groups=$groupCount, " +
            s"queries=${group.queries}, candidates=${merger.size}, $how"
        )
        candidates(merger, group.ids, execution.collectThreads)
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
      request: Request,
      allocator: BufferAllocator,
      metrics: SearchMetrics,
      execution: SearchExecutionPlan.Task
  ): Iterator[(Long, Array[Byte])] = {
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
          request.layout,
          allocator
        )
        taken += 1
      }
      require(
        taken == groupCount,
        s"A task answering $groupCount query groups was given $taken"
      )
    } catch {
      case failure: Throwable =>
        matrices.iterator.filter(_ != null).foreach(_.close())
        throw failure
    }
    // The matrices are searchKept's from here, failure included.
    searchKept(
      segments,
      open,
      ids,
      matrices,
      request,
      allocator,
      metrics,
      execution
    )
  }

  /** The search of a task that keeps its queries, once every group is a matrix:
    * every segment once, then the candidates group by group. Owns the matrices
    * from here on and closes them.
    */
  private def searchKept(
      segments: Seq[Long],
      open: Long => SegmentSearch.Source,
      ids: Array[Array[Long]],
      matrices: Array[QueryMatrix],
      request: Request,
      allocator: BufferAllocator,
      metrics: SearchMetrics,
      execution: SearchExecutionPlan.Task
  ): Iterator[(Long, Array[Byte])] = {
    val groupCount = matrices.length
    try {
      val (mergers, counters) = SegmentSearch.runGroups(
        segments,
        open,
        matrices.toSeq,
        request.k,
        request.metric,
        request.parameters,
        allocator,
        metrics.record,
        execution.prefetchIndexes,
        execution.collectThreads,
        request.function
      )
      val kept = mergers.toArray
      report(metrics, counters, kept.iterator.map(_.size).sum)
      logInfo(
        s"Search task: segments=${counters.segments}, groups=$groupCount, " +
          s"queries=${ids.iterator.map(_.length).sum}, " +
          s"candidates=${kept.iterator.map(_.size).sum}, queries kept"
      )
      Iterator.range(0, groupCount).flatMap { index =>
        val rows = candidates(kept(index), ids(index), execution.collectThreads)
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
      execution: SearchExecutionPlan.Task,
      request: Request,
      segments: Seq[Long],
      open: Long => SegmentSearch.Source,
      metrics: SearchMetrics
  ): Option[SegmentSearch.Held] =
    execution.retention match {
      case SearchExecutionPlan.Retention.Stream(bytes) =>
        logInfo(
          s"Segment set streamed: $bytes bytes over the ${execution.keptMaxBytes} " +
            s"a task keeps; every query group reads the ${segments.size} segments again"
        )
        None
      case _ =>
        SegmentSearch.hold(
          segments,
          open,
          execution.keptMaxBytes,
          request.layout,
          request.metric,
          request.function,
          execution.callsInFlight
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

  private def read(metrics: SearchMetrics, of: VectorBatch.ReadCost): Unit = {
    metrics.readBytes.add(of.bytes)
    metrics.readNanos.add(of.nanos)
  }

  private def searching[A](
      group: SearchQueries.Group,
      request: Request,
      allocator: BufferAllocator
  )(search: QueryMatrix => A): A = {
    val queries = QueryMatrix.ofPacked(
      group.vectors,
      group.firstQuery,
      group.queries,
      request.layout,
      allocator
    )
    try search(queries)
    finally queries.close()
  }

  /** The candidates of one group: one record for each query that found
    * anything, named by query id rather than by its position in the group, and
    * holding that query's best k packed into bytes.
    *
    * One record per query rather than one per candidate is what keeps this
    * stage's output at the `queries * k * CandidateBytes.Width` the plan counts
    * on, and what lets the merge stage walk two answers instead of aggregating
    * candidate by candidate (docs/design/architecture/vector-search.html
    * section 2.1). The merger retains its primitive columns alongside the
    * packed bytes until the caller drops the group; SearchPlan budgets for both
    * payloads.
    */
  private def candidates(
      merger: TopKMerger,
      ids: Array[Long],
      threads: Int
  ): Seq[(Long, Array[Byte])] = {
    require(threads > 0, s"Packing takes at least one thread: $threads")
    val bytes = new Array[Array[Byte]](merger.queries)
    // Sort and encode each query in the same shard, after collection ends.
    SearchThreads.shards(merger.queries, threads) { (_, from, until) =>
      var query = from
      while (query < until) {
        merger.sort(query)
        bytes(query) = merger.takePacked(query)
        query += 1
      }
    }
    val packed = Vector.newBuilder[(Long, Array[Byte])]
    var query = 0
    while (query < ids.length) {
      if (bytes(query).length > 0) packed += ids(query) -> bytes(query)
      query += 1
    }
    packed.result()
  }

  /** What this segment offers the search: its vectors, or its index. */
  private def source(
      partition: MilvusInputPartition,
      request: Request,
      allocator: BufferAllocator,
      metrics: SearchMetrics,
      batchMaxBytes: Long
  ): SegmentSearch.Source = {
    val task = partition.task
    val binding = ColumnBinding(partition, StructType(Seq.empty))
    val collection = CollectionSchema.parseFrom(task.schemaBytes)
    val exclusions = RowExclusions.of(
      task,
      collection,
      request.filter,
      binding.columnNameFor,
      request.predicate
    )
    val selected = request.mode match {
      case SearchMode.PreferIndex =>
        SegmentIndexHandle.select(task, request.fieldId, request.metric)
      case SearchMode.Exact => None
    }
    selected match {
      case Some(descriptor) =>
        val started = System.nanoTime()
        val excluded = exclusions.bitmap(
          task,
          binding.arrowSchema,
          descriptor.rowCount,
          allocator,
          cost =>
            read(metrics, VectorBatch.ReadCost(cost.arrowBytes, cost.jniNanos))
        )
        metrics.bitmapNanos.add(System.nanoTime() - started)
        val handle = SegmentIndexHandle.open(
          task,
          descriptor,
          request.layout,
          request.nullable
        )
        metrics.indexBytes.add(handle.bytes)
        metrics.indexLoadNanos.add(handle.loadNanos)
        SegmentSearch.Index(task.segmentId, handle, excluded)
      case None =>
        // The block one exact-scan call takes: the option when the call set
        // it, otherwise 32 MiB (search-resources.html section 3.2).
        SegmentSearch.Exact(
          task.segmentId,
          SegmentVectors.open(
            task.copy(limits = task.limits.copy(batchMaxBytes = batchMaxBytes)),
            binding.arrowSchema,
            binding.columnNameFor,
            binding.arrowColumnFor(request.vectorColumn),
            request.layout,
            exclusions,
            allocator
          )
        )
    }
  }
}
