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
import org.apache.spark.TaskContext

import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.milvus.storage.index.{QueryMatrix, SegmentSearch, TopKMerger}
import com.zilliz.milvus.storage.read.exec.{
  RowExclusions,
  SegmentIndexHandle,
  SegmentVectors
}
import com.zilliz.milvus.storage.read.exec.ReadMetrics
import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.CollectionSchema

/** One first-stage task: one segment set, every query group, the candidates
  * they produce.
  *
  * The task opens what the Milvus format side offers for each of its segments —
  * the vector batches of an exact scan, or the index handle of an index probe —
  * and the computation in `core.index` searches it. A task that answers more
  * than one query group keeps its segment set in memory and runs the groups on
  * it one after another, so the segments are read once whichever way the query
  * set was delivered (docs/design/architecture/vector-search.html section 2.1).
  */
private[read] object SegmentSetSearch extends Logging {

  /** What every task of one search needs and the driver already knows. */
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
      vectorsMaxBytes: Long,
      arrowMaxBytes: Long
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

  def run(
      set: Seq[MilvusInputPartition],
      spec: Spec,
      groups: Iterator[SearchQueries.Group],
      groupCount: Int,
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
      if (step.compared != 0L) metrics.compared.add(step.compared)
      if (step.segments != 0) metrics.segments.add(step.segments.toLong)
    }
    def open(segmentId: Long): SegmentSearch.Source =
      source(partitions(segmentId), spec, allocator.allocator, metrics)
    if (groupCount == 1) {
      try {
        val group = groups.next()
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
          s"Search task: segments=${counters.segments}, groups=1, " +
            s"queries=${group.queries}, candidates=${merger.size}"
        )
        candidates(merger, group).iterator
      } finally allocator.close()
    } else {
      val held = SegmentSearch.hold(
        segments,
        open,
        spec.vectorsMaxBytes,
        spec.layout
      )
      read(metrics, held.read)
      Option(TaskContext.get()).foreach(
        _.addTaskCompletionListener[Unit] { _ =>
          try held.close()
          finally allocator.close()
        }
      )
      groups.flatMap { group =>
        val (merger, counters) = searching(group, spec, allocator.allocator) {
          queries =>
            held.search(
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
            s"queries=${group.queries}, candidates=${merger.size}"
        )
        candidates(merger, group)
      }
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
  private def candidates(
      merger: TopKMerger,
      group: SearchQueries.Group
  ): Seq[Row] = merger.candidates.map(candidate =>
    Row(
      group.ids(candidate.query),
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
          spec.layout.dimension,
          spec.nullable
        )
        metrics.indexBytes.add(handle.bytes)
        metrics.indexLoadNanos.add(handle.loadNanos)
        SegmentSearch.Index(task.segmentId, handle, excluded)
      case None =>
        SegmentSearch.Exact(
          task.segmentId,
          SegmentVectors.open(
            task,
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
