package com.zilliz.spark.connector.read

import java.util.Arrays
import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{col, hash, lit, pmod}
import org.apache.spark.sql.types.StructType
import org.apache.spark.TaskContext

import com.zilliz.milvus.storage.read.exec.SegmentReaderRegistry
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.types.{ArrowAllocator, ArrowConverter}

/** The second stage: the output columns of the rows a search selected.
  *
  * Only the rows that survived the merge are read, one segment at a time and
  * each row once however many queries hit it. Reading them is the Milvus format
  * side taking rows by address (docs/design/architecture/vector-search.html
  * section 2.1).
  */
private[read] object SearchTake {

  /** What one buffered hit costs on the heap in `bySegment`: the copied
    * `UnsafeRow` of the five hit columns (48 bytes of row data, its object and
    * byte array headers) and its slot in the buffer. 100k queries at k=10,000
    * buffered 62.5 million hits per task in 16 tasks and ran an 8-core
    * executor's 6 GiB heap out of memory (2026-09-25 decision).
    */
  private[read] val HitRowBytes: Long = 128L

  /** How the hits are spread over the take tasks: `partitions` tasks, a
    * segment's hits cut into `buckets` slices by row offset so that a segment
    * with more hits than one task should buffer spans several tasks. Every
    * slice still reads its segment once and each row once.
    */
  final case class Partitioning(partitions: Int, buckets: Int) {
    require(partitions > 0 && buckets > 0, s"A take stage needs tasks: $this")
  }

  /** The partitioning that keeps a task's buffered hits inside
    * `heapBytesPerTask`, at half that to leave room for hash skew: never fewer
    * partitions than `minimum` (the session's shuffle partitions, so a small
    * search keeps its shape), never more than a bucket per hit.
    */
  def partitioning(
      hits: Long,
      segments: Int,
      heapBytesPerTask: Long,
      minimum: Int
  ): Partitioning = {
    require(hits >= 0L && segments > 0 && minimum > 0, s"$hits hits, $segments segments, $minimum minimum")
    val rowsPerTask =
      math.max(1L, math.max(0L, heapBytesPerTask) / HitRowBytes / 2L)
    val needed = (hits + rowsPerTask - 1L) / rowsPerTask
    val partitions =
      math.max(minimum.toLong, math.min(needed, Int.MaxValue.toLong)).toInt
    val buckets =
      math.max(1L, (partitions.toLong + segments - 1L) / segments).toInt
    Partitioning(partitions, buckets)
  }

  /** The hits of one search, with their output columns.
    *
    * The hits are shuffled by segment so that a task holds every hit of the
    * segments it reads, and then grouped by segment in the task rather than
    * sorted into groups. Sorting them was the expensive half of this stage: a
    * JFR profile of one P3 chunk put 10% of the executor's Java samples in
    * `UnsafeExternalRowSorter` for 25 million hit rows whose order this stage
    * then threw away, since `take` asks the reader for the distinct offsets in
    * ascending order and derives that order itself. What the sort did buy was a
    * bound on memory -- a task held one segment's hits at a time -- so the hits
    * are buffered as `InternalRow` instead of `Row`, which is the same 25
    * million rows in about half the bytes, and the number of tasks and the
    * buckets a segment is cut into come from [[partitioning]], sized so that
    * what one task buffers fits the heap it has.
    */
  def rows(
      hits: DataFrame,
      output: StructType,
      partitions: Map[Long, MilvusInputPartition],
      arrowMaxBytes: Long,
      metrics: SearchMetrics,
      partitioning: Partitioning
  ): RDD[Row] = {
    val hitSchema = hits.schema
    val segmentColumn = hitSchema.fieldIndex("_segment_id")
    val offsetColumn = hitSchema.fieldIndex("_row_offset")
    val known = hits.sparkSession.sparkContext.broadcast(partitions)
    // Hits of one segment land in one task unless the plan cut the segment
    // into buckets, in which case a bucket of its row offsets does; the
    // bucket is a repartitioning expression, not a column the result carries.
    val keys =
      if (partitioning.buckets == 1) Seq(col("_segment_id"))
      else
        Seq(
          col("_segment_id"),
          pmod(hash(col("_row_offset")), lit(partitioning.buckets))
        )
    hits
      .repartition(partitioning.partitions, keys: _*)
      .queryExecution
      .toRdd
      .mapPartitions { rows =>
        val allocator = ArrowAllocator.forSearchTask(
          TaskContext.get().partitionId(),
          arrowMaxBytes
        )
        Option(TaskContext.get())
          .foreach(_.addTaskCompletionListener[Unit](_ => allocator.close()))
        val toHit = CatalystTypeConverters.createToScalaConverter(hitSchema)
        bySegment(rows, segmentColumn).iterator.flatMap {
          case (segmentId, hitRows) =>
            val partition = known.value.getOrElse(
              segmentId,
              throw new IllegalStateException(
                s"Segment $segmentId is not in the plan this search was built from"
              )
            )
            take(
              partition,
              output,
              hitRows,
              offsetColumn,
              toHit,
              allocator.allocator,
              metrics
            )
        }
      }
  }

  /** The task's hits in one bucket per segment.
    *
    * Every row is copied, because the shuffle reader hands the same `UnsafeRow`
    * back with new bytes on each step.
    */
  private def bySegment(
      rows: Iterator[InternalRow],
      segmentColumn: Int
  ): mutable.LongMap[mutable.ArrayBuffer[InternalRow]] = {
    val groups = mutable.LongMap.empty[mutable.ArrayBuffer[InternalRow]]
    while (rows.hasNext) {
      val row = rows.next()
      groups
        .getOrElseUpdate(
          row.getLong(segmentColumn),
          mutable.ArrayBuffer.empty[InternalRow]
        )
        .addOne(row.copy())
    }
    groups
  }

  /** The distinct row offsets of these hits, ascending: what the reader takes.
    */
  private def offsetsOf(
      hits: mutable.ArrayBuffer[InternalRow],
      offsetColumn: Int
  ): Array[Long] = {
    val all = new Array[Long](hits.size)
    var at = 0
    while (at < hits.size) {
      all(at) = hits(at).getLong(offsetColumn)
      at += 1
    }
    Arrays.sort(all)
    var kept = 0
    at = 0
    while (at < all.length) {
      if (at == 0 || all(at) != all(at - 1)) {
        all(kept) = all(at)
        kept += 1
      }
      at += 1
    }
    if (kept == all.length) all else Arrays.copyOf(all, kept)
  }

  private def take(
      partition: MilvusInputPartition,
      output: StructType,
      hits: mutable.ArrayBuffer[InternalRow],
      offsetColumn: Int,
      toHit: Any => Any,
      allocator: org.apache.arrow.memory.BufferAllocator,
      metrics: SearchMetrics
  ): Iterator[Row] = {
    val binding = ColumnBinding(partition, output)
    val offsets = offsetsOf(hits, offsetColumn)
    val columns = output.fieldNames.toSeq.map(binding.arrowColumnFor)
    // A zero-column projection still needs row cardinality from one column.
    val projected = if (columns.nonEmpty) columns else Seq(binding.pkColumnName)
    val toScala = CatalystTypeConverters.createToScalaConverter(output)
    // Row i of this array is the row at offsets(i), which is the order the
    // reader returns them in, so a hit finds its row by a search over offsets
    // rather than through a map keyed by a boxed Long.
    val taken = new Array[Row](offsets.length)
    val started = System.nanoTime()
    val reader = SegmentReaderRegistry.open(
      partition.task,
      binding.arrowSchema,
      projected,
      binding.columnNameFor,
      allocator
    )
    try {
      val batches = reader.take(offsets, projected)
      var index = 0
      try {
        var next = batches.next()
        while (next.nonEmpty) {
          val batch = next.get
          try {
            var row = 0
            while (row < batch.getRowCount) {
              require(
                index < offsets.length,
                s"Segment ${partition.task.segmentId} returned more rows than the ${offsets.length} asked for"
              )
              taken(index) = toScala(
                ArrowConverter
                  .arrowToInternalRow(
                    batch,
                    row,
                    output,
                    binding.arrowColumnNames
                  )
                  .copy()
              ).asInstanceOf[Row]
              index += 1
              row += 1
            }
          } finally batch.close()
          next = batches.next()
        }
        require(
          index == offsets.length,
          s"Segment ${partition.task.segmentId} returned $index of the ${offsets.length} rows asked for"
        )
      } finally batches.close()
    } finally {
      try reader.close()
      finally {
        metrics.takeRows.add(offsets.length.toLong)
        metrics.takeNanos.add(System.nanoTime() - started)
        metrics.readBytes.add(reader.metrics.arrowBytes)
        metrics.readNanos.add(reader.metrics.jniNanos)
      }
    }
    hits.iterator.map { hit =>
      val at = Arrays.binarySearch(offsets, hit.getLong(offsetColumn))
      require(
        at >= 0,
        s"Segment ${partition.task.segmentId} has no row for offset ${hit.getLong(offsetColumn)}"
      )
      Row.merge(toHit(hit).asInstanceOf[Row], taken(at))
    }
  }
}
