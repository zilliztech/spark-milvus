package com.zilliz.spark.connector.search

import java.util.Arrays
import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{
  BoundReference,
  GenericInternalRow,
  JoinedRow,
  UnsafeProjection
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{
  coalesce,
  col,
  floor,
  hash,
  lit,
  pmod,
  try_element_at,
  typedLit
}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.TaskContext

import com.zilliz.milvus.storage.read.exec.SegmentReader
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.read.{ColumnBinding, MilvusInputPartition}
import com.zilliz.spark.connector.types.{ArrowAllocator, ArrowConverter}

/** The second stage: the output columns of the rows a search selected.
  *
  * Only the rows that survived the merge are read, one segment at a time and
  * each row once however many queries hit it. Reading them is the Milvus format
  * side taking rows by address (docs/design/architecture/vector-search.html
  * section 2.1).
  */
private[search] object SearchTake {

  /** What one buffered hit costs on the heap in `bySegment`: the copied
    * `UnsafeRow` and its slot in the buffer. The three address columns now use
    * 32 bytes of row data; this keeps the previous conservative estimate. 100k
    * queries at k=10,000 buffered 62.5 million hits per task in 16 tasks and
    * ran an 8-core executor's 6 GiB heap out of memory (2026-09-25 decision).
    */
  private[search] val HitRowBytes: Long = 128L

  /** How the hits are spread over the take tasks: `partitions` tasks, a
    * segment's hits cut into `buckets` slices of consecutive row offsets
    * ([[rangeBucket]]) so that a segment with more hits than one task should
    * buffer spans several tasks. The slices together read their segment once
    * and each row once.
    */
  final case class Partitioning(partitions: Int, buckets: Int) {
    require(partitions > 0 && buckets > 0, s"A take stage needs tasks: $this")
  }

  /** The bucket of a hit at `offset` in a segment of `rows` rows, cut into
    * `buckets` slices of consecutive rows: slice i holds the rows from i × rows
    * ÷ buckets on. A slice's hits sit in a run of the segment's row groups, so
    * a task's take reads that run and the buckets together read the segment
    * once. Hashing the offset instead spread every task over every row group of
    * its segment: at k=100,000 each of 109 bucket tasks read the whole segment
    * again, 31 TB in all against 8 GB of primary keys
    * (docs/design/enhance/2026-10-13-search-take-stage.html section 5).
    */
  def rangeBucket(offset: Long, rows: Long, buckets: Int): Int = {
    require(rows > 0 && buckets > 0, s"$rows rows, $buckets buckets")
    math.min(buckets - 1, math.max(0L, offset * buckets / rows)).toInt
  }

  /** [[rangeBucket]] as a column of the hits, per segment by `rowsBySegment`; a
    * hit of a segment whose row count the snapshot did not record falls back to
    * the hash of its offset, which only costs that segment the repeated reads.
    */
  private[search] def bucketColumn(
      buckets: Int,
      rowsBySegment: Map[Long, Long]
  ): Column = {
    val hashed = pmod(hash(col("_row_offset")), lit(buckets))
    if (rowsBySegment.isEmpty) hashed
    else {
      val rows = try_element_at(typedLit(rowsBySegment), col("_segment_id"))
      val ranged = floor(col("_row_offset") * lit(buckets.toLong) / rows)
      coalesce(ranged, hashed).cast(IntegerType)
    }
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
    * buckets a segment is cut into come from
    * [[SearchExecutionPlan.takePartitioning]], sized so that what one task
    * buffers fits the heap it has.
    *
    * Each query id comes out with its requested base columns in an owned row.
    * `positions` selects them from the hit columns followed by `output`,
    * without materializing that combined row. A hit whose segment is NULL -- a
    * query a LEFT OUTER nearest-by join keeps without a hit -- reads nothing
    * and comes out with NULL output columns.
    */
  def rows(
      hits: DataFrame,
      output: StructType,
      positions: Seq[Int],
      partitions: Map[Long, MilvusInputPartition],
      arrowMaxBytes: Long,
      metrics: SearchMetrics,
      partitioning: Partitioning
  ): RDD[(Long, InternalRow)] = {
    val hitSchema = hits.schema
    val segmentColumn = hitSchema.fieldIndex("_segment_id")
    val offsetColumn = hitSchema.fieldIndex("_row_offset")
    val known = hits.sparkSession.sparkContext.broadcast(partitions)
    val resultSchema = StructType(hitSchema.fields ++ output.fields)
    // Hits of one segment land in one task unless the plan cut the segment
    // into buckets, in which case a slice of its row offsets does; the
    // bucket is a repartitioning expression, not a column the result carries.
    val keys =
      if (partitioning.buckets == 1) Seq(col("_segment_id"))
      else
        Seq(
          col("_segment_id"),
          bucketColumn(
            partitioning.buckets,
            partitions.flatMap { case (id, partition) =>
              partition.task.expectedRows.filter(_ > 0L).map(id -> _)
            }
          )
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
        val joined = new JoinedRow()
        val (withoutHit, bySegment) = SearchTake.bySegment(rows, segmentColumn)
        val nothing = new GenericInternalRow(output.size)
        val taken = withoutHit.iterator.map(hit => joined(hit, nothing)) ++
          bySegment.iterator.flatMap { case (segmentId, hitRows) =>
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
              allocator.allocator,
              metrics
            ).map { case (hit, taken) => joined(hit, taken) }
          }
        project(taken, resultSchema, positions)
      }
  }

  /** Select requested base columns by position, owning the result before an
    * upstream iterator reuses its row or a downstream join retains it.
    */
  def project(
      rows: Iterator[InternalRow],
      schema: StructType,
      positions: Seq[Int]
  ): Iterator[(Long, InternalRow)] = {
    val columns = positions.map(at =>
      // Outer hits may be NULL even where the storage field is non-nullable.
      BoundReference(at, schema(at).dataType, nullable = true)
    )
    val projection = UnsafeProjection.create(columns)
    rows.map(row => row.getLong(0) -> projection(row).copy())
  }

  /** The task's hits in one bucket per segment, and apart from them the hits
    * that have no segment.
    *
    * Every row is copied, because the shuffle reader hands the same `UnsafeRow`
    * back with new bytes on each step.
    */
  private def bySegment(
      rows: Iterator[InternalRow],
      segmentColumn: Int
  ): (
      mutable.ArrayBuffer[InternalRow],
      mutable.LongMap[mutable.ArrayBuffer[InternalRow]]
  ) = {
    val withoutHit = mutable.ArrayBuffer.empty[InternalRow]
    val groups = mutable.LongMap.empty[mutable.ArrayBuffer[InternalRow]]
    while (rows.hasNext) {
      val row = rows.next()
      if (row.isNullAt(segmentColumn)) withoutHit += row.copy()
      else
        groups
          .getOrElseUpdate(
            row.getLong(segmentColumn),
            mutable.ArrayBuffer.empty[InternalRow]
          ) += row.copy()
    }
    (withoutHit, groups)
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

  /** Each hit of one segment with the output columns of its row. */
  private def take(
      partition: MilvusInputPartition,
      output: StructType,
      hits: mutable.ArrayBuffer[InternalRow],
      offsetColumn: Int,
      allocator: org.apache.arrow.memory.BufferAllocator,
      metrics: SearchMetrics
  ): Iterator[(InternalRow, InternalRow)] = {
    val binding = ColumnBinding(partition, output)
    val offsets = offsetsOf(hits, offsetColumn)
    val columns = output.fieldNames.toSeq.map(binding.arrowColumnFor)
    // A zero-column projection still needs row cardinality from one column.
    val projected = if (columns.nonEmpty) columns else Seq(binding.pkColumnName)
    // Row i of this array is the row at offsets(i), which is the order the
    // reader returns them in, so a hit finds its row by a search over offsets
    // rather than through a map keyed by a boxed Long.
    val taken = new Array[InternalRow](offsets.length)
    val started = System.nanoTime()
    val reader = SegmentReader.open(
      partition.task,
      binding.arrowSchema,
      projected,
      binding.columnNameFor,
      allocator
    )
    try {
      val batches = reader.take(offsets, projected)
      try
        copyRows(
          batches,
          taken,
          output,
          binding.arrowColumnNames,
          partition.task.segmentId
        )
      finally batches.close()
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
      hit -> taken(at)
    }
  }

  /** Every row of `batches` copied into `taken`, in the order they come, each
    * batch closed once its rows are out: exactly as many rows as `taken` holds.
    */
  private def copyRows(
      batches: SegmentReader.TakeResult,
      taken: Array[InternalRow],
      output: StructType,
      columnNames: Map[String, String],
      segmentId: Long
  ): Unit = {
    var index = 0
    var next = batches.next()
    while (next.nonEmpty) {
      val batch = next.get
      try {
        var row = 0
        while (row < batch.getRowCount) {
          require(
            index < taken.length,
            s"Segment $segmentId returned more rows than the ${taken.length} asked for"
          )
          taken(index) =
            ArrowConverter.arrowToInternalRow(batch, row, output, columnNames)
          index += 1
          row += 1
        }
      } finally batch.close()
      next = batches.next()
    }
    require(
      index == taken.length,
      s"Segment $segmentId returned $index of the ${taken.length} rows asked for"
    )
  }
}
