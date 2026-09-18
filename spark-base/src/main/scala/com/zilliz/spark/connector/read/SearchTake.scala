package com.zilliz.spark.connector.read

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.functions.col
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

  def rows(
      hits: DataFrame,
      output: StructType,
      partitions: Map[Long, MilvusInputPartition],
      arrowMaxBytes: Long,
      metrics: SearchMetrics
  ): RDD[Row] = {
    val hitSchema = hits.schema
    val segmentColumn = hitSchema.fieldIndex("_segment_id")
    val offsetColumn = hitSchema.fieldIndex("_row_offset")
    val known = hits.sparkSession.sparkContext.broadcast(partitions)
    hits
      .repartition(col("_segment_id"))
      .sortWithinPartitions(col("_segment_id"), col("_row_offset"))
      .rdd
      .mapPartitions { rows =>
        val allocator = ArrowAllocator.forSearchTask(
          TaskContext.get().partitionId(),
          arrowMaxBytes
        )
        Option(TaskContext.get())
          .foreach(_.addTaskCompletionListener[Unit](_ => allocator.close()))
        bySegment(rows, segmentColumn).flatMap { case (segmentId, hitRows) =>
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
          )
        }
      }
  }

  /** The rows of one segment, which sorting has already put together. */
  private def bySegment(
      rows: Iterator[Row],
      segmentColumn: Int
  ): Iterator[(Long, Seq[Row])] = new Iterator[(Long, Seq[Row])] {
    private val pending: BufferedIterator[Row] = rows.buffered

    override def hasNext: Boolean = pending.hasNext

    override def next(): (Long, Seq[Row]) = {
      val segmentId = pending.head.getLong(segmentColumn)
      val group = mutable.ArrayBuffer.empty[Row]
      while (
        pending.hasNext && pending.head.getLong(segmentColumn) == segmentId
      ) group += pending.next()
      (segmentId, group.toSeq)
    }
  }

  private def take(
      partition: MilvusInputPartition,
      output: StructType,
      hits: Seq[Row],
      offsetColumn: Int,
      allocator: org.apache.arrow.memory.BufferAllocator,
      metrics: SearchMetrics
  ): Iterator[Row] = {
    val binding = ColumnBinding(partition, output)
    val offsets = hits.map(_.getLong(offsetColumn)).distinct.sorted.toArray
    val columns = output.fieldNames.toSeq.map(binding.arrowColumnFor)
    // A zero-column projection still needs row cardinality from one column.
    val projected = if (columns.nonEmpty) columns else Seq(binding.pkColumnName)
    val toScala = CatalystTypeConverters.createToScalaConverter(output)
    val byOffset = mutable.Map.empty[Long, Row]
    val started = System.nanoTime()
    val reader = SegmentReaderRegistry.open(
      partition.task,
      binding.arrowSchema,
      projected,
      binding.columnNameFor,
      allocator
    )
    try {
      val taken = reader.take(offsets, projected)
      var index = 0
      try {
        var next = taken.next()
        while (next.nonEmpty) {
          val batch = next.get
          try {
            var row = 0
            while (row < batch.getRowCount) {
              require(
                index < offsets.length,
                s"Segment ${partition.task.segmentId} returned more rows than the ${offsets.length} asked for"
              )
              byOffset(offsets(index)) = toScala(
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
          next = taken.next()
        }
        require(
          index == offsets.length,
          s"Segment ${partition.task.segmentId} returned $index of the ${offsets.length} rows asked for"
        )
      } finally taken.close()
    } finally {
      try reader.close()
      finally {
        metrics.takeRows.add(offsets.length.toLong)
        metrics.takeNanos.add(System.nanoTime() - started)
        metrics.readBytes.add(reader.metrics.arrowBytes)
        metrics.readNanos.add(reader.metrics.jniNanos)
      }
    }
    hits.iterator.map(hit =>
      Row.merge(hit, byOffset(hit.getLong(offsetColumn)))
    )
  }
}
