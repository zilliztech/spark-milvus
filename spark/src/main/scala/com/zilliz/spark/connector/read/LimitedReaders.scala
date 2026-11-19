package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.zilliz.spark.connector.metrics.{ReadEndReason, ReadProgress}

/** Stops a batch reader after `limit` rows, cutting the last batch short.
  *
  * Spark keeps the global limit. Each partition counts only the rows left after
  * deletes and predicates have been applied.
  *
  * `ColumnarBatch.setNumRows` narrows the window a consumer sees without
  * touching the columns underneath, so the last batch is trimmed in place and
  * nothing is copied.
  */
final class LimitedBatchReader(
    underlying: PartitionReader[ColumnarBatch],
    limit: Int,
    progress: Option[ReadProgress] = None
) extends PartitionReader[ColumnarBatch] {
  require(limit >= 0, s"limit must not be negative, got $limit")

  private var remaining = limit
  private var current: ColumnarBatch = null
  private var closed = false

  override def next(): Boolean = {
    current = null
    if (closed) return false
    if (remaining <= 0) {
      progress.foreach(_.end(ReadEndReason.Limit))
      return false
    }
    if (!underlying.next()) return false
    current = underlying.get()
    val rows = current.numRows()
    if (rows > remaining) current.setNumRows(remaining)
    remaining -= current.numRows()
    if (remaining == 0) progress.foreach(_.end(ReadEndReason.Limit))
    true
  }

  override def get(): ColumnarBatch = {
    require(
      current != null && !closed,
      "No current batch; call next() before get()"
    )
    current
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      current = null
      underlying.close()
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] =
    underlying.currentMetricsValues()
}
