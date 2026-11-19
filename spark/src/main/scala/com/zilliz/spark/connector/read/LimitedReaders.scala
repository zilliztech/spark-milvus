package com.zilliz.spark.connector.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.zilliz.spark.connector.metrics.{ReadEndReason, ReadProgress}

/** Stops a row reader after `limit` rows.
  *
  * A pushed-down limit is per partition: Spark keeps its own Limit on top and
  * we report `isPartiallyPushed`, so every partition may deliver up to `limit`
  * rows and the global cut happens in Spark. What the pushdown saves is the
  * rest of every segment.
  *
  * Counting happens on rows the underlying reader actually returns, which is
  * after its own filtering and deletes, so a limit never lets a filtered row
  * through and never stops short because of one.
  */
final class LimitedRowReader(
    underlying: PartitionReader[InternalRow],
    limit: Int,
    progress: Option[ReadProgress] = None
) extends PartitionReader[InternalRow] {
  require(limit >= 0, s"limit must not be negative, got $limit")

  private var delivered = 0
  private var ready = false
  private var current: InternalRow = null
  private var closed = false

  override def next(): Boolean = {
    ready = false
    current = null
    if (closed) return false
    if (delivered >= limit) {
      progress.foreach(_.end(ReadEndReason.Limit))
      return false
    }
    val more = underlying.next()
    ready = more
    if (more) {
      delivered += 1
      if (delivered == limit) progress.foreach(_.end(ReadEndReason.Limit))
    }
    more
  }

  override def get(): InternalRow = {
    require(ready && !closed, "No current row; call next() before get()")
    if (current == null) current = underlying.get()
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

/** Stops a batch reader after `limit` rows, cutting the last batch short.
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
