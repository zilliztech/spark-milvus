package com.zilliz.spark.connector.scan

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch

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
    limit: Int
) extends PartitionReader[InternalRow] {
  require(limit >= 0, s"limit must not be negative, got $limit")

  private var delivered = 0

  override def next(): Boolean = {
    if (delivered >= limit) return false
    val more = underlying.next()
    if (more) delivered += 1
    more
  }

  override def get(): InternalRow = underlying.get()

  override def close(): Unit = underlying.close()
}

/** Stops a batch reader after `limit` rows, cutting the last batch short.
  *
  * `ColumnarBatch.setNumRows` narrows the window a consumer sees without
  * touching the columns underneath, so the last batch is trimmed in place and
  * nothing is copied.
  */
final class LimitedBatchReader(
    underlying: PartitionReader[ColumnarBatch],
    limit: Int
) extends PartitionReader[ColumnarBatch] {
  require(limit >= 0, s"limit must not be negative, got $limit")

  private var remaining = limit
  private var current: ColumnarBatch = null

  override def next(): Boolean = {
    if (remaining <= 0) return false
    if (!underlying.next()) return false
    current = underlying.get()
    val rows = current.numRows()
    if (rows > remaining) current.setNumRows(remaining)
    remaining -= current.numRows()
    true
  }

  override def get(): ColumnarBatch = current

  override def close(): Unit = underlying.close()
}
