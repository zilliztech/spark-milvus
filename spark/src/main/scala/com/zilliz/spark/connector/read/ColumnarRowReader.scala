package com.zilliz.spark.connector.read

import java.util.{Iterator => JavaIterator}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Exposes a batch reader as rows without repeating its conversion or
  * filtering. The underlying reader owns every batch and releases it on advance
  * or close.
  */
final class ColumnarRowReader(underlying: PartitionReader[ColumnarBatch])
    extends PartitionReader[InternalRow] {
  private var rows: JavaIterator[InternalRow] = null
  private var current: InternalRow = null
  private var closed = false

  override def next(): Boolean = {
    if (closed) return false
    current = null
    while (rows == null || !rows.hasNext) {
      if (!underlying.next()) return false
      rows = underlying.get().rowIterator()
    }
    current = rows.next()
    true
  }

  override def get(): InternalRow = {
    require(current != null, "No current row; call next() before get()")
    current
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] =
    underlying.currentMetricsValues()

  override def close(): Unit = {
    if (!closed) {
      closed = true
      current = null
      rows = null
      underlying.close()
    }
  }
}
