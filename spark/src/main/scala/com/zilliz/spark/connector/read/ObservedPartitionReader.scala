package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext

import com.zilliz.spark.connector.metrics.{
  ReadEndReason,
  ReadFailurePhase,
  ReadOperationalMetrics,
  ReadProgress
}

/** The outermost read boundary counts the values handed to Spark, after a
  * partition limit may have shortened the final batch. Repeated get() calls
  * return the same value and never count it again.
  */
private[connector] final class ObservedPartitionReader(
    underlying: PartitionReader[ColumnarBatch],
    progress: ReadProgress
) extends PartitionReader[ColumnarBatch] {
  private var current: Option[ColumnarBatch] = None
  private var ready = false
  private var exhausted = false
  private var closed = false

  // Register after the observation has been created. Spark runs listeners in
  // reverse registration order, so resource cleanup and terminal counters are
  // ready before the observation's fallback listener sends its final snapshot.
  Option(TaskContext.get())
    .foreach(_.addTaskCompletionListener[Unit](_ => close()))

  override def next(): Boolean = {
    if (closed || exhausted) return false
    current = None
    ready = false
    val more = progress.inPhase(ReadFailurePhase.Read)(underlying.next())
    ready = more
    if (!more) {
      exhausted = true
      progress.end(ReadEndReason.Exhausted)
    }
    more
  }

  override def get(): ColumnarBatch = {
    require(ready && !closed, "No current value; call next() before get()")
    current match {
      case Some(value) => value
      case None =>
        val value = progress.inPhase(ReadFailurePhase.Convert)(underlying.get())
        current = Some(value)
        progress.emittedRows(value.numRows().toLong)
        value
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    // Spark samples once more before closing readers at task completion.
    // That sample must distinguish an early consumer stop from physical EOF.
    Option(TaskContext.get()).filter(_.isCompleted()).foreach { context =>
      progress.end(
        if (context.isInterrupted()) ReadEndReason.Cancelled
        else ReadEndReason.ConsumerStop
      )
    }
    ReadOperationalMetrics.withTaskValues(
      underlying.currentMetricsValues(),
      progress
    )
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      ready = false
      current = None
      try progress.inPhase(ReadFailurePhase.Close)(underlying.close())
      finally progress.close()
    }
  }
}
