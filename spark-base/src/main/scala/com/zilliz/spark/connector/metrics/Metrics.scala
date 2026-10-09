package com.zilliz.spark.connector.metrics

import org.apache.spark.sql.connector.metric.{
  CustomMetric,
  CustomSumMetric,
  CustomTaskMetric
}

import com.zilliz.milvus.storage.read.exec.ReadMetrics
import com.zilliz.milvus.storage.write.exec.WriteMetrics

/** A metric Spark sums over tasks.
  *
  * Each declared metric is its own class with a no-argument constructor: the
  * driver's SQL listener aggregates task values through an instance it makes
  * from the class name the plan recorded.
  */
abstract class SumMetric(metricName: String, metricDescription: String)
    extends CustomSumMetric {
  override def name(): String = metricName
  override def description(): String = metricDescription
}

/** A metric Spark takes the maximum of over tasks, shown in bytes. */
abstract class MaxBytesMetric(metricName: String, metricDescription: String)
    extends CustomMetric {
  override def name(): String = metricName
  override def description(): String = metricDescription
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    val max = if (taskMetrics.isEmpty) 0L else taskMetrics.max
    MaxBytesMetric.format(max)
  }
}

final class JniCallsMetric extends SumMetric(ScanMetrics.JniCalls, "JNI calls")
final class JniNanosMetric
    extends SumMetric(ScanMetrics.JniNanos, "time in JNI calls (ns)")
final class ArrowBatchesMetric
    extends SumMetric(ScanMetrics.ArrowBatches, "Arrow batches handed over")
final class ArrowBytesMetric
    extends SumMetric(ScanMetrics.ArrowBytes, "Arrow bytes handed over")
final class CopiesMetric
    extends SumMetric(
      ScanMetrics.Copies,
      "columns copied in C (sliced batches)"
    )
final class CopiedBytesMetric
    extends SumMetric(ScanMetrics.CopiedBytes, "bytes those copies produced")
final class RowsMaterializedMetric
    extends SumMetric(
      ScanMetrics.RowsMaterialized,
      "rows turned into InternalRow"
    )
final class ArrowAllocatedMaxMetric
    extends MaxBytesMetric(
      ScanMetrics.ArrowAllocatedMax,
      "Arrow allocator peak (bytes)"
    )

object MaxBytesMetric {
  def format(bytes: Long): String = {
    val units = Array("B", "KiB", "MiB", "GiB", "TiB")
    var value = bytes.toDouble
    var unit = 0
    while (value >= 1024.0 && unit < units.length - 1) {
      value /= 1024.0
      unit += 1
    }
    if (unit == 0) s"$bytes B" else f"$value%.1f ${units(unit)}"
  }
}

final class TaskMetric(metricName: String, metricValue: Long)
    extends CustomTaskMetric {
  override def name(): String = metricName
  override def value(): Long = metricValue
}

/** The metrics a scan declares and the readers report (capability G5): what
  * `core.read.exec` counted on the crossing plus the rows the read layer turned
  * into `InternalRow`.
  */
object ScanMetrics {
  val JniCalls = "milvus.jni.calls"
  val JniNanos = "milvus.jni.nanos"
  val ArrowBatches = "milvus.arrow.batches"
  val ArrowBytes = "milvus.arrow.bytes"
  val Copies = "milvus.copies"
  val CopiedBytes = "milvus.copied.bytes"
  val RowsMaterialized = "milvus.rows.materialized"
  val ArrowAllocatedMax = "milvus.arrow.allocated.max"

  def supported: Array[CustomMetric] = Array(
    new JniCallsMetric,
    new JniNanosMetric,
    new ArrowBatchesMetric,
    new ArrowBytesMetric,
    new CopiesMetric,
    new CopiedBytesMetric,
    new RowsMaterializedMetric,
    new ArrowAllocatedMaxMetric
  )

  def taskValues(
      metrics: ReadMetrics,
      rowsMaterialized: Long
  ): Array[CustomTaskMetric] = Array(
    new TaskMetric(JniCalls, metrics.jniCalls),
    new TaskMetric(JniNanos, metrics.jniNanos),
    new TaskMetric(ArrowBatches, metrics.batches),
    new TaskMetric(ArrowBytes, metrics.arrowBytes),
    new TaskMetric(Copies, metrics.copies),
    new TaskMetric(CopiedBytes, metrics.copiedBytes),
    new TaskMetric(RowsMaterialized, rowsMaterialized),
    new TaskMetric(ArrowAllocatedMax, metrics.allocatedMax)
  )
}

/** The metrics a write declares and its data writers report (capability G5):
  * what `core.write.exec` counted on the crossing.
  */
object WriteMetricsReport {
  val JniCalls: String = ScanMetrics.JniCalls
  val JniNanos: String = ScanMetrics.JniNanos
  val ArrowBatches: String = ScanMetrics.ArrowBatches
  val ArrowBytes: String = ScanMetrics.ArrowBytes
  val ArrowAllocatedMax: String = ScanMetrics.ArrowAllocatedMax

  def supported: Array[CustomMetric] = Array(
    new JniCallsMetric,
    new JniNanosMetric,
    new ArrowBatchesMetric,
    new ArrowBytesMetric,
    new ArrowAllocatedMaxMetric
  )

  def taskValues(metrics: WriteMetrics): Array[CustomTaskMetric] = Array(
    new TaskMetric(JniCalls, metrics.jniCalls),
    new TaskMetric(JniNanos, metrics.jniNanos),
    new TaskMetric(ArrowBatches, metrics.batches),
    new TaskMetric(ArrowBytes, metrics.arrowBytes),
    new TaskMetric(ArrowAllocatedMax, metrics.allocatedMax)
  )
}
