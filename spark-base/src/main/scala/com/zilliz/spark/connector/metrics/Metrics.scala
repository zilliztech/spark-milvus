package com.zilliz.spark.connector.metrics

import org.apache.spark.sql.connector.metric.{
  CustomMetric,
  CustomSumMetric,
  CustomTaskMetric
}

import com.zilliz.milvus.storage.read.exec.ReadMetrics
import com.zilliz.milvus.storage.write.exec.WriteMetrics

/** A metric Spark sums over tasks. */
final class SumMetric(metricName: String, metricDescription: String)
    extends CustomSumMetric {
  override def name(): String = metricName
  override def description(): String = metricDescription
}

/** A metric Spark takes the maximum of over tasks, shown in bytes. */
final class MaxBytesMetric(metricName: String, metricDescription: String)
    extends CustomMetric {
  override def name(): String = metricName
  override def description(): String = metricDescription
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    val max = if (taskMetrics.isEmpty) 0L else taskMetrics.max
    MaxBytesMetric.format(max)
  }
}

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
    new SumMetric(JniCalls, "JNI calls"),
    new SumMetric(JniNanos, "time in JNI calls (ns)"),
    new SumMetric(ArrowBatches, "Arrow batches handed over"),
    new SumMetric(ArrowBytes, "Arrow bytes handed over"),
    new SumMetric(Copies, "columns copied in C (sliced batches)"),
    new SumMetric(CopiedBytes, "bytes those copies produced"),
    new SumMetric(RowsMaterialized, "rows turned into InternalRow"),
    new MaxBytesMetric(ArrowAllocatedMax, "Arrow allocator peak (bytes)")
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
  val JniCalls = "milvus.jni.calls"
  val JniNanos = "milvus.jni.nanos"
  val ArrowBatches = "milvus.arrow.batches"
  val ArrowBytes = "milvus.arrow.bytes"
  val ArrowAllocatedMax = "milvus.arrow.allocated.max"

  def supported: Array[CustomMetric] = Array(
    new SumMetric(JniCalls, "JNI calls"),
    new SumMetric(JniNanos, "time in JNI calls (ns)"),
    new SumMetric(ArrowBatches, "Arrow batches handed over"),
    new SumMetric(ArrowBytes, "Arrow bytes handed over"),
    new MaxBytesMetric(ArrowAllocatedMax, "Arrow allocator peak (bytes)")
  )

  def taskValues(metrics: WriteMetrics): Array[CustomTaskMetric] = Array(
    new TaskMetric(JniCalls, metrics.jniCalls),
    new TaskMetric(JniNanos, metrics.jniNanos),
    new TaskMetric(ArrowBatches, metrics.batches),
    new TaskMetric(ArrowBytes, metrics.arrowBytes),
    new TaskMetric(ArrowAllocatedMax, metrics.allocatedMax)
  )
}
