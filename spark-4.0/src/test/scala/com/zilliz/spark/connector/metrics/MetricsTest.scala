package com.zilliz.spark.connector.metrics

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.read.exec.ReadMetrics
import com.zilliz.milvus.storage.write.exec.WriteMetrics

class MetricsTest extends AnyFunSuite {

  test("every task metric a reader reports is one the scan declared") {
    val declared = ScanMetrics.supported.map(_.name()).toSet
    val reported =
      ScanMetrics.taskValues(ReadMetrics.Zero, rowsMaterialized = 0L)
    assert(reported.map(_.name()).toSet == declared)
    assert(declared.size == ScanMetrics.supported.length)
  }

  test("every task metric a writer reports is one the write declared") {
    val declared = WriteMetricsReport.supported.map(_.name()).toSet
    val reported = WriteMetricsReport.taskValues(WriteMetrics.Zero)
    assert(reported.map(_.name()).toSet == declared)
  }

  test("task values carry the counted numbers") {
    val values = ScanMetrics
      .taskValues(
        ReadMetrics(
          jniCalls = 7,
          jniNanos = 8,
          batches = 3,
          arrowBytes = 4096,
          copies = 1,
          copiedBytes = 512,
          allocatedMax = 65536
        ),
        rowsMaterialized = 30
      )
      .map(m => m.name() -> m.value())
      .toMap
    assert(values(ScanMetrics.JniCalls) == 7)
    assert(values(ScanMetrics.ArrowBytes) == 4096)
    assert(values(ScanMetrics.Copies) == 1)
    assert(values(ScanMetrics.RowsMaterialized) == 30)
    assert(values(ScanMetrics.ArrowAllocatedMax) == 65536)
  }

  test("the allocator peak aggregates by max and reads in bytes") {
    val metric = new MaxBytesMetric("m", "d")
    assert(
      metric.aggregateTaskMetrics(Array(10L, 3L << 20, 2L << 20)) == "3.0 MiB"
    )
    assert(metric.aggregateTaskMetrics(Array.empty[Long]) == "0 B")
    assert(MaxBytesMetric.format(1536) == "1.5 KiB")
  }

  test("a sum metric adds the tasks up") {
    assert(
      new SumMetric("m", "d").aggregateTaskMetrics(Array(1L, 2L, 3L)) == "6"
    )
  }
}
