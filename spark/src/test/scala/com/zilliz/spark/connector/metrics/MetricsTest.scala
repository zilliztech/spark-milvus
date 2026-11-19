package com.zilliz.spark.connector.metrics

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.read.exec.ReadMetrics

class MetricsTest extends AnyFunSuite {

  test("driver and task values cover every declared read metric") {
    val declared = ScanMetrics.supported.map(_.name()).toSet
    val reported =
      ScanMetrics.taskValues(ReadMetrics.Zero) ++
        new ReadProgress().taskValues ++ ReadOperationalMetrics.planned(0L)
    assert(reported.map(_.name()).toSet == declared)
    assert(declared.size == ScanMetrics.supported.length)
  }

  test("Spark can re-create every declared metric from its class name") {
    // The driver's SQL listener aggregates task values through a new instance
    // made from the metric's class name, with the no-argument constructor.
    ScanMetrics.supported.foreach { m =>
      val copy = m.getClass.getConstructor().newInstance()
      assert(copy.name() == m.name())
      assert(copy.description() == m.description())
    }
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
        )
      )
      .map(m => m.name() -> m.value())
      .toMap
    assert(values(ScanMetrics.JniCalls) == 7)
    assert(values(ScanMetrics.ArrowBytes) == 4096)
    assert(values(ScanMetrics.Copies) == 1)
    assert(values(ScanMetrics.ArrowAllocatedMax) == 65536)
  }

  test("the allocator peak aggregates by max and reads in bytes") {
    val metric = new ArrowAllocatedMaxMetric
    assert(
      metric.aggregateTaskMetrics(Array(10L, 3L << 20, 2L << 20)) == "3.0 MiB"
    )
    assert(metric.aggregateTaskMetrics(Array.empty[Long]) == "0 B")
    assert(MaxBytesMetric.format(1536) == "1.5 KiB")
  }

  test("a sum metric adds the tasks up") {
    assert(
      new JniCallsMetric().aggregateTaskMetrics(Array(1L, 2L, 3L)) == "6"
    )
  }

  test(
    "the operational bridge accepts read counters without SQL-only state metrics"
  ) {
    val token = OperationToken("read-metrics-test", "read", Some("1"))
    val observation = new TaskObservation(
      None,
      Some(
        TaskIdentity(
          token,
          1L,
          0,
          0,
          "executor",
          "reader",
          Some("segment-1")
        )
      ),
      "reading"
    )
    val progress = new ReadProgress(Some(observation))
    progress.decodedRows(8L)
    progress.emittedRows(3L)
    progress.excludedRows(2L, 1L)
    progress.end(ReadEndReason.Limit)
    progress.close()
    progress.close()
    assert(progress.values(ReadOperationalMetrics.CompletedSegments) == 1L)
    assert(
      progress.values(ReadOperationalMetrics.endName(ReadEndReason.Limit)) == 1L
    )
  }
}
