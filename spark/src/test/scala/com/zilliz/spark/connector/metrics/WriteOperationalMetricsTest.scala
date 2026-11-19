package com.zilliz.spark.connector.metrics

import java.io.IOException
import java.util.concurrent.{Callable, Executors}

import org.scalatest.funsuite.AnyFunSuite

class WriteOperationalMetricsTest extends AnyFunSuite {
  private val expected = Map(30L -> 4L, 31L -> 6L)

  test("accepted attempts do not become adopted files or committed rows") {
    val adopted = new WriteTaskMetrics
    val speculative = new WriteTaskMetrics
    (1 to 10).foreach { _ => adopted.rowAccepted(); speculative.rowAccepted() }
    val driver = new WriteOperationalMetrics(expected)
    assert(adopted.values(WriteOperationalMetrics.AcceptedRows) == 10L)
    assert(speculative.values(WriteOperationalMetrics.AcceptedRows) == 10L)
    assert(driver.values(WriteOperationalMetrics.ReadyRows) == 0L)
    assert(driver.values(WriteOperationalMetrics.CommittedRows) == 0L)
    driver.filesReady(10L, 4L)
    driver.filesReady(10L, 4L)
    assert(driver.values(WriteOperationalMetrics.ReadyRows) == 10L)
    assert(driver.values(WriteOperationalMetrics.ReadyFiles) == 4L)
    assert(driver.values(WriteOperationalMetrics.CommittedRows) == 0L)
    intercept[IllegalArgumentException](driver.filesReady(20L, 8L))
  }

  test(
    "partial and unknown manifest outcomes never imply receipt publication"
  ) {
    val driver = new WriteOperationalMetrics(expected)
    driver.enter(WritePhase.Committing)
    driver.segmentOutcome(30L, "committed")
    driver.segmentOutcome(31L, "unknown")
    driver.fail(new IOException("manifest outcome could not be inspected"))
    val values = driver.values
    assert(values(WriteOperationalMetrics.CommittedRows) == 4L)
    assert(values(WriteOperationalMetrics.CommittedSegments) == 1L)
    assert(values(WriteOperationalMetrics.UnknownSegments) == 1L)
    assert(values(WriteOperationalMetrics.ReceiptPublished) == 0L)
    assert(values(WriteOperationalMetrics.Phase) == WritePhase.Failed)
    assert(
      values(WriteOperationalMetrics.FailurePhase) == WritePhase.Committing
    )
    intercept[IllegalArgumentException](driver.published())
  }

  test(
    "all manifests can be committed while receipt publication is unconfirmed"
  ) {
    val driver = new WriteOperationalMetrics(expected)
    expected.keys.foreach(id => driver.segmentOutcome(id, "committed"))
    driver.enter(WritePhase.Publishing)
    driver.fail(
      new IOException("publication response lost and readback failed")
    )
    driver.taskFailed()
    driver.cleanupFailed()
    val values = driver.values
    assert(values(WriteOperationalMetrics.CommittedRows) == 10L)
    assert(values(WriteOperationalMetrics.ReceiptPublished) == 0L)
    assert(
      values(WriteOperationalMetrics.FailurePhase) == WritePhase.Publishing
    )
    assert(
      values(
        WriteOperationalMetrics.FailureCategory
      ) == WriteFailureCategory.PublicationUnconfirmed
    )
    assert(values(WriteOperationalMetrics.CleanupFailures) == 1L)
  }

  test("confirmed receipt remains successful when cleanup fails") {
    val driver = new WriteOperationalMetrics(expected)
    expected.keys.foreach(id => driver.segmentOutcome(id, "committed"))
    driver.published()
    driver.cleanupFailed()
    driver.taskFailed()
    assert(driver.values(WriteOperationalMetrics.Phase) == WritePhase.Succeeded)
    assert(driver.values(WriteOperationalMetrics.ReceiptPublished) == 1L)
    assert(driver.values(WriteOperationalMetrics.CleanupFailures) == 1L)
    assert(
      driver.values(
        WriteOperationalMetrics.FailureCategory
      ) == WriteFailureCategory.None
    )
  }

  test("concurrent and repeated outcomes count each segment once") {
    val segments = (1L to 100L).map(id => id -> id).toMap
    val driver = new WriteOperationalMetrics(segments)
    val pool = Executors.newFixedThreadPool(4)
    try {
      val callbacks = (1 to 3).flatMap(_ => segments.keys).map { id =>
        pool.submit(new Callable[Unit] {
          override def call(): Unit = driver.segmentOutcome(id, "committed")
        })
      }
      callbacks.foreach(_.get())
      assert(
        driver.values(
          WriteOperationalMetrics.CommittedRows
        ) == segments.values.sum
      )
      assert(driver.values(WriteOperationalMetrics.CommittedSegments) == 100L)
    } finally pool.shutdownNow()
  }

  test("failure and cleanup do not advance the last confirmed progress time") {
    var now = 100000L
    val driver = new WriteOperationalMetrics(expected, () => now)
    driver.enter(WritePhase.Writing)
    now = 110000L
    driver.filesReady(10L, 4L)
    now = 200000L
    driver.fail(new IllegalArgumentException("missing input"))
    driver.cleanupFailed()
    assert(driver.values(WriteOperationalMetrics.LastProgress) == 110L)
  }

  test("all reported SQL metrics are declared and can be recreated by Spark") {
    val driver = new WriteOperationalMetrics(expected)
    val task = new WriteTaskMetrics
    val reported = (driver.driverValues ++ task.taskValues).map(_.name()).toSet
    val supported = WriteOperationalMetrics.supported
    assert(reported == supported.map(_.name()).toSet)
    assert(reported.size == supported.length)
    supported.foreach { metric =>
      val recreated = metric.getClass.getConstructor().newInstance()
      assert(recreated.name() == metric.name())
    }
  }
}
