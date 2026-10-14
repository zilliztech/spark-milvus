package com.zilliz.spark.connector.procedure

import org.scalatest.funsuite.AnyFunSuite

class ProcedureWaitTest extends AnyFunSuite {

  test("wait options reject ignored and invalid timeouts") {
    val ignored = ProcedureArgs(
      Map("collection" -> "c", "timeout_seconds" -> 1L),
      Map.empty
    )
    assertThrows[IllegalArgumentException](
      ProcedureWait.options(ignored, "test")
    )

    val invalid = ProcedureArgs(
      Map(
        "collection" -> "c",
        "wait" -> true,
        "timeout_seconds" -> 0L
      ),
      Map.empty
    )
    assertThrows[IllegalArgumentException](
      ProcedureWait.options(invalid, "test")
    )
  }

  test("bounded polling returns a terminal value and reports a timeout") {
    var probes = 0
    var now = 0L
    var budgets = Vector.empty[Long]
    val done = ProcedureWait.await(
      "test operation",
      timeoutSeconds = 1L,
      pollMillis = 1L,
      nanoTime = () => now,
      sleep = millis => now += millis * 1000000L
    )(remainingMillis => {
      budgets :+= remainingMillis
      probes += 1
      if (probes == 1) "loading" else "done"
    }) {
      case "done" => WaitDecision.Done
      case _      => WaitDecision.Continue
    }
    assert(done == "done")
    assert(budgets == Vector(1000L, 999L))

    var timeoutNow = 0L
    val timeout = intercept[IllegalStateException](
      ProcedureWait.await(
        "slow operation",
        timeoutSeconds = 1L,
        pollMillis = 1L,
        nanoTime = () => {
          val current = timeoutNow
          timeoutNow += 1000000000L
          current
        },
        sleep = _ => ()
      )(_ => "loading")(_ => WaitDecision.Continue)
    )
    assert(timeout.getMessage.contains("did not complete"))
    assert(timeout.getMessage.contains("loading"))

    var lateNow = 0L
    val lateSuccess = intercept[IllegalStateException](
      ProcedureWait.await(
        "late operation",
        timeoutSeconds = 1L,
        pollMillis = 1L,
        nanoTime = () => {
          val current = lateNow
          lateNow += 1000000000L
          current
        },
        sleep = _ => ()
      )(_ => "done")(_ => WaitDecision.Done)
    )
    assert(lateSuccess.getMessage.contains("did not complete"))
  }

  test("bounded polling preserves interruption while sleeping") {
    try {
      val interrupted = intercept[InterruptedException] {
        ProcedureWait.await(
          "interrupted operation",
          timeoutSeconds = 1L,
          pollMillis = 1L,
          nanoTime = () => 0L,
          sleep = _ => throw new InterruptedException("cancelled")
        )(_ => "loading")(_ => WaitDecision.Continue)
      }
      assert(interrupted.getMessage == "cancelled")
      assert(Thread.currentThread().isInterrupted)
    } finally {
      Thread.interrupted()
    }
  }
}
