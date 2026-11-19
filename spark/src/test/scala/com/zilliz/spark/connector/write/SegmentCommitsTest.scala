package com.zilliz.spark.connector.write

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite

class SegmentCommitsTest extends AnyFunSuite {
  private val plans = (1L to 3L).map { id =>
    PlannedCommit(id, 1L, s"segment/$id", 1L, 1L, Seq.empty)
  }

  private def committed(plan: PlannedCommit): SegmentOutcome =
    SegmentOutcome(
      plan.segmentId,
      plan.sourceVersion,
      SegmentState.Committed,
      Some(2L),
      None
    )

  test(
    "the first failed segment stops queued commits, including after an acknowledged commit's cleanup fails"
  ) {
    val first = committed(plans.head)
    val failures = Seq(
      first.copy(
        state = SegmentState.Unknown,
        version = None,
        error = Some("no acknowledgement")
      ),
      first.copy(error = Some("could not close transaction"))
    )
    failures.foreach { failure =>
      val entered = mutable.ArrayBuffer.empty[Long]
      val reported = mutable.Map.empty[Long, SegmentOutcome]
      SegmentCommits.commitAll(
        plans,
        outcome => reported.update(outcome.segmentId, outcome),
        parallelism = 1
      ) { plan =>
        entered += plan.segmentId
        failure
      }
      assert(entered.toSeq == Seq(first.segmentId))
      assert(reported(first.segmentId) == failure)
      plans.tail.foreach { plan =>
        assert(reported(plan.segmentId).state == SegmentState.NotAttempted)
      }
    }
  }

  test("driver interruption before dispatch leaves every segment unsubmitted") {
    val reported = mutable.Map.empty[Long, SegmentOutcome]
    Thread.currentThread().interrupt()
    try {
      intercept[InterruptedException] {
        SegmentCommits.commitAll(
          plans,
          outcome => reported.update(outcome.segmentId, outcome)
        ) { _ =>
          fail("a worker started after the driver was interrupted")
        }
      }
    } finally Thread.interrupted()
    assert(reported.keySet == plans.map(_.segmentId).toSet)
    assert(reported.values.forall(_.state == SegmentState.NotAttempted))
  }

  test(
    "driver interruption skips queued segments and leaves an in-flight transaction undisturbed"
  ) {
    val inStorage = new CountDownLatch(1)
    val releaseStorage = new CountDownLatch(1)
    val storageThread = new AtomicReference[Thread]()
    val storageInterrupted = new AtomicBoolean(false)
    val entered = mutable.ArrayBuffer.empty[Long]
    val reported = mutable.Map.empty[Long, SegmentOutcome]
    val interrupted = new AtomicReference[Throwable]()
    val driver = new Thread(() => {
      try {
        SegmentCommits.commitAll(
          plans,
          outcome => reported.update(outcome.segmentId, outcome),
          parallelism = 1
        ) { plan =>
          entered += plan.segmentId
          if (plan == plans(1)) {
            storageThread.set(Thread.currentThread())
            inStorage.countDown()
            try releaseStorage.await()
            catch {
              case interrupted: InterruptedException =>
                storageInterrupted.set(true)
                throw interrupted
            }
          }
          committed(plan)
        }
      } catch {
        case failure: Throwable => interrupted.set(failure)
      }
    })
    driver.start()
    try {
      assert(inStorage.await(10, TimeUnit.SECONDS))
      driver.interrupt()
      driver.join(10000)
      assert(!driver.isAlive)
      assert(interrupted.get().isInstanceOf[InterruptedException])
      assert(reported(1L) == committed(plans.head))
      // The driver's initial unknown remains for the transaction still in storage.
      assert(!reported.contains(2L))
      assert(reported(3L).state == SegmentState.NotAttempted)
      assert(storageThread.get().isAlive)
    } finally {
      releaseStorage.countDown()
      driver.interrupt()
      driver.join(10000)
      Option(storageThread.get()).foreach(_.join(10000))
    }
    assert(!storageThread.get().isAlive)
    assert(!storageInterrupted.get())
    assert(entered.toSeq == Seq(1L, 2L))
    // Returning from storage after abort began cannot overwrite its outcome.
    assert(!reported.contains(2L))
  }
}
