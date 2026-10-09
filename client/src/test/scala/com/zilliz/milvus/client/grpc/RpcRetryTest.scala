package com.zilliz.milvus.client.grpc

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.grpc.Status

class RpcRetryTest extends AnyFunSuite with Matchers {

  /** A clock that moves only when the retry sleeps or an attempt takes time. */
  private final class Clock {
    var nanos = 0L
    val sleeps = ArrayBuffer.empty[Long]
    def now(): Long = nanos
    def sleep(millis: Long): Unit = {
      sleeps += millis
      nanos += millis * 1000000L
    }
  }

  private val unavailable = Status.UNAVAILABLE.asRuntimeException()

  test("a retryable failure is tried again with a growing wait") {
    val clock = new Clock
    var attempts = 0
    val result =
      RpcRetry(maxAttempts = 5).run(10000L, () => clock.now(), clock.sleep) {
        _ =>
          attempts += 1
          if (attempts < 3) throw unavailable else "ok"
      }(RpcRetry.isUnavailable)
    result shouldBe Success("ok")
    clock.sleeps.toSeq shouldBe Seq(500L, 1000L)
  }

  test("each attempt is given the time left, and no wait runs past it") {
    val clock = new Clock
    val given = ArrayBuffer.empty[Long]
    val result =
      RpcRetry(maxAttempts = 10).run(1600L, () => clock.now(), clock.sleep) {
        left =>
          given += left
          clock.nanos += 50L * 1000000L
          throw unavailable
      }(RpcRetry.isUnavailable)
    result shouldBe a[Failure[_]]
    // 1600 left; after 50 + 500 ms, 1050 left; after another 50, 1000 left,
    // and the next wait of 1000 would end at the deadline.
    given.toSeq shouldBe Seq(1600L, 1050L)
    clock.sleeps.toSeq shouldBe Seq(500L)
  }

  test("a result that is not retryable is returned at once") {
    val clock = new Clock
    var attempts = 0
    val denied = Status.PERMISSION_DENIED.asRuntimeException()
    RpcRetry().run(10000L, () => clock.now(), clock.sleep) { _ =>
      attempts += 1
      throw denied
    }(RpcRetry.isUnavailable) shouldBe Failure(denied)
    attempts shouldBe 1
    clock.sleeps shouldBe empty
  }

  test("the attempt count bounds the retries") {
    val clock = new Clock
    var attempts = 0
    RpcRetry(maxAttempts = 3, maxDelayMillis = 600L)
      .run(100000L, () => clock.now(), clock.sleep) { _ =>
        attempts += 1
        throw unavailable
      }(RpcRetry.isUnavailable) shouldBe a[Failure[_]]
    attempts shouldBe 3
    clock.sleeps.toSeq shouldBe Seq(500L, 600L)
  }

  test("an interrupted wait returns the last result and keeps the flag") {
    var attempts = 0
    val result = RpcRetry().run(
      10000L,
      () => 0L,
      _ => throw new InterruptedException()
    ) { _ =>
      attempts += 1
      throw unavailable
    }(RpcRetry.isUnavailable)
    try {
      result shouldBe Failure(unavailable)
      attempts shouldBe 1
      Thread.currentThread().isInterrupted shouldBe true
    } finally Thread.interrupted()
  }

  test("only gRPC UNAVAILABLE counts as unavailable") {
    RpcRetry.isUnavailable(Failure(unavailable)) shouldBe true
    RpcRetry.isUnavailable(
      Failure(Status.DEADLINE_EXCEEDED.asRuntimeException())
    ) shouldBe false
    RpcRetry.isUnavailable(Try("ok")) shouldBe false
  }
}
