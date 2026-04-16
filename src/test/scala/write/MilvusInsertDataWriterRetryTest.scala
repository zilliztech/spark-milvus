package com.zilliz.spark.connector.write

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.{MilvusRateLimitException, MilvusRpcException}
import com.zilliz.spark.connector.write.MilvusInsertDataWriter.{
  decideRetry,
  Abort,
  Retry
}

import io.grpc.StatusRuntimeException

/** Unit tests for MilvusInsertDataWriter retry decision logic.
  *
  * Covers the review feedback on:
  *   - app-layer rate limit classification (MilvusRateLimitException)
  *   - transport-layer RESOURCE_EXHAUSTED path (gRPC failure surfacing as a
  *     generic exception, since the connector's GrpcRetryInterceptor marks
  *     RESOURCE_EXHAUSTED as non-retryable)
  *   - rateLimitDelay not being reset across alternating generic/rate-limit
  *     paths
  *   - independent interaction of the two counters (retries / rateLimitRetries)
  */
class MilvusInsertDataWriterRetryTest extends AnyFunSuite {

  private val retryInterval = 1000L
  private val initialDelay = 500L
  private val maxDelay = 10000L

  // ---------- app-layer rate limit classification ----------

  test("rate limit error consumes a rate-limit retry and doubles backoff") {
    val decision = decideRetry(
      error = new MilvusRateLimitException("app layer"),
      retries = 3,
      rateLimitRetries = 10,
      rateLimitDelay = initialDelay,
      retryInterval = retryInterval,
      maxRateLimitDelay = maxDelay
    )
    decision match {
      case Retry(3, 9, 500L, 1000L, true) => succeed
      case other                          => fail(s"unexpected: $other")
    }
  }

  test("exponential backoff progression caps at maxRateLimitDelay") {
    // 500 -> 1000 -> 2000 -> 4000 -> 8000 -> 10000 (capped) -> 10000
    val progression = List(500L, 1000L, 2000L, 4000L, 8000L, 10000L)
    val next = progression.tail :+ 10000L
    progression.zip(next).foreach { case (current, expected) =>
      decideRetry(
        error = new MilvusRateLimitException("app layer"),
        retries = 3,
        rateLimitRetries = 10,
        rateLimitDelay = current,
        retryInterval = retryInterval,
        maxRateLimitDelay = maxDelay
      ) match {
        case Retry(_, _, _, nextDelay, true) =>
          assert(
            nextDelay == expected,
            s"from $current expected $expected but got $nextDelay"
          )
        case other => fail(s"unexpected: $other")
      }
    }
  }

  test("rate limit with exhausted rateLimitRetries aborts") {
    decideRetry(
      error = new MilvusRateLimitException("app layer"),
      retries = 3,
      rateLimitRetries = 0,
      rateLimitDelay = 8000L,
      retryInterval = retryInterval
    ) match {
      case Abort(err: MilvusRpcException) =>
        assert(err.getMessage.contains("rate limit retries"))
      case other => fail(s"unexpected: $other")
    }
  }

  // ---------- transport-layer RESOURCE_EXHAUSTED path ----------

  test("transport-layer RESOURCE_EXHAUSTED is handled as a generic retry") {
    // The connector's GrpcRetryInterceptor marks RESOURCE_EXHAUSTED as
    // non-retryable at the transport layer, so it propagates up as a
    // StatusRuntimeException wrapped inside a non-MilvusRateLimitException.
    // decideRetry must NOT classify this as a rate-limit retry; it should
    // consume a generic retry slot.
    val transportError =
      new StatusRuntimeException(io.grpc.Status.RESOURCE_EXHAUSTED)
    decideRetry(
      error = transportError,
      retries = 3,
      rateLimitRetries = 10,
      rateLimitDelay = initialDelay,
      retryInterval = retryInterval
    ) match {
      case Retry(2, 10, 1000L, 500L, false) => succeed
      case other                            => fail(s"unexpected: $other")
    }
  }

  test("generic exception consumes a generic retry and sleeps retryInterval") {
    decideRetry(
      error = new RuntimeException("schema mismatch"),
      retries = 3,
      rateLimitRetries = 10,
      rateLimitDelay = initialDelay,
      retryInterval = retryInterval
    ) match {
      case Retry(2, 10, 1000L, 500L, false) => succeed
      case other                            => fail(s"unexpected: $other")
    }
  }

  test("generic exception with exhausted retries aborts") {
    decideRetry(
      error = new RuntimeException("schema mismatch"),
      retries = 0,
      rateLimitRetries = 10,
      rateLimitDelay = initialDelay,
      retryInterval = retryInterval
    ) match {
      case Abort(err: MilvusRpcException) =>
        assert(!err.getMessage.contains("rate limit retries"))
      case other => fail(s"unexpected: $other")
    }
  }

  // ---------- rateLimitDelay non-reset behavior ----------

  test("generic error preserves the escalated rateLimitDelay") {
    // Scenario: a rate-limit sequence has escalated delay to 4000ms, then
    // a generic error occurs. The next rate-limit hit should continue from
    // 4000ms (doubling to 8000ms), not fall back to the initial 500ms.
    decideRetry(
      error = new RuntimeException("transient"),
      retries = 3,
      rateLimitRetries = 8,
      rateLimitDelay = 4000L,
      retryInterval = retryInterval
    ) match {
      case Retry(_, _, _, nextRateLimitDelay, _) =>
        assert(
          nextRateLimitDelay == 4000L,
          "generic error must not reset rateLimitDelay"
        )
      case other => fail(s"unexpected: $other")
    }
  }

  test("alternating sequence preserves rate-limit delay escalation") {
    // RL(500) -> RL(1000) -> G -> G -> RL should double from 2000 to 4000
    val step1 = decideRetry(
      new MilvusRateLimitException("rl"),
      3,
      10,
      500L,
      retryInterval
    ).asInstanceOf[Retry]
    assert(step1.nextRateLimitDelay == 1000L)

    val step2 = decideRetry(
      new MilvusRateLimitException("rl"),
      step1.retries,
      step1.rateLimitRetries,
      step1.nextRateLimitDelay,
      retryInterval
    ).asInstanceOf[Retry]
    assert(step2.nextRateLimitDelay == 2000L)

    val step3 = decideRetry(
      new RuntimeException("g"),
      step2.retries,
      step2.rateLimitRetries,
      step2.nextRateLimitDelay,
      retryInterval
    ).asInstanceOf[Retry]
    assert(step3.nextRateLimitDelay == 2000L, "generic error preserves delay")

    val step4 = decideRetry(
      new RuntimeException("g"),
      step3.retries,
      step3.rateLimitRetries,
      step3.nextRateLimitDelay,
      retryInterval
    ).asInstanceOf[Retry]
    assert(step4.nextRateLimitDelay == 2000L)

    val step5 = decideRetry(
      new MilvusRateLimitException("rl"),
      step4.retries,
      step4.rateLimitRetries,
      step4.nextRateLimitDelay,
      retryInterval
    ).asInstanceOf[Retry]
    assert(
      step5.nextRateLimitDelay == 4000L,
      "rate-limit resumes doubling from escalated delay"
    )
  }

  // ---------- independent counter interactions ----------

  test("generic error does not consume a rate-limit retry") {
    decideRetry(
      error = new RuntimeException("g"),
      retries = 3,
      rateLimitRetries = 5,
      rateLimitDelay = initialDelay,
      retryInterval = retryInterval
    ) match {
      case Retry(2, 5, _, _, false) => succeed
      case other                    => fail(s"unexpected: $other")
    }
  }

  test("rate-limit error does not consume a generic retry") {
    decideRetry(
      error = new MilvusRateLimitException("rl"),
      retries = 3,
      rateLimitRetries = 5,
      rateLimitDelay = initialDelay,
      retryInterval = retryInterval
    ) match {
      case Retry(3, 4, _, _, true) => succeed
      case other                   => fail(s"unexpected: $other")
    }
  }

  test("exhausted generic retries still allow rate-limit retries") {
    // With retries=0 and rateLimitRetries=5, a rate-limit error should
    // still be retried (the two counters are independent).
    decideRetry(
      error = new MilvusRateLimitException("rl"),
      retries = 0,
      rateLimitRetries = 5,
      rateLimitDelay = initialDelay,
      retryInterval = retryInterval
    ) match {
      case Retry(0, 4, _, _, true) => succeed
      case other                   => fail(s"unexpected: $other")
    }
  }

  test("exhausted rate-limit retries still allow generic retries") {
    decideRetry(
      error = new RuntimeException("g"),
      retries = 3,
      rateLimitRetries = 0,
      rateLimitDelay = maxDelay,
      retryInterval = retryInterval
    ) match {
      case Retry(2, 0, _, _, false) => succeed
      case other                    => fail(s"unexpected: $other")
    }
  }
}
