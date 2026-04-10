package com.zilliz.spark.connector

import io.milvus.grpc.common.{ErrorCode, Status}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.{Failure, Success}

/**
 * Unit tests for MilvusClient.checkStatus classification logic.
 *
 * Covers the review feedback on ordering, NPE safety, success-path
 * misclassification, case-insensitive reason matching, and the named
 * rate-limit error code constant.
 */
class MilvusClientCheckStatusTest extends AnyFunSuite {

  private val client = new MilvusClient(
    MilvusConnectionParams(
      uri = "http://localhost:19530",
      token = "",
      databaseName = "default"
    )
  )

  test("success status returns Success regardless of reason content") {
    // A coincidental reason containing 'rate limit exceeded' must not flip a
    // success into a failure.
    val ok = Status(code = 0, errorCode = ErrorCode.Success, reason = "rate limit exceeded")
    assert(client.checkStatus("insert", ok).isInstanceOf[Success[_]])
  }

  test("success status with empty reason returns Success") {
    val ok = Status(code = 0, errorCode = ErrorCode.Success, reason = "")
    assert(client.checkStatus("insert", ok).isInstanceOf[Success[_]])
  }

  test("code == RateLimitErrorCode returns MilvusRateLimitException") {
    val status = Status(
      code = MilvusClient.RateLimitErrorCode,
      errorCode = ErrorCode.UnexpectedError,
      reason = "request is rejected by grpc RateLimiter middleware"
    )
    client.checkStatus("insert", status) match {
      case Failure(_: MilvusRateLimitException) => succeed
      case other => fail(s"expected MilvusRateLimitException, got $other")
    }
  }

  test("case-insensitive reason matching classifies as rate limit") {
    val status = Status(
      code = 99,
      errorCode = ErrorCode.UnexpectedError,
      reason = "Rate Limit Exceeded [rate=6.29e+06]"
    )
    client.checkStatus("insert", status) match {
      case Failure(_: MilvusRateLimitException) => succeed
      case other => fail(s"expected MilvusRateLimitException, got $other")
    }
  }

  test("null reason does not NPE and falls through to generic failure") {
    // scalapb proto case classes default reason to "", but defensively handle null.
    val status = Status(code = 5, errorCode = ErrorCode.UnexpectedError, reason = null)
    client.checkStatus("insert", status) match {
      case Failure(e: MilvusRateLimitException) => fail(s"unexpected rate-limit classification: $e")
      case Failure(_) => succeed
      case Success(_) => fail("expected Failure")
    }
  }

  test("non-rate-limit failure returns generic Exception") {
    val status = Status(code = 1, errorCode = ErrorCode.UnexpectedError, reason = "schema mismatch")
    client.checkStatus("insert", status) match {
      case Failure(_: MilvusRateLimitException) => fail("should not be classified as rate limit")
      case Failure(_) => succeed
      case Success(_) => fail("expected Failure")
    }
  }

  test("errorCode != Success with code 0 returns Failure") {
    // Defensive: code=0 but errorCode flags a real failure.
    val status = Status(code = 0, errorCode = ErrorCode.UnexpectedError, reason = "something broke")
    assert(client.checkStatus("insert", status).isFailure)
  }
}
