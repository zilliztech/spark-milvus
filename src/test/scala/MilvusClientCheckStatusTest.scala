package com.zilliz.spark.connector

import scala.util.{Failure, Success}

import org.scalatest.funsuite.AnyFunSuite

import io.milvus.grpc.common.{ErrorCode, Status}

import io.grpc.{Status => GrpcStatus, StatusRuntimeException}

/** Unit tests for MilvusClient.checkStatus classification logic.
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
    val ok = Status(
      code = 0,
      errorCode = ErrorCode.Success,
      reason = "rate limit exceeded"
    )
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
    val status =
      Status(code = 5, errorCode = ErrorCode.UnexpectedError, reason = null)
    client.checkStatus("insert", status) match {
      case Failure(e: MilvusRateLimitException) =>
        fail(s"unexpected rate-limit classification: $e")
      case Failure(_) => succeed
      case Success(_) => fail("expected Failure")
    }
  }

  test("non-rate-limit failure returns generic Exception") {
    val status = Status(
      code = 1,
      errorCode = ErrorCode.UnexpectedError,
      reason = "schema mismatch"
    )
    client.checkStatus("insert", status) match {
      case Failure(_: MilvusRateLimitException) =>
        fail("should not be classified as rate limit")
      case Failure(_) => succeed
      case Success(_) => fail("expected Failure")
    }
  }

  test("errorCode != Success with code 0 returns Failure") {
    // Defensive: code=0 but errorCode flags a real failure.
    val status = Status(
      code = 0,
      errorCode = ErrorCode.UnexpectedError,
      reason = "something broke"
    )
    assert(client.checkStatus("insert", status).isFailure)
  }

  test("classifies grpc UNIMPLEMENTED as service not implemented") {
    val err = new StatusRuntimeException(
      GrpcStatus.UNIMPLEMENTED.withDescription("unknown method CreateSnapshot")
    )
    assert(MilvusClient.isServiceNotImplemented(err))
  }

  test("classifies wrapped grpc UNIMPLEMENTED as service not implemented") {
    val cause = new StatusRuntimeException(
      GrpcStatus.UNIMPLEMENTED.withDescription("snapshot service disabled")
    )
    val err = new RuntimeException("wrapped", cause)
    assert(MilvusClient.isServiceNotImplemented(err))
  }

  test("does not classify non-grpc service-not-implemented text") {
    val err = new RuntimeException(
      "Failed to createSnapshot: service not implemented"
    )
    assert(!MilvusClient.isServiceNotImplemented(err))
  }

  test("does not classify ordinary errors as service not implemented") {
    val err = new RuntimeException("permission denied")
    assert(!MilvusClient.isServiceNotImplemented(err))
  }

  test("classifies grpc snapshot unknown method as service not implemented") {
    val err = new StatusRuntimeException(
      GrpcStatus.UNKNOWN.withDescription("unknown method CreateSnapshot")
    )
    assert(MilvusClient.isServiceNotImplemented(err))
  }

  test(
    "classifies grpc snapshot method-not-registered as service not implemented"
  ) {
    val err = new StatusRuntimeException(
      GrpcStatus.UNKNOWN.withDescription(
        "method not registered: DescribeSnapshot"
      )
    )
    assert(MilvusClient.isServiceNotImplemented(err))
  }

  test("does not classify grpc unknown method for unrelated RPC") {
    val err = new StatusRuntimeException(
      GrpcStatus.UNKNOWN.withDescription("unknown method SomeOtherMethod")
    )
    assert(!MilvusClient.isServiceNotImplemented(err))
  }

  test("does not classify grpc UNKNOWN with null description") {
    val err = new StatusRuntimeException(
      GrpcStatus.UNKNOWN.withDescription(null)
    )
    assert(!MilvusClient.isServiceNotImplemented(err))
  }

  test("classifies grpc NOT_FOUND as snapshot already dropped") {
    val err = new StatusRuntimeException(
      GrpcStatus.NOT_FOUND.withDescription("snapshot not found")
    )
    assert(MilvusClient.isSnapshotAlreadyDropped(err))
    assert(MilvusClient.isTerminalSnapshotDropError(err))
  }

  test("classifies wrapped grpc terminal snapshot drop errors") {
    Seq(GrpcStatus.PERMISSION_DENIED, GrpcStatus.INVALID_ARGUMENT).foreach {
      status =>
        val err = new RuntimeException(
          "wrapped",
          new StatusRuntimeException(status.withDescription("terminal"))
        )
        assert(!MilvusClient.isSnapshotAlreadyDropped(err))
        assert(MilvusClient.isTerminalSnapshotDropError(err))
    }
  }

  test("does not classify grpc UNAVAILABLE as terminal snapshot drop error") {
    val err = new StatusRuntimeException(
      GrpcStatus.UNAVAILABLE.withDescription("transient")
    )
    assert(!MilvusClient.isSnapshotAlreadyDropped(err))
    assert(!MilvusClient.isTerminalSnapshotDropError(err))
  }

  test("snapshot drop terminal detection stops on cyclic causes") {
    val err = new RuntimeException("ordinary error") {
      override def getCause: Throwable = this
    }
    assert(!MilvusClient.isSnapshotAlreadyDropped(err))
    assert(!MilvusClient.isTerminalSnapshotDropError(err))
  }

  test("service-not-implemented detection stops on cyclic causes") {
    val err = new RuntimeException("ordinary error") {
      override def getCause: Throwable = this
    }
    assert(!MilvusClient.isServiceNotImplemented(err))
  }
}
