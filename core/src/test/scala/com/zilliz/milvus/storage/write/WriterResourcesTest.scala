package com.zilliz.milvus.storage.write

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.write.WriterResources

final class WriterResourcesTest extends AnyFunSuite {
  test(
    "cleanup attempts every resource and propagates the first close failure"
  ) {
    val closed = ArrayBuffer.empty[String]
    val nativeFailure = new IllegalStateException("native close")
    val arrowFailure = new IllegalStateException("Arrow close")
    val failure = intercept[IllegalStateException] {
      WriterResources.closeAll(
        null,
        () => { closed += "native"; throw nativeFailure },
        () => { closed += "Arrow"; throw arrowFailure },
        () => { closed += "allocator"; () }
      )
    }
    assert(failure eq nativeFailure)
    assert(failure.getSuppressed.toSeq == Seq(arrowFailure))
    assert(closed.toSeq == Seq("native", "Arrow", "allocator"))
  }

  test(
    "an export or initialization failure remains primary when releasing also fails"
  ) {
    val primary = new IllegalStateException("export")
    val release = new IllegalStateException("release callback")
    val free = new IllegalStateException("C struct close")
    val failure = intercept[IllegalStateException] {
      WriterResources.closing(
        WriterResources.closeAll(null, () => throw release, () => throw free)
      ) {
        throw primary
      }
    }
    assert(failure eq primary)
    assert(failure.getSuppressed.toSeq == Seq(release))
    assert(release.getSuppressed.toSeq == Seq(free))
  }

  test(
    "a produced native result is closed if releasing its producer prevents handoff"
  ) {
    val release = new IllegalStateException("writer destroy")
    val resultClose = new IllegalStateException("column groups destroy")
    var resultClosed = false
    val result = new AutoCloseable {
      override def close(): Unit = { resultClosed = true; throw resultClose }
    }
    val failure = intercept[IllegalStateException] {
      WriterResources.handoff(throw release)(result)
    }
    assert(failure eq release)
    assert(resultClosed)
    assert(failure.getSuppressed.toSeq == Seq(resultClose))
  }

  test("successful handoff leaves the result owned by the caller") {
    var producerClosed = false
    var resultClosed = false
    val result = new AutoCloseable {
      override def close(): Unit = resultClosed = true
    }
    assert(WriterResources.handoff { producerClosed = true }(result) eq result)
    assert(producerClosed && !resultClosed)
    result.close()
    assert(resultClosed)
  }
}
