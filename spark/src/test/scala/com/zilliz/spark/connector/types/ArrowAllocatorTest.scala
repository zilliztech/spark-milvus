package com.zilliz.spark.connector.types

import org.apache.arrow.memory.OutOfMemoryException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ArrowAllocatorTest extends AnyFunSuite with Matchers {

  test("a task child enforces its limit and closes idempotently") {
    val child = ArrowAllocator.forReadTask(7L, 64L)
    try {
      intercept[OutOfMemoryException] {
        child.allocator.buffer(128L)
      }
    } finally {
      child.close()
      child.close()
    }
    child.isClosed shouldBe true

    // Closing a task scope must not close the process-wide allocator.
    val rootBuffer = ArrowAllocator.get.buffer(8L)
    rootBuffer.close()
  }
}
