package com.zilliz.milvus.storage.index

import java.nio.ByteOrder
import java.util.BitSet

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The compaction a batch with excluded rows goes through before the batched
  * distance entry, which takes no bitmap
  * (docs/design/architecture/vector-search.html section 2.3).
  */
class ExactScanTest extends AnyFunSuite with Matchers {

  private val rowBytes = 8

  private def withAllocator(body: RootAllocator => Unit): Unit = {
    val allocator = new RootAllocator()
    try body(allocator)
    finally allocator.close()
  }

  /** Row `i` holds the two floats (i, -i). */
  private def rows(allocator: RootAllocator, count: Int) = {
    val buffer = allocator.buffer(count.toLong * rowBytes)
    (0 until count).foreach { row =>
      buffer.setFloat(row.toLong * rowBytes, row.toFloat)
      buffer.setFloat(row.toLong * rowBytes + 4L, -row.toFloat)
    }
    buffer
  }

  private def excluded(rows: Int*): BitSet = {
    val set = new BitSet()
    rows.foreach(set.set)
    set
  }

  test("the visible rows come out in order, each mapped to its batch row") {
    withAllocator { allocator =>
      val source = rows(allocator, 8)
      val compacted = ExactScan.compact(
        source.nioBuffer(0, 8 * rowBytes).order(ByteOrder.nativeOrder()),
        8,
        excluded(0, 3, 4, 7),
        rowBytes,
        allocator
      )
      try {
        compacted.rows shouldBe 4
        compacted.batchRows.toSeq shouldBe Seq(1, 2, 5, 6)
        compacted.buffer.capacity() shouldBe 4L * rowBytes
        (0 until 4).foreach { row =>
          compacted.buffer.getFloat(row.toLong * rowBytes) shouldBe
            compacted.batchRows(row).toFloat
          compacted.buffer.getFloat(row.toLong * rowBytes + 4L) shouldBe
            -compacted.batchRows(row).toFloat
        }
      } finally {
        compacted.close()
        source.close()
      }
    }
  }

  test("a run of kept rows at either end is copied whole") {
    withAllocator { allocator =>
      val source = rows(allocator, 5)
      val compacted = ExactScan.compact(
        source.nioBuffer(0, 5 * rowBytes).order(ByteOrder.nativeOrder()),
        5,
        excluded(2),
        rowBytes,
        allocator
      )
      try {
        compacted.batchRows.toSeq shouldBe Seq(0, 1, 3, 4)
        compacted.buffer.getFloat(3L * rowBytes) shouldBe 4f
      } finally {
        compacted.close()
        source.close()
      }
    }
  }

  test("a batch with every row excluded is refused") {
    withAllocator { allocator =>
      val source = rows(allocator, 2)
      try
        an[IllegalArgumentException] should be thrownBy ExactScan.compact(
          source.nioBuffer(0, 2 * rowBytes).order(ByteOrder.nativeOrder()),
          2,
          excluded(0, 1),
          rowBytes,
          allocator
        )
      finally source.close()
    }
  }
}
