package com.zilliz.spark.connector.read

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** R11: a pushed-down limit stops each partition after N rows. */
class LimitedReadersTest extends AnyFunSuite with Matchers {

  /** Yields batches of the given sizes. */
  private class Batches(sizes: Seq[Int], allocator: RootAllocator)
      extends PartitionReader[ColumnarBatch] {
    private var i = -1
    private var current: ColumnarBatch = null
    override def next(): Boolean = {
      i += 1
      // A real reader releases a batch before producing the next; the fake
      // must too or the allocator's leak check fails for the fake's sake.
      if (current != null) { current.close(); current = null }
      if (i >= sizes.size) return false
      val v = new BigIntVector("id", allocator)
      v.allocateNew(sizes(i));
      (0 until sizes(i)).foreach(j => v.setSafe(j, j.toLong));
      v.setValueCount(sizes(i))
      current = new ColumnarBatch(Array(new ArrowColumnVector(v)), sizes(i))
      true
    }
    override def get(): ColumnarBatch = current
    override def close(): Unit =
      if (current != null) { current.close(); current = null }
  }

  test("a zero limit stops before opening the first batch") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val underlying = new Batches(Seq(1), allocator)
      val reader = new LimitedBatchReader(underlying, 0)
      try {
        reader.next() shouldBe false
        underlying.get() shouldBe null
      } finally reader.close()
    } finally allocator.close()
  }

  test("a batch reader cuts the last batch short and stops") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val r = new LimitedBatchReader(new Batches(Seq(4, 4, 4), allocator), 6)
      try {
        r.next() shouldBe true; r.get().numRows() shouldBe 4
        r.next() shouldBe true; r.get().numRows() shouldBe 2
        r.next() shouldBe false
      } finally r.close()
    } finally allocator.close()
  }

  test(
    "a batch limit that lands exactly on a boundary does not read another batch"
  ) {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val r = new LimitedBatchReader(new Batches(Seq(3, 3), allocator), 3)
      try {
        r.next() shouldBe true; r.get().numRows() shouldBe 3
        r.next() shouldBe false
      } finally r.close()
    } finally allocator.close()
  }
}
