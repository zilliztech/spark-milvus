package com.zilliz.spark.connector.scan

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** R11: a pushed-down limit stops each partition after N rows. */
class LimitedReadersTest extends AnyFunSuite with Matchers {

  /** Yields the given rows, then stops. Records whether it was closed. */
  private class Rows(values: Seq[Long]) extends PartitionReader[InternalRow] {
    private var i = -1
    var closed = false
    override def next(): Boolean = { i += 1; i < values.size }
    override def get(): InternalRow = InternalRow(values(i))
    override def close(): Unit = closed = true
  }

  test("a row reader stops after the limit") {
    val r = new LimitedRowReader(new Rows(Seq(1L, 2L, 3L, 4L, 5L)), 3)
    val seen = Iterator.continually(r.next()).takeWhile(identity).map(_ => r.get().getLong(0)).toList
    seen shouldBe List(1L, 2L, 3L)
  }

  test("a limit larger than the partition delivers everything") {
    val r = new LimitedRowReader(new Rows(Seq(1L, 2L)), 10)
    Iterator.continually(r.next()).takeWhile(identity).size shouldBe 2
  }

  test("a limit of zero delivers nothing and never touches the reader") {
    val under = new Rows(Seq(1L))
    val r = new LimitedRowReader(under, 0)
    r.next() shouldBe false
  }

  test("close reaches the underlying reader") {
    val under = new Rows(Seq(1L))
    new LimitedRowReader(under, 1).close()
    under.closed shouldBe true
  }

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
      v.allocateNew(sizes(i)); (0 until sizes(i)).foreach(j => v.setSafe(j, j.toLong)); v.setValueCount(sizes(i))
      current = new ColumnarBatch(Array(new ArrowColumnVector(v)), sizes(i))
      true
    }
    override def get(): ColumnarBatch = current
    override def close(): Unit = if (current != null) { current.close(); current = null }
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

  test("a batch limit that lands exactly on a boundary does not read another batch") {
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
