package com.zilliz.milvus.storage.io

import java.util.concurrent.{
  CountDownLatch,
  ExecutorService,
  Executors,
  TimeUnit
}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.io.ConcurrentRangeReads.{Budget, Range}

class ConcurrentRangeReadsTest extends AnyFunSuite {

  /** A range's bytes are a function of its key and position, so what comes back
    * says where it was read from.
    */
  private def byteAt(key: String, position: Long): Byte =
    ((key.length * 31L + position) % 251L).toByte

  private class Store(read: (String, Long, Int) => Unit = (_, _, _) => ())
      extends ObjectStore {
    val inFlight = new AtomicInteger()
    val maxInFlight = new AtomicInteger()
    override def readAt(
        key: String,
        offset: Long,
        length: Long,
        fileSize: Long
    ): Array[Byte] = {
      val now = inFlight.incrementAndGet()
      maxInFlight.accumulateAndGet(now, (a: Int, b: Int) => math.max(a, b))
      try {
        read(key, offset, length.toInt)
        Array.tabulate(length.toInt)(i => byteAt(key, offset + i))
      } finally inFlight.decrementAndGet()
    }
    override def readAll(key: String): Array[Byte] =
      throw new UnsupportedOperationException
    override def size(key: String): Long =
      throw new UnsupportedOperationException
    override def list(key: String, recursive: Boolean): Seq[FileInfo] =
      throw new UnsupportedOperationException
    override def exists(key: String): Boolean =
      throw new UnsupportedOperationException
    override def write(key: String, data: Array[Byte]): Unit =
      throw new UnsupportedOperationException
    override def createDir(key: String, recursive: Boolean): Unit =
      throw new UnsupportedOperationException
    override def delete(key: String): Unit =
      throw new UnsupportedOperationException
    override def close(): Unit = ()
  }

  private def withPool(threads: Int)(body: ExecutorService => Unit): Unit = {
    val pool = Executors.newFixedThreadPool(threads)
    try body(pool)
    finally pool.shutdownNow()
  }

  private def ranges(key: String, count: Int, length: Int): IndexedSeq[Range] =
    (0 until count).map(i =>
      Range(key, i.toLong * length, length, count.toLong * length)
    )

  /** Background reads give their bytes back when they end, not when the call
    * returns.
    */
  private def eventually(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (!condition && System.nanoTime() < deadline) Thread.sleep(5)
    assert(condition)
  }

  test("ranges come back in list order with their own bytes") {
    val store = new Store((_, offset, _) => Thread.sleep((offset / 10) % 4))
    val list = ranges("a", 20, 10) ++ ranges("bb", 5, 7)
    val seen = ArrayBuffer.empty[(Range, Array[Byte])]
    withPool(8) { pool =>
      ConcurrentRangeReads.foreach(store, list, new Budget(64), pool) {
        (range, bytes) => seen += ((range, bytes))
      }
    }
    assert(seen.map(_._1) == list)
    seen.foreach { case (range, bytes) =>
      assert(
        bytes.toSeq == (0 until range.length).map(i =>
          byteAt(range.key, range.offset + i)
        )
      )
    }
  }

  test("reads overlap while the budget has room") {
    val gate = new CountDownLatch(3)
    val overlapped = new AtomicBoolean(true)
    val store = new Store((_, _, _) => {
      gate.countDown()
      if (!gate.await(5, TimeUnit.SECONDS)) overlapped.set(false)
    })
    withPool(4) { pool =>
      ConcurrentRangeReads.foreach(
        store,
        ranges("a", 6, 10),
        new Budget(60),
        pool
      )((_, _) => ())
    }
    assert(overlapped.get(), "three reads were never in flight together")
  }

  test("the budget bounds what is requested and not yet consumed") {
    val store = new Store((_, _, _) => Thread.sleep(2))
    val budget = new Budget(30)
    withPool(8) { pool =>
      ConcurrentRangeReads.foreach(store, ranges("a", 24, 10), budget, pool) {
        (_, _) =>
          // Held until consumed: in flight plus waiting never exceeds three.
          assert(budget.available <= 20)
          Thread.sleep(3)
      }
    }
    assert(store.maxInFlight.get() <= 3)
    assert(store.maxInFlight.get() >= 2)
    assert(budget.available == 30)
  }

  test("a range longer than the whole budget is read alone") {
    val store = new Store()
    val budget = new Budget(8)
    val list = IndexedSeq(
      Range("a", 0, 5, 45),
      Range("a", 5, 20, 45),
      Range("a", 25, 20, 45)
    )
    val seen = ArrayBuffer.empty[Long]
    withPool(4) { pool =>
      ConcurrentRangeReads.foreach(store, list, budget, pool) { (range, _) =>
        seen += range.offset
      }
    }
    assert(seen == Seq(0L, 5L, 25L))
    assert(store.maxInFlight.get() == 1)
    assert(budget.available == 8)
  }

  test("a failed read fails the call with its own exception") {
    val store = new Store((_, offset, _) =>
      if (offset == 30L) throw new IllegalStateException("read 30 failed")
    )
    val budget = new Budget(50)
    val consumed = ArrayBuffer.empty[Long]
    withPool(8) { pool =>
      val failure = intercept[IllegalStateException] {
        ConcurrentRangeReads.foreach(store, ranges("a", 10, 10), budget, pool) {
          (range, _) => consumed += range.offset
        }
      }
      assert(failure.getMessage == "read 30 failed")
      eventually(budget.available == 50)
    }
    assert(consumed == Seq(0L, 10L, 20L))
  }

  test("a consumer failure returns every byte to the budget") {
    val store = new Store((_, _, _) => Thread.sleep(1))
    val budget = new Budget(40)
    withPool(8) { pool =>
      intercept[IllegalArgumentException] {
        ConcurrentRangeReads.foreach(store, ranges("a", 12, 10), budget, pool) {
          (range, _) => require(range.offset != 20L, "stop at 20")
        }
      }
      eventually(budget.available == 40)
    }
  }

  test("a range that comes back short fails the call") {
    val store = new Store() {
      override def readAt(
          key: String,
          offset: Long,
          length: Long,
          fileSize: Long
      ): Array[Byte] = new Array[Byte](length.toInt - 1)
    }
    withPool(2) { pool =>
      val failure = intercept[IllegalArgumentException] {
        ConcurrentRangeReads.foreach(
          store,
          ranges("a", 3, 10),
          new Budget(30),
          pool
        )((_, _) => ())
      }
      assert(failure.getMessage.contains("size changed"))
    }
  }

  test("an empty list reads nothing") {
    val store = new Store()
    withPool(1) { pool =>
      ConcurrentRangeReads.foreach(
        store,
        IndexedSeq.empty,
        new Budget(10),
        pool
      )((_, _) => fail("nothing to consume"))
    }
    assert(store.maxInFlight.get() == 0)
  }

  test("the budget is a sixteenth of the heap between 64 and 256 MiB") {
    val mib = 1L << 20
    assert(ConcurrentRangeReads.budgetFor(512 * mib) == 64 * mib)
    assert(ConcurrentRangeReads.budgetFor(2048 * mib) == 128 * mib)
    assert(ConcurrentRangeReads.budgetFor(4096 * mib) == 256 * mib)
    assert(ConcurrentRangeReads.budgetFor(Long.MaxValue) == 256 * mib)
  }
}
