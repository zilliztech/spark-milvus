package com.zilliz.milvus.storage.index

import java.util.concurrent.{
  CompletableFuture,
  CountDownLatch,
  Executors,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicBoolean

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SearchThreadsTest extends AnyFunSuite with Matchers {

  private def awaitRelease(release: CountDownLatch): Unit = {
    var finished = false
    while (!finished) {
      try {
        release.await()
        finished = true
      } catch {
        case _: InterruptedException => ()
      }
    }
  }

  test("an interrupted collector caller waits until its buffer reader exits") {
    val collector = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val exited = new AtomicBoolean
    val interrupted = new AtomicBoolean
    val result = new CompletableFuture[Throwable]()
    val caller = new Thread(() => {
      try {
        SearchThreads.collect(Some(collector)) {
          started.countDown()
          awaitRelease(release)
          exited.set(true)
        }
        result.complete(null)
      } catch {
        case failure: Throwable =>
          interrupted.set(Thread.currentThread().isInterrupted)
          result.complete(failure)
      }
    })
    try {
      caller.start()
      started.await(10, TimeUnit.SECONDS) shouldBe true
      caller.interrupt()
      intercept[java.util.concurrent.TimeoutException] {
        result.get(100, TimeUnit.MILLISECONDS)
      }
      exited.get() shouldBe false
      release.countDown()
      result.get(10, TimeUnit.SECONDS) shouldBe a[InterruptedException]
      exited.get() shouldBe true
      interrupted.get() shouldBe true
    } finally {
      release.countDown()
      caller.join(10000)
      SearchThreads.shutdown(collector)
    }
  }

  test("a failed shard cannot return while another shard still uses a buffer") {
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val exited = new AtomicBoolean
    val failure = new IllegalArgumentException("bad answer")
    val result = new CompletableFuture[Throwable]()
    val caller = new Thread(() => {
      try {
        SearchThreads.shards(2, 2) { (shard, _, _) =>
          if (shard == 0) throw failure
          started.countDown()
          awaitRelease(release)
          exited.set(true)
        }
        result.complete(null)
      } catch {
        case cause: Throwable => result.complete(cause)
      }
    })
    try {
      caller.start()
      started.await(10, TimeUnit.SECONDS) shouldBe true
      intercept[java.util.concurrent.TimeoutException] {
        result.get(100, TimeUnit.MILLISECONDS)
      }
      release.countDown()
      result.get(10, TimeUnit.SECONDS).getMessage should include("bad answer")
      exited.get() shouldBe true
    } finally {
      release.countDown()
      caller.join(10000)
    }
  }

  test("interruption does not cancel shards and release their buffers early") {
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val exited = new AtomicBoolean
    val interrupted = new AtomicBoolean
    val result = new CompletableFuture[Throwable]()
    val caller = new Thread(() => {
      try {
        SearchThreads.shards(2, 2) { (shard, _, _) =>
          if (shard == 0) {
            started.countDown()
            awaitRelease(release)
            exited.set(true)
          }
        }
        result.complete(null)
      } catch {
        case failure: Throwable =>
          interrupted.set(Thread.currentThread().isInterrupted)
          result.complete(failure)
      }
    })
    try {
      caller.start()
      started.await(10, TimeUnit.SECONDS) shouldBe true
      caller.interrupt()
      intercept[java.util.concurrent.TimeoutException] {
        result.get(100, TimeUnit.MILLISECONDS)
      }
      exited.get() shouldBe false
      release.countDown()
      result.get(10, TimeUnit.SECONDS) shouldBe a[InterruptedException]
      exited.get() shouldBe true
      interrupted.get() shouldBe true
    } finally {
      release.countDown()
      caller.join(10000)
    }
  }

  test(
    "graceful shutdown waits for a native-style call and keeps interruption"
  ) {
    val worker = Executors.newSingleThreadExecutor()
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val exited = new AtomicBoolean
    val stopped = new CompletableFuture[Boolean]()
    worker.submit(new Runnable {
      def run(): Unit = {
        started.countDown()
        awaitRelease(release)
        exited.set(true)
      }
    })
    val closer = new Thread(() => {
      SearchThreads.shutdown(worker)
      stopped.complete(Thread.currentThread().isInterrupted)
    })
    try {
      started.await(10, TimeUnit.SECONDS) shouldBe true
      closer.start()
      closer.interrupt()
      intercept[java.util.concurrent.TimeoutException] {
        stopped.get(100, TimeUnit.MILLISECONDS)
      }
      exited.get() shouldBe false
      release.countDown()
      stopped.get(10, TimeUnit.SECONDS) shouldBe true
      worker.isTerminated shouldBe true
      exited.get() shouldBe true
    } finally {
      release.countDown()
      closer.join(10000)
      SearchThreads.shutdown(worker)
    }
  }
}
