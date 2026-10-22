package com.zilliz.spark.connector.search

import java.util.concurrent.{
  CompletableFuture,
  CountDownLatch,
  TimeUnit,
  TimeoutException
}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SearchGroupPipelineTest extends AnyFunSuite with Matchers {

  private def awaitRelease(release: CountDownLatch): Unit = {
    var finished = false
    while (!finished) {
      try {
        release.await()
        finished = true
      } catch { case _: InterruptedException => () }
    }
  }

  test(
    "search overlaps consumption by one group and input stays on its caller"
  ) {
    val caller = Thread.currentThread()
    val read = new AtomicInteger
    val secondStarted = new CountDownLatch(1)
    val releaseSecond = new CountDownLatch(1)
    val thirdStarted = new CountDownLatch(1)
    val closed = new AtomicInteger
    val groups = Iterator(1, 2, 3, 4).map { group =>
      Thread.currentThread() shouldBe caller
      read.incrementAndGet()
      group
    }
    val pipeline = new SearchGroupPipeline[Int, Int](
      groups,
      group => {
        Thread.currentThread() should not be caller
        if (group == 2) {
          secondStarted.countDown()
          awaitRelease(releaseSecond)
        }
        if (group == 3) thirdStarted.countDown()
        group * 10
      },
      () => { closed.incrementAndGet(); () }
    )
    try {
      pipeline.next() shouldBe 10
      secondStarted.await(10, TimeUnit.SECONDS) shouldBe true
      read.get() shouldBe 2
      pipeline.hasNext shouldBe true
      pipeline.hasNext shouldBe true
      read.get() shouldBe 2
      thirdStarted.getCount shouldBe 1L
      releaseSecond.countDown()
      pipeline.toVector shouldBe Vector(20, 30, 40)
      closed.get() shouldBe 1
      pipeline.hasNext shouldBe false
    } finally {
      releaseSecond.countDown()
      pipeline.close()
    }
    closed.get() shouldBe 1
  }

  test("empty and unconsumed input release resources exactly once") {
    val released = new AtomicInteger
    val searched = new AtomicInteger
    val empty = new SearchGroupPipeline[Int, Int](
      Iterator.empty,
      group => { searched.incrementAndGet(); group },
      () => { released.incrementAndGet(); () }
    )
    empty.hasNext shouldBe false
    intercept[NoSuchElementException](empty.next())
    empty.close()
    released.get() shouldBe 1
    searched.get() shouldBe 0

    val untouched = new SearchGroupPipeline[Int, Int](
      Iterator(1),
      group => { searched.incrementAndGet(); group },
      () => { released.incrementAndGet(); () }
    )
    untouched.close()
    untouched.hasNext shouldBe false
    released.get() shouldBe 2
    searched.get() shouldBe 0
  }

  test("early close waits for the next search before releasing its resources") {
    val started = new CountDownLatch(1)
    val releaseSearch = new CountDownLatch(1)
    val exited = new AtomicBoolean
    val closed = new AtomicInteger
    val read = new AtomicInteger
    val stopped = new CompletableFuture[Unit]()
    val pipeline = new SearchGroupPipeline[Int, Int](
      Iterator(1, 2, 3).map { group => read.incrementAndGet(); group },
      group => {
        if (group == 2) {
          started.countDown()
          awaitRelease(releaseSearch)
          exited.set(true)
        }
        group
      },
      () => {
        exited.get() shouldBe true
        closed.incrementAndGet()
        ()
      }
    )
    val closer = new Thread(() => {
      try {
        pipeline.close()
        stopped.complete(())
      } catch {
        case failure: Throwable => stopped.completeExceptionally(failure)
      }
      ()
    })
    try {
      pipeline.next() shouldBe 1
      started.await(10, TimeUnit.SECONDS) shouldBe true
      closer.start()
      intercept[TimeoutException](stopped.get(100, TimeUnit.MILLISECONDS))
      closed.get() shouldBe 0
      releaseSearch.countDown()
      stopped.get(10, TimeUnit.SECONDS)
      closed.get() shouldBe 1
      read.get() shouldBe 2
      pipeline.hasNext shouldBe false
    } finally {
      releaseSearch.countDown()
      closer.join(10000)
      pipeline.close()
    }
  }

  test(
    "search failure remains primary when releasing its resources also fails"
  ) {
    val failure = new IllegalArgumentException("invalid search answer")
    val cleanup = new IllegalStateException("closing index")
    val closed = new AtomicInteger
    val pipeline = new SearchGroupPipeline[Int, Int](
      Iterator(1, 2),
      _ => throw failure,
      () => { closed.incrementAndGet(); throw cleanup }
    )
    intercept[IllegalArgumentException](pipeline.next()) shouldBe failure
    failure.getSuppressed.toSeq should contain(cleanup)
    closed.get() shouldBe 1
    pipeline.close()
    closed.get() shouldBe 1
  }

  test(
    "failure reading the next group closes the already searched group scope"
  ) {
    val failure = new IllegalArgumentException("query shuffle read failed")
    val closed = new AtomicInteger
    val searched = new AtomicInteger
    val groups = Iterator(1, 2).map { group =>
      if (group == 2) throw failure
      group
    }
    val pipeline = new SearchGroupPipeline[Int, Int](
      groups,
      group => { searched.incrementAndGet(); group },
      () => { closed.incrementAndGet(); () }
    )
    intercept[IllegalArgumentException](pipeline.next()) shouldBe failure
    searched.get() shouldBe 1
    closed.get() shouldBe 1
    pipeline.hasNext shouldBe false
  }

  test(
    "close reports a prefetched search failure even when its result is unused"
  ) {
    val failure = new IllegalArgumentException("next group failed")
    val closed = new AtomicInteger
    val pipeline = new SearchGroupPipeline[Int, Int](
      Iterator(1, 2, 3),
      group => if (group == 2) throw failure else group,
      () => { closed.incrementAndGet(); () }
    )
    pipeline.next() shouldBe 1
    intercept[IllegalArgumentException](pipeline.close()) shouldBe failure
    closed.get() shouldBe 1
    pipeline.close()
  }

  test(
    "interrupted callers drain native-style work before closing its buffers"
  ) {
    val started = new CountDownLatch(1)
    val releaseSearch = new CountDownLatch(1)
    val exited = new AtomicBoolean
    val closed = new AtomicBoolean
    val interrupted = new AtomicBoolean
    val result = new CompletableFuture[Throwable]()
    val caller = new Thread(() => {
      val pipeline = new SearchGroupPipeline[Int, Int](
        Iterator(1, 2),
        group => {
          started.countDown()
          awaitRelease(releaseSearch)
          exited.set(true)
          group
        },
        () => {
          exited.get() shouldBe true
          closed.set(true)
        }
      )
      try {
        pipeline.next()
        result.complete(null)
      } catch {
        case failure: Throwable =>
          interrupted.set(Thread.currentThread().isInterrupted)
          result.complete(failure)
      } finally pipeline.close()
      ()
    })
    try {
      caller.start()
      started.await(10, TimeUnit.SECONDS) shouldBe true
      caller.interrupt()
      intercept[TimeoutException](result.get(100, TimeUnit.MILLISECONDS))
      exited.get() shouldBe false
      closed.get() shouldBe false
      releaseSearch.countDown()
      result.get(10, TimeUnit.SECONDS) shouldBe a[InterruptedException]
      exited.get() shouldBe true
      closed.get() shouldBe true
      interrupted.get() shouldBe true
    } finally {
      releaseSearch.countDown()
      caller.join(10000)
    }
  }

  test(
    "interruption before the first submission releases without starting search"
  ) {
    val searched = new AtomicInteger
    val closed = new AtomicInteger
    val result = new CompletableFuture[(Throwable, Boolean)]()
    val caller = new Thread(() => {
      val pipeline = new SearchGroupPipeline[Int, Int](
        Iterator(1, 2),
        group => { searched.incrementAndGet(); group },
        () => { closed.incrementAndGet(); () }
      )
      Thread.currentThread().interrupt()
      try {
        pipeline.hasNext
        result.complete((null, Thread.currentThread().isInterrupted))
      } catch {
        case failure: Throwable =>
          result.complete((failure, Thread.currentThread().isInterrupted))
      } finally pipeline.close()
      ()
    })
    caller.start()
    val (failure, interrupted) = result.get(10, TimeUnit.SECONDS)
    caller.join(10000)
    failure shouldBe a[InterruptedException]
    interrupted shouldBe true
    searched.get() shouldBe 0
    closed.get() shouldBe 1
  }
}
