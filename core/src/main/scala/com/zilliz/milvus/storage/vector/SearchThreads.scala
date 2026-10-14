package com.zilliz.milvus.storage.vector

import java.util.concurrent.{
  Callable,
  CancellationException,
  ExecutionException,
  ExecutorService,
  ForkJoinPool,
  ForkJoinWorkerThread,
  Future,
  TimeUnit
}
import scala.collection.mutable.ArrayBuffer

/** The threads a search task's Java-side work runs on when it is cut into
  * shards: an answer's checking and collecting, a group's packing.
  *
  * A search task's Knowhere call takes every core of its executor; between two
  * calls the cores are free, and this is what fills them. One pool for the JVM,
  * as wide as the processors it may use (`-XX:ActiveProcessorCount` on the
  * executors), daemon threads named for the log
  * (docs/design/architecture/vector-search.html section 2.1, 2026-09-27
  * decision).
  */
object SearchThreads {

  /** Keeps a merger phase on the task's collector. The caller retains the
    * buffers the phase reads until the work has actually finished, including
    * when the caller is interrupted while waiting.
    */
  def collect[A](collector: Option[ExecutorService])(body: => A): A =
    collector match {
      case None => body
      case Some(worker) =>
        await(worker.submit(new Callable[A] { def call(): A = body }))
    }

  /** A cancelled future cannot prove that a native call stopped. These waits
    * are for work whose owner never cancels it: interruption is reported only
    * after every submitted body has returned and stopped using its buffers.
    */
  private[vector] def await[A](future: Future[A]): A =
    awaitAll(Seq(future)).head

  private[vector] def awaitAll[A](futures: Iterable[Future[A]]): Seq[A] = {
    val results = Vector.newBuilder[A]
    var failure: Throwable = null
    var interrupted: InterruptedException = null
    def failed(cause: Throwable): Unit = {
      if (failure == null) failure = cause
      else if (failure ne cause) failure.addSuppressed(cause)
    }
    futures.foreach { future =>
      var finished = false
      while (!finished) {
        try {
          results += future.get()
          finished = true
        } catch {
          case cause: InterruptedException =>
            if (interrupted == null) interrupted = cause
          case wrapped: ExecutionException =>
            failed(Option(wrapped.getCause).getOrElse(wrapped))
            finished = true
          case cancelled: CancellationException =>
            failed(cancelled)
            finished = true
        }
      }
    }
    if (interrupted != null) {
      if (failure != null && (failure ne interrupted))
        interrupted.addSuppressed(failure)
      Thread.currentThread().interrupt()
      throw interrupted
    }
    if (failure != null) throw failure
    results.result()
  }

  /** Transfers an asynchronously created resource, or releases it after an
    * interrupted wait. The future is never cancelled: its native work must
    * return before its result or the resources it borrows can be closed.
    */
  private[storage] def take[A](future: Future[A])(release: A => Unit): A = {
    var interrupted: InterruptedException = null
    var failure: Throwable = null
    var result: Option[A] = None
    var finished = false
    while (!finished) {
      try {
        result = Some(future.get())
        finished = true
      } catch {
        case cause: InterruptedException =>
          if (interrupted == null) interrupted = cause
        case wrapped: ExecutionException =>
          failure = Option(wrapped.getCause).getOrElse(wrapped)
          finished = true
        case cancelled: CancellationException =>
          failure = cancelled
          finished = true
      }
    }
    if (interrupted != null) {
      if (failure != null) interrupted.addSuppressed(failure)
      result.foreach { value =>
        try release(value)
        catch { case cause: Throwable => interrupted.addSuppressed(cause) }
      }
      Thread.currentThread().interrupt()
      throw interrupted
    }
    if (failure != null) throw failure
    result.get
  }

  /** Stops accepting work and waits for every body to return. Native calls do
    * not stop when their Java thread is interrupted, so shutdownNow is not a
    * resource-lifetime boundary.
    */
  private[storage] def shutdown(worker: ExecutorService): Unit = {
    worker.shutdown()
    var interrupted: InterruptedException = null
    var finished = false
    while (!finished) {
      try
        finished = worker.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
      catch {
        case cause: InterruptedException =>
          if (interrupted == null) interrupted = cause
      }
    }
    if (interrupted != null) Thread.currentThread().interrupt()
  }

  lazy val pool: ForkJoinPool = new ForkJoinPool(
    math.max(1, Runtime.getRuntime.availableProcessors()),
    (pool: ForkJoinPool) => {
      val thread = new ForkJoinWorkerThread(pool) {}
      thread.setName(s"search-shard-${thread.getPoolIndex}")
      thread.setDaemon(true)
      thread
    },
    null,
    false
  )

  /** Cuts `items` positions into at most `shards` even, contiguous ranges and
    * runs `body(shard, from, until)` on every one at once, returning the
    * results in shard order. One shard runs on the caller's thread with no pool
    * at all, so a caller that asked for one thread pays nothing.
    */
  def shards[A](items: Int, shards: Int)(body: (Int, Int, Int) => A): Seq[A] = {
    require(items >= 0, s"Shards cut a non-negative count: $items")
    val count = math.max(1, math.min(shards, math.max(1, items)))
    if (count == 1) return Seq(body(0, 0, items))
    val per = (items + count - 1) / count
    val futures = ArrayBuffer.empty[Future[A]]
    try {
      (0 until count).foreach { shard =>
        val from = math.min(shard * per, items)
        val until = math.min(from + per, items)
        futures += pool.submit(new Callable[A] {
          def call(): A = body(shard, from, until)
        })
      }
    } catch {
      case failure: Throwable =>
        try awaitAll(futures.toSeq)
        catch {
          case later: Throwable if later ne failure =>
            failure.addSuppressed(later)
        }
        throw failure
    }
    awaitAll(futures.toSeq)
  }
}
