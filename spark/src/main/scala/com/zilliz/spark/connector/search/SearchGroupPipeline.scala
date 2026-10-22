package com.zilliz.spark.connector.search

import java.util.concurrent.{
  Callable,
  ExecutionException,
  Executors,
  Future,
  ThreadFactory,
  TimeUnit
}

/** Searches one group ahead while its caller packs and emits the current one.
  *
  * Only the caller reads `groups`; only the worker calls `search`. The result
  * must own its data after search returns. A completed result waits in the
  * single pending future until the caller takes it: no further group starts
  * while that result waits. Close drains the worker before releasing borrowed
  * resources, including after an interrupted wait or an abandoned iterator.
  */
private[search] final class SearchGroupPipeline[A, B](
    groups: Iterator[A],
    search: A => B,
    release: () => Unit
) extends Iterator[B]
    with AutoCloseable {

  private val worker = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(runnable, "search-group")
      thread.setDaemon(true)
      thread
    }
  })
  private var started = false
  private var closed = false
  private var pending: Future[B] = null

  private def submit(): Unit = {
    if (Thread.currentThread().isInterrupted)
      throw new InterruptedException("Query group search interrupted")
    if (groups.hasNext) {
      val group = groups.next()
      pending = worker.submit(new Callable[B] {
        override def call(): B = search(group)
      })
    }
  }

  private def guarded[C](body: => C): C =
    try body
    catch {
      case failure: Throwable =>
        try close()
        catch {
          case later: Throwable if later ne failure =>
            failure.addSuppressed(later)
        }
        throw failure
    }

  override def hasNext: Boolean = guarded {
    if (closed) false
    else {
      if (!started) {
        started = true
        submit()
      }
      if (pending == null) close()
      pending != null
    }
  }

  override def next(): B = guarded {
    if (!hasNext) throw new NoSuchElementException("No query group remains")
    val result = await(pending)
    pending = null
    // Read the next group's Spark input on the caller, before its search
    // overlaps the caller's packing and output of this result.
    submit()
    result
  }

  private def await(future: Future[B]): B = {
    var interrupted: InterruptedException = null
    var failure: Throwable = null
    var result: Option[B] = None
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
      }
    }
    if (interrupted != null) {
      if (failure != null && (failure ne interrupted))
        interrupted.addSuppressed(failure)
      Thread.currentThread().interrupt()
      throw interrupted
    }
    if (failure != null) throw failure
    result.get
  }

  override def close(): Unit = {
    if (closed) return
    closed = true
    worker.shutdown()
    var failure: Throwable = null
    def completed(body: => Unit): Unit =
      try body
      catch {
        case cause: Throwable =>
          if (failure == null) failure = cause
          else if (failure ne cause) failure.addSuppressed(cause)
      }
    // Never cancel the future: native work can still hold the query matrix,
    // index and workspace after Java reports a cancelled future as done.
    if (pending != null) completed { await(pending); () }
    pending = null
    var interrupted = false
    var stopped = false
    while (!stopped) {
      try stopped = worker.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)
      catch { case _: InterruptedException => interrupted = true }
    }
    completed(release())
    if (interrupted) Thread.currentThread().interrupt()
    if (failure != null) throw failure
  }
}
