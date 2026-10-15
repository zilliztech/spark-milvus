package com.zilliz.milvus.storage.io

import java.util.concurrent.{
  CompletableFuture,
  CompletionException,
  ExecutionException,
  ExecutorService,
  Executors,
  Semaphore,
  ThreadFactory,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicInteger

import com.zilliz.milvus.storage.Logging

/** Reads a list of object ranges several at a time and hands each one back, in
  * list order, to the calling thread.
  *
  * A range request to object storage waits on a round trip and on what one
  * connection transfers: 8 MiB ranges of a 2.58 GB index object read one after
  * another reach 47 MB/s, and 32 of them in flight reach 1.1 GB/s
  * (docs/design/projects/spark-milvus/architecture/vector-search.html section
  * 2.5). The requests run on a pool shared by the JVM; the caller consumes the
  * bytes in list order on its own thread, so whatever it copies them into has
  * one writer.
  *
  * The bytes every caller in the JVM has requested and not yet consumed stay
  * within one budget. The tasks of an executor share its heap and its network,
  * so the bound is per JVM rather than per call and does not grow with the
  * slots. A call blocks for the next range it needs only when it holds no other
  * range, so callers sharing the budget never wait on each other; ranges after
  * the next one are requested only while the budget has room.
  */
object ConcurrentRangeReads extends Logging {

  /** One range of one object; `fileSize` is the object's size on the store. */
  final case class Range(
      key: String,
      offset: Long,
      length: Int,
      fileSize: Long
  ) {
    require(
      offset >= 0L && length > 0 && offset <= fileSize - length,
      s"Range $offset+$length lies outside $key ($fileSize bytes)"
    )
  }

  val MinBudgetBytes: Long = 64L << 20
  val MaxBudgetBytes: Long = 256L << 20

  /** Requests in flight at once; more requests than this wait in the pool's
    * queue with their bytes already counted against the budget.
    */
  val Threads: Int = 32

  /** The budget of this JVM: a sixteenth of the maximum heap, at least 64 MiB
    * and at most 256 MiB, which is 32 ranges of 8 MiB.
    */
  val BudgetBytes: Int = budgetFor(Runtime.getRuntime.maxMemory())

  private[io] def budgetFor(maxHeapBytes: Long): Int =
    math.max(MinBudgetBytes, math.min(MaxBudgetBytes, maxHeapBytes / 16)).toInt

  /** Bytes requested and not yet consumed, counted in permits of one byte. A
    * range longer than the whole budget takes all of it and is read alone.
    */
  private[io] final class Budget(val bytes: Int) {
    require(bytes > 0, s"A read budget must be positive: $bytes")
    private val permits = new Semaphore(bytes, true)

    def permitsFor(length: Int): Int = math.min(length, bytes)
    def acquire(count: Int): Unit = permits.acquire(count)

    // A zero timeout keeps the fairness order, so a caller blocked on its next
    // range is served before other callers' read-ahead.
    def tryAcquire(count: Int): Boolean =
      permits.tryAcquire(count, 0L, TimeUnit.SECONDS)
    def release(count: Int): Unit = permits.release(count)
    def available: Int = permits.availablePermits()
  }

  private lazy val shared: Budget = {
    logInfo(
      s"Concurrent range reads: budget=${BudgetBytes >> 20}MiB " +
        s"(max heap ${Runtime.getRuntime.maxMemory() >> 20}MiB / 16, 64 to 256 MiB), threads=$Threads"
    )
    new Budget(BudgetBytes)
  }

  private lazy val pool: ExecutorService =
    Executors.newFixedThreadPool(
      Threads,
      new ThreadFactory {
        private val count = new AtomicInteger()
        override def newThread(task: Runnable): Thread = {
          val thread =
            new Thread(task, s"milvus-range-read-${count.incrementAndGet()}")
          thread.setDaemon(true)
          thread
        }
      }
    )

  /** Reads every range and calls `consume` with each one's bytes, in list
    * order, on the calling thread. A failed read fails the call with that
    * read's own exception. Every submitted read finishes before this returns,
    * so the caller can then close the borrowed store, even after a failure.
    */
  def foreach(store: ObjectStore, ranges: IndexedSeq[Range])(
      consume: (Range, Array[Byte]) => Unit
  ): Unit = foreach(store, ranges, shared, pool)(consume)

  private[io] def foreach(
      store: ObjectStore,
      ranges: IndexedSeq[Range],
      budget: Budget,
      executor: ExecutorService
  )(consume: (Range, Array[Byte]) => Unit): Unit = {
    val count = ranges.length
    val reads = new Array[CompletableFuture[Array[Byte]]](count)
    val held = new Array[Int](count)
    var issued = 0
    var next = 0
    var failure: Throwable = null
    def issue(index: Int, permits: Int): Unit = {
      val range = ranges(index)
      try
        reads(index) = CompletableFuture.supplyAsync[Array[Byte]](
          () => {
            val bytes = store.readAt(
              range.key,
              range.offset,
              range.length.toLong,
              range.fileSize
            )
            require(
              bytes.length == range.length,
              s"Object size changed while reading ${range.key} at offset ${range.offset}: " +
                s"${bytes.length} of ${range.length} bytes"
            )
            bytes
          },
          executor
        )
      catch {
        case rejected: Throwable =>
          budget.release(permits)
          throw rejected
      }
      held(index) = permits
    }
    try {
      while (next < count) {
        if (issued == next) {
          // Every earlier range is consumed and nothing later is requested, so
          // this call holds no bytes while it waits here.
          val permits = budget.permitsFor(ranges(next).length)
          budget.acquire(permits)
          issue(next, permits)
          issued += 1
        }
        var room = true
        while (room && issued < count) {
          val permits = budget.permitsFor(ranges(issued).length)
          if (budget.tryAcquire(permits)) {
            issue(issued, permits)
            issued += 1
          } else room = false
        }
        val bytes = await(reads(next))
        try consume(ranges(next), bytes)
        finally {
          budget.release(held(next))
          held(next) = 0
          reads(next) = null
        }
        next += 1
      }
    } catch {
      case cause: Throwable => failure = cause
    } finally {
      // Native reads cannot be cancelled. join waits through interruption and
      // leaves its flag set; every borrower must finish before the store owner
      // regains control. A failed consume already released its own permits.
      var index = next
      while (index < issued) {
        val permits = held(index)
        if (permits > 0) {
          try reads(index).join()
          catch {
            case thrown: Throwable =>
              val cause = thrown match {
                case wrapped: CompletionException if wrapped.getCause != null =>
                  wrapped.getCause
                case other => other
              }
              if (failure == null) failure = cause
              else if (cause ne failure) failure.addSuppressed(cause)
          } finally {
            budget.release(permits)
            held(index) = 0
            reads(index) = null
          }
        }
        index += 1
      }
    }
    if (failure != null) {
      if (failure.isInstanceOf[InterruptedException])
        Thread.currentThread().interrupt()
      throw failure
    }
  }

  private def await(read: CompletableFuture[Array[Byte]]): Array[Byte] =
    try read.get()
    catch {
      case failure: ExecutionException =>
        failure.getCause match {
          case wrapped: CompletionException if wrapped.getCause != null =>
            throw wrapped.getCause
          case null  => throw failure
          case cause => throw cause
        }
    }
}
