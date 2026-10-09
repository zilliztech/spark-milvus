package com.zilliz.milvus.storage.index

import java.util.concurrent.{
  Callable,
  ExecutionException,
  ForkJoinPool,
  ForkJoinWorkerThread
}
import scala.jdk.CollectionConverters._

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
    val callables = (0 until count).map { shard =>
      val from = math.min(shard * per, items)
      val until = math.min(from + per, items)
      new Callable[A] { def call(): A = body(shard, from, until) }
    }
    pool
      .invokeAll(callables.asJava)
      .asScala
      .map { future =>
        try future.get()
        catch {
          case wrapped: ExecutionException =>
            throw Option(wrapped.getCause).getOrElse(wrapped)
        }
      }
      .toSeq
  }
}
