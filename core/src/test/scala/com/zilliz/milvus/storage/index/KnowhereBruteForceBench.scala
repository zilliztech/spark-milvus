package com.zilliz.milvus.storage.index

import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.util.SplittableRandom
import scala.jdk.CollectionConverters._

import com.zilliz.milvus.jni.vector.{NativeVectorLibrary, NativeVectorSearch}

import io.knowhere.{DType, Knowhere}

/** A microbenchmark of `Knowhere.bruteForce` alone: no Spark, no storage, no
  * connector code between the caller and the JNI. It answers how the engine
  * wants to be called: how large a base block per call, how many queries per
  * call, how many concurrent callers, and how large its search pool.
  *
  * Every caller thread owns one base block (as a task owns a segment) and all
  * of them share one query buffer (as tasks share a broadcast query group). The
  * engine runs one single-threaded task per query row on its process-wide
  * search pool, so one caller already uses the whole machine when the query
  * count is large; concurrent callers compete for the same pool and the same
  * caches.
  *
  * Output is CSV on stdout, one line per cell, flushed as it goes. Not part of
  * the test suite; run with `sbt -Dmilvus.native.bundle=... "core/Test/runMain
  * com.zilliz.milvus.storage.index.KnowhereBruteForceBench"`. Environment:
  * BENCH_DIM (1024), BENCH_TOPK (10), BENCH_TARGET_PAIRS per cell (2e10),
  * BENCH_MAX_PAIRS per cell before it is skipped (8e10), BENCH_MAX_CALLS per
  * caller (256), BENCH_BLOCKS_MIB (1,4,32,128), BENCH_QUERIES
  * (1024,8192,65536), BENCH_THREADS (1,4,16,64), BENCH_POOLS (0 = leave the
  * default).
  */
object KnowhereBruteForceBench {

  private def env(name: String, fallback: String): String =
    sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)

  private def list(name: String, fallback: String): Seq[Long] =
    env(name, fallback).split(",").map(_.trim.toLong).toSeq

  def main(args: Array[String]): Unit = {
    val dim = env("BENCH_DIM", "1024").toInt
    val topK = env("BENCH_TOPK", "10").toInt
    val rowBytes = dim * 4L
    val targetPairs = env("BENCH_TARGET_PAIRS", "20000000000").toDouble
    val maxPairs = env("BENCH_MAX_PAIRS", "80000000000").toDouble
    val maxCalls = env("BENCH_MAX_CALLS", "256").toLong
    val blocksMiB = list("BENCH_BLOCKS_MIB", "1,4,32,128")
    val queryCounts = list("BENCH_QUERIES", "1024,8192,65536")
    val threadCounts = list("BENCH_THREADS", "1,4,16,64")
    val pools = list("BENCH_POOLS", "0")

    NativeVectorLibrary.load()
    val maxBlockBytes = blocksMiB.max * 1024L * 1024L
    val maxThreads = threadCounts.max.toInt
    val maxQueries = queryCounts.max

    System.err.println(
      s"allocating ${maxThreads} base blocks of ${blocksMiB.max} MiB and ${maxQueries} queries " +
        s"(${maxQueries * rowBytes / 1048576} MiB), dim=$dim"
    )
    val seed = new SplittableRandom(42L)
    val pattern = direct(maxBlockBytes)
    fillRandom(pattern, seed)
    val bases = (0 until maxThreads).map { i =>
      if (i == 0) pattern
      else {
        val b = direct(maxBlockBytes)
        b.put(pattern.duplicate()); b.clear(); b
      }
    }
    val queries = direct(maxQueries * rowBytes)
    fillRandom(queries, seed)
    System.err.println("data ready")

    println(
      "block_mib,base_rows,queries,threads,pool,calls,elapsed_s,pairs,pairs_per_s,per_call_ms,per_caller_pairs_per_s"
    )
    // Cheap cells first, so partial output is still a curve if the run is cut.
    val cells = for {
      pool <- pools
      threads <- threadCounts
      q <- queryCounts
      block <- blocksMiB
    } yield (pool.toInt, threads.toInt, q, block)
    cells
      .sortBy { case (_, threads, q, block) => threads.toLong * q * block }
      .foreach { case (pool, threads, q, blockMiB) =>
        if (pool > 0) Knowhere.resizeSearchThreadPool(pool)
        val poolNow = Knowhere.searchThreadPoolSize()
        val baseRows = blockMiB * 1024L * 1024L / rowBytes
        val pairsPerCall = baseRows.toDouble * q
        val perThreadTarget = targetPairs / threads
        val calls = math.min(
          maxCalls,
          math.max(1L, math.ceil(perThreadTarget / pairsPerCall).toLong)
        )
        val total = pairsPerCall * calls * threads
        if (total > maxPairs) {
          println(
            s"$blockMiB,$baseRows,$q,$threads,$poolNow,skipped,,${total.toLong},,,"
          )
        } else {
          val result = cell(
            bases.take(threads),
            baseRows,
            queries,
            q,
            dim,
            topK,
            calls.toInt
          )
          val (elapsedNanos, callNanos) = result
          val elapsed = elapsedNanos / 1e9
          val perCallMs = callNanos / 1e6 / (calls * threads)
          println(
            f"$blockMiB,$baseRows,$q,$threads,$poolNow,${calls * threads},$elapsed%.2f,${total.toLong}," +
              f"${(total / elapsed).toLong},$perCallMs%.1f,${(pairsPerCall * calls / (callNanos / 1e9 / threads)).toLong}"
          )
        }
        System.out.flush()
      }
  }

  /** Runs `calls` brute-force calls on each of `threads` callers concurrently.
    * Returns (wall nanos from first start to last end, summed per-call nanos).
    */
  private def cell(
      bases: Seq[ByteBuffer],
      baseRows: Long,
      queries: ByteBuffer,
      queryRows: Long,
      dim: Int,
      topK: Int,
      calls: Int
  ): (Long, Long) = {
    val rowBytes = dim * 4L
    val executor = Executors.newFixedThreadPool(bases.size)
    try {
      val tasks = bases.map { base =>
        new Callable[Long] {
          override def call(): Long = {
            val view = base.duplicate().order(ByteOrder.nativeOrder())
            view.limit((baseRows * rowBytes).toInt)
            val qv = queries.duplicate().order(ByteOrder.nativeOrder())
            qv.limit((queryRows * rowBytes).toInt)
            val ids = direct(queryRows * topK * 8L)
            val dists = direct(queryRows * topK * 4L)
            var spent = 0L
            var i = 0
            while (i < calls) {
              val started = System.nanoTime()
              NativeVectorSearch.bruteForce(
                DType.FLOAT32,
                view,
                baseRows,
                qv,
                queryRows,
                dim,
                topK,
                null,
                ids,
                dists,
                """{"metric_type":"L2"}"""
              )
              spent += System.nanoTime() - started
              i += 1
            }
            spent
          }
        }
      }
      val started = System.nanoTime()
      val futures = executor.invokeAll(tasks.asJava)
      val callNanos = futures.asScala.map(_.get()).sum
      (System.nanoTime() - started, callNanos)
    } finally {
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.MINUTES)
    }
  }

  private def direct(bytes: Long): ByteBuffer =
    ByteBuffer.allocateDirect(bytes.toInt).order(ByteOrder.nativeOrder())

  private def fillRandom(buffer: ByteBuffer, rng: SplittableRandom): Unit = {
    val floats =
      buffer.duplicate().order(ByteOrder.nativeOrder()).asFloatBuffer()
    val chunk = new Array[Float](1 << 16)
    var left = floats.capacity()
    while (left > 0) {
      val n = math.min(chunk.length, left)
      var i = 0
      while (i < n) {
        chunk(i) = (rng.nextDouble() * 2.0 - 1.0).toFloat; i += 1
      }
      floats.put(chunk, 0, n)
      left -= n
    }
  }
}
