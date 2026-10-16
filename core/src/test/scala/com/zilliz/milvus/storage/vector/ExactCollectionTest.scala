package com.zilliz.milvus.storage.vector

import java.lang.{Float => JavaFloat}
import java.nio.ByteBuffer
import java.util.concurrent.{
  CountDownLatch,
  Executors,
  LinkedBlockingQueue,
  ThreadPoolExecutor,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.BitSet

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}

/** Mixed collection and bounded function scoring need no native library. */
class ExactCollectionTest extends AnyFunSuite with Matchers {
  private val layout = VectorLayout(VectorElementType.Float32, 2)

  test("exact scores while an index collects, but waits its turn to merge") {
    Seq(1, 4).foreach { threads =>
      val allocator = new RootAllocator()
      val collector = new ThreadPoolExecutor(
        1,
        1,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue[Runnable]()
      )
      val caller = Executors.newSingleThreadExecutor()
      val collecting = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val scored = new CountDownLatch(8)
      val block = new AtomicBoolean(false)
      val matrix =
        QueryMatrix.ofFloats(Seq.fill(8)(Array(0f, 0f)), layout, allocator)
      val batch = VectorBatchFixture.floats(
        Array(Array(Float.PositiveInfinity, 0f)),
        1,
        2,
        20L,
        allocator
      )
      val target = new IndexSearch.Target {
        def segmentId: Long = 1L
        def rows: Long = 1L
        def dimension: Int = 2
        def metric: MetricType = MetricType.L2
        def indexType: String = "FLAT"
        def family: String = "FLAT"
        def bytes: Long = 0L
        def labelsOf(excluded: BitSet): BitSet = excluded
        def close(): Unit = ()
        def rowOf(label: Long): Long = {
          if (block.get()) {
            collecting.countDown()
            assert(release.await(10, TimeUnit.SECONDS))
          }
          label
        }
        def search(
            queries: ByteBuffer,
            queryRows: Long,
            topK: Int,
            excluded: ByteBuffer,
            ids: ByteBuffer,
            scores: ByteBuffer,
            parameters: String
        ): Unit = (0 until queryRows.toInt).foreach { query =>
          ids.putLong(query * 8, 0L)
          scores.putFloat(query * 4, 2f)
        }
      }
      val pipeline = new IndexSearch.Pipeline(
        target,
        new BitSet(),
        1,
        Map.empty,
        allocator,
        8,
        threads,
        collector = Some(collector)
      )
      val merger = new TopKMerger(8, 1, MetricType.L2)
      try {
        block.set(true)
        pipeline.run(matrix, merger, _ => ())
        assert(collecting.await(5, TimeUnit.SECONDS))
        val exact = caller.submit[Unit] { () =>
          val scoringThread = Thread.currentThread()
          val function = new RankingFunction {
            def score(
                query: Array[Float],
                row: Array[Float]
            ): JavaFloat = {
              Thread.currentThread() shouldBe scoringThread
              scored.countDown()
              JavaFloat.valueOf(1.0f)
            }
          }
          ExactScan.batch(
            batch,
            matrix,
            2L,
            1,
            MetricType.L2,
            allocator,
            merger,
            _ => Thread.currentThread() shouldBe scoringThread,
            Some(EngineRange.Batch.of(batch, layout, MetricType.L2, function)),
            Some(collector),
            threads
          )
        }
        assert(scored.await(5, TimeUnit.SECONDS))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (collector.getQueue.isEmpty && System.nanoTime() < deadline)
          Thread.sleep(1)
        collector.getQueue.isEmpty shouldBe false
        exact.isDone shouldBe false
        // Index collection is blocked before its first write; exact collection
        // is queued behind it, so neither may have changed the heap yet.
        merger.size shouldBe 0
        release.countDown()
        exact.get(5, TimeUnit.SECONDS)
        pipeline.finish(_ => ())
        (0 until 8).foreach { query =>
          merger
            .sort(query)
            .results(query)
            .map(c => (c.segmentId, c.rowOffset, c.score)) shouldBe
            Seq((2L, 20L, 1.0f))
        }
        merger.scannedRows shouldBe 1L
      } finally {
        release.countDown()
        SearchThreads.shutdown(caller)
        pipeline.close()
        SearchThreads.shutdown(collector)
        batch.close()
        matrix.close()
        allocator.close()
      }
    }
  }

  test(
    "function candidates cross batch and query boundaries without losing rows"
  ) {
    val allocator = new RootAllocator()
    val collector = Executors.newSingleThreadExecutor()
    val vectors = Array.tabulate(5003) { row =>
      if (row % 19 == 0) null else Array(Float.PositiveInfinity, row.toFloat)
    }
    val batch =
      VectorBatchFixture.floats(vectors, vectors.length, 2, 100L, allocator)
    val queries = Seq(-1f, 42f, 3000f, 4999f)
    val matrix =
      QueryMatrix.ofFloats(queries.map(q => Array(q, 0f)), layout, allocator)
    val function = new RankingFunction {
      def score(query: Array[Float], row: Array[Float]): JavaFloat =
        if (query(0) < 0 || row(1).toInt % 13 == 0) null
        else if (row(1).toInt % 17 == 0) JavaFloat.valueOf(Float.NaN)
        else JavaFloat.valueOf(math.abs(query(0).toDouble - row(1)).toFloat)
    }
    val ranked = EngineRange.Batch.of(batch, layout, MetricType.L2, function)
    try {
      ranked.engineRows shouldBe 0
      Seq(1, 4).foreach { threads =>
        val merger = new TopKMerger(queries.size, 10, MetricType.L2)
        var compared = 0L
        ExactScan.batch(
          batch,
          matrix,
          7L,
          10,
          MetricType.L2,
          allocator,
          merger,
          step => compared += step.compared,
          Some(ranked),
          Some(collector),
          threads
        )
        queries.zipWithIndex.foreach { case (query, at) =>
          val expected =
            if (query < 0) Seq.empty[(Float, Long)]
            else
              vectors.indices
                .filter { row =>
                  vectors(row) != null && row % 13 != 0 && row % 17 != 0
                }
                .map { row =>
                  (math.abs(query.toDouble - row).toFloat, 100L + row)
                }
                .sorted
                .take(10)
          merger
            .sort(at)
            .results(at)
            .map(c => (c.score, c.rowOffset)) shouldBe expected
        }
        merger.scannedRows shouldBe batch.visibleRows.toLong
        compared shouldBe queries.size.toLong * ranked.rankedRows.length
      }
    } finally {
      SearchThreads.shutdown(collector)
      matrix.close()
      batch.close()
      allocator.close()
    }
  }
}
