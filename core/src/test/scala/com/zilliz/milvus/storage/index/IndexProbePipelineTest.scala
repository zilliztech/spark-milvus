package com.zilliz.milvus.storage.index

import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.BitSet

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.read.exec.IndexRowMapping
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

/** A segment's groups through the probe pipeline: the answer of one group is
  * checked and collected while the index searches the next, and the mergers end
  * up exactly as a sequential probe would leave them.
  */
class IndexProbePipelineTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private val allocator = new RootAllocator(Long.MaxValue)
  private val layout = VectorLayout(VectorElementType.Float32, 2)
  private val rows = 1000L
  private val k = 4

  override def afterAll(): Unit = allocator.close()

  private def matrix(queries: Int, first: Int): QueryMatrix = {
    val vectors = (0 until queries).map(index =>
      Array((first + index).toFloat, -(first + index).toFloat)
    )
    QueryMatrix.ofPacked(
      QueryMatrix.packFloats(vectors, layout),
      0,
      queries,
      layout,
      allocator
    )
  }

  /** A deterministic index: query q's hit s is row (q * 31 + s * 7 + segment)
    * mod rows with score s + q / 1000, best first. `shortUntilEf` makes the
    * last hit of query 0 missing until the search is at least that wide.
    */
  private final class Fake(
      val segmentId: Long,
      shortUntilEf: Int = 0,
      badRow: Boolean = false
  ) extends IndexProbe.Target {
    val calls = new AtomicInteger
    var lastParameters = ""
    var lastExcluded: Option[ByteBuffer] = None
    def rows: Long = IndexProbePipelineTest.this.rows
    def dimension: Int = 2
    def metric: String = "L2"
    def indexType: String = "HNSW"
    def family: String = "HNSW"
    def mapping: IndexRowMapping = IndexRowMapping.identity(rows)
    def rowOf(query: Int, slot: Int, first: Int): Long =
      ((first + query).toLong * 31L + slot * 7L + segmentId) % rows
    def scoreOf(query: Int, slot: Int, first: Int): Float =
      slot + (first + query) / 1000f
    def search(
        queries: ByteBuffer,
        queryRows: Long,
        topK: Int,
        excluded: ByteBuffer,
        ids: ByteBuffer,
        scores: ByteBuffer,
        parameters: String
    ): Unit = {
      calls.incrementAndGet()
      lastParameters = parameters
      lastExcluded = Option(excluded).map(_.duplicate())
      val ef = "\"ef\":(\\d+)".r
        .findFirstMatchIn(parameters)
        .map(_.group(1).toInt)
        .getOrElse(0)
      // the first query's first value is its number in the whole set
      val q0 = queries.getFloat(0).toInt
      (0 until queryRows.toInt).foreach { query =>
        (0 until topK).foreach { slot =>
          val at = (query * topK + slot).toLong
          val short = query == 0 && slot == topK - 1 && ef < shortUntilEf
          val id =
            if (short) -1L
            else if (badRow && query == 1 && slot == 0) rows + 5
            else rowOf(query, slot, q0)
          ids.putLong((at * 8).toInt, id)
          scores.putFloat((at * 4).toInt, scoreOf(query, slot, q0))
        }
      }
    }
  }

  private def progress()
      : (SegmentSearch.Progress => Unit, () => (Int, Long)) = {
    var calls = 0
    var nanos = 0L
    (
      step => { calls += step.nativeCalls; nanos += step.nativeNanos },
      () => (calls, nanos)
    )
  }

  test("pipelined groups leave the mergers as a sequential probe would") {
    val groups = Seq(7, 5, 6, 8, 3)
    val starts = groups.scanLeft(0)(_ + _)
    val matrices =
      groups.zip(starts).map { case (n, first) => matrix(n, first) }
    try {
      // Expected: top-k across three segments, computed from the fake alone.
      val segments = Seq(11L, 22L, 33L)
      def expected(group: Int): Seq[Seq[(Double, Long, Long)]] =
        (0 until groups(group)).map { query =>
          segments
            .flatMap { segment =>
              val fake = new Fake(segment)
              (0 until k).map(slot =>
                (
                  fake.scoreOf(query, slot, starts(group)).toDouble,
                  segment,
                  fake.rowOf(query, slot, starts(group))
                )
              )
            }
            .sorted
            .take(k)
        }
      val (onProgress, seen) = progress()
      val mergers = groups.map(n => new TopKMerger(n, k, "L2"))
      segments.foreach { segment =>
        val fake = new Fake(segment)
        val pipeline =
          new IndexProbe.Pipeline(
            fake,
            new BitSet(),
            k,
            Map.empty,
            allocator,
            groups.max
          )
        try {
          matrices.zip(mergers).zipWithIndex.foreach { case ((m, merger), g) =>
            pipeline.run(m, merger, onProgress)
          }
          pipeline.finish(onProgress)
        } finally pipeline.close()
        fake.calls.get() shouldBe groups.size
      }
      mergers.zipWithIndex.foreach { case (merger, g) =>
        (0 until groups(g)).foreach { query =>
          val got = merger
            .results(query)
            .map(c => (c.score, c.segmentId, c.rowOffset))
          got shouldBe expected(g)(query)
        }
      }
      seen()._1 shouldBe segments.size * groups.size
    } finally matrices.foreach(_.close())
  }

  test("a short answer is searched again wider on the worker and reported") {
    val m = matrix(3, 0)
    try {
      val fake = new Fake(5L, shortUntilEf = 200)
      val merger = new TopKMerger(3, k, "L2")
      val (onProgress, seen) = progress()
      val pipeline =
        new IndexProbe.Pipeline(fake, new BitSet(), k, Map.empty, allocator, 3)
      try {
        pipeline.run(m, merger, onProgress)
        pipeline.finish(onProgress)
      } finally pipeline.close()
      // ef 64 -> 128 -> 256: two retries, both on the worker, reported at finish
      fake.calls.get() shouldBe 3
      fake.lastParameters should include("\"ef\":256")
      seen()._1 shouldBe 3
      merger.size shouldBe 3 * k
    } finally m.close()
  }

  test("a bad answer fails the task where the pipeline is next used") {
    val m = matrix(3, 0)
    try {
      val fake = new Fake(9L, badRow = true)
      val merger = new TopKMerger(3, k, "L2")
      val pipeline =
        new IndexProbe.Pipeline(fake, new BitSet(), k, Map.empty, allocator, 3)
      try {
        pipeline.run(m, merger, _ => ())
        val failure =
          the[IllegalArgumentException] thrownBy pipeline.finish(_ => ())
        failure.getMessage should include("outside its 1000 rows")
      } finally pipeline.close()
    } finally m.close()
  }

  test("no excluded row passes no bitmap; an excluded row passes one") {
    val m = matrix(2, 0)
    try {
      val fake = new Fake(1L)
      val merger = new TopKMerger(2, k, "L2")
      val pipeline =
        new IndexProbe.Pipeline(fake, new BitSet(), k, Map.empty, allocator, 2)
      try {
        pipeline.run(m, merger, _ => ())
        pipeline.finish(_ => ())
      } finally pipeline.close()
      fake.calls.get() shouldBe 1
      // as Milvus does without a filter: the index searches unfiltered
      fake.lastExcluded shouldBe None

      // row 999 is never a hit of these queries; excluding it must reach the
      // index as a bitmap with that bit alone set
      val excluded = new BitSet()
      excluded.set(999)
      val filtered = new TopKMerger(2, k, "L2")
      val withMask =
        new IndexProbe.Pipeline(fake, excluded, k, Map.empty, allocator, 2)
      try {
        withMask.run(m, filtered, _ => ())
        withMask.finish(_ => ())
      } finally withMask.close()
      val mask = fake.lastExcluded.getOrElse(fail("no bitmap was passed"))
      mask.remaining() shouldBe ((rows + 7) / 8).toInt
      (0 until mask.remaining()).count(mask.get(_) != 0) shouldBe 1
      (mask.get(999 / 8) & 0xff) shouldBe (1 << (999 % 8))
      filtered.size shouldBe 2 * k
    } finally m.close()
  }

  test("a segment with nothing visible searches nothing") {
    val m = matrix(2, 0)
    try {
      val fake = new Fake(1L)
      val excluded = new BitSet()
      (0 until rows.toInt).foreach(excluded.set)
      val pipeline =
        new IndexProbe.Pipeline(fake, excluded, k, Map.empty, allocator, 2)
      try {
        pipeline.count shouldBe 0
        val merger = new TopKMerger(2, k, "L2")
        pipeline.run(m, merger, _ => ())
        pipeline.finish(_ => ())
        merger.size shouldBe 0
        fake.calls.get() shouldBe 0
      } finally pipeline.close()
    } finally m.close()
  }
}

/** Sources opened one ahead of their use, and closed if never used. */
class PrefetcherTest extends AnyFunSuite with Matchers {

  private final class Source(val id: Long, val openedAt: Long)
      extends AutoCloseable {
    var closed = false
    def close(): Unit = closed = true
  }

  test("with prefetch the next source opens while the current one is in use") {
    val opened = new java.util.concurrent.ConcurrentLinkedQueue[Long]()
    val inUse = new CountDownLatch(1)
    val prefetcher = new SegmentSearch.Prefetcher[Source](
      Seq(1L, 2L, 3L),
      id => { opened.add(id); new Source(id, System.nanoTime()) },
      prefetch = true
    )
    val seen = scala.collection.mutable.ArrayBuffer.empty[Long]
    try
      prefetcher.foreach { source =>
        if (source.id == 1L) {
          // the second source's open is submitted before body(1) runs
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
          while (!opened.contains(2L) && System.nanoTime() < deadline)
            Thread.sleep(5)
          opened.contains(2L) shouldBe true
          inUse.countDown()
        }
        seen += source.id
        source.close()
      }
    finally prefetcher.close()
    seen.toSeq shouldBe Seq(1L, 2L, 3L)
    opened.size shouldBe 3
  }

  test("without prefetch sources open in turn") {
    val opened = scala.collection.mutable.ArrayBuffer.empty[Long]
    val prefetcher = new SegmentSearch.Prefetcher[Source](
      Seq(1L, 2L),
      id => { opened += id; new Source(id, 0L) },
      prefetch = false
    )
    try
      prefetcher.foreach { source =>
        opened.toSeq shouldBe (1L to source.id)
        source.close()
      }
    finally prefetcher.close()
  }

  test(
    "a source opened ahead but never used is closed, and an open failure surfaces at its turn"
  ) {
    val sources = scala.collection.mutable.ArrayBuffer.empty[Source]
    val prefetcher = new SegmentSearch.Prefetcher[Source](
      Seq(1L, 2L, 3L),
      id => {
        if (id == 3L) throw new IllegalStateException("no segment 3")
        val s = new Source(id, 0L); sources += s; s
      },
      prefetch = true
    )
    val failure = the[RuntimeException] thrownBy {
      try
        prefetcher.foreach { source =>
          if (source.id == 1L) throw new RuntimeException("stop after one")
        }
      finally prefetcher.close()
    }
    // body(1) failed with our own exception; close() must still have taken and
    // closed the source opened ahead (2) without turning that into a failure
    failure.getMessage shouldBe "stop after one"
    sources.map(_.id) should contain(2L)
    sources.find(_.id == 2L).get.closed shouldBe true
  }
}
