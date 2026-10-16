package com.zilliz.spark.connector.search

import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.BitSet

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.milvus.storage.vector.{
  CandidateBytes,
  CandidateDictionary,
  CandidateEncoding,
  IndexSearch,
  QueryMatrix,
  SearchThreads,
  SegmentSearch,
  TopKMerger
}

/** Actual held-index traversal, collection and packing with deterministic index
  * answers. No JNI library or external storage is needed.
  */
class SearchGroupPipelineHeldTest extends AnyFunSuite with Matchers {

  test(
    "held search packs identical answers while the next group uses its workspace"
  ) {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val k = 3
    val groups = Vector(Vector(10f, 11f), Vector(20f, 21f, 22f), Vector(30f))

    def run(
        pipelined: Boolean,
        encoding: CandidateEncoding
    ): Vector[Vector[Vector[Byte]]] = {
      val allocator = new RootAllocator()
      val closes = new AtomicInteger
      val searches = new AtomicInteger
      val secondStarted = new CountDownLatch(1)
      val firstPacked = new CountDownLatch(1)
      def target(id: Long): IndexSearch.Target = new IndexSearch.Target {
        private val closed = new AtomicInteger
        def segmentId: Long = id
        def rows: Long = 4L
        def dimension: Int = 2
        def metric: MetricType = MetricType.L2
        def indexType: String = "FLAT"
        def family: String = "FLAT"
        def bytes: Long = 64L
        def rowOf(label: Long): Long = {
          closed.get() shouldBe 0
          label
        }
        def labelsOf(excluded: BitSet): BitSet = excluded
        def search(
            queries: ByteBuffer,
            queryRows: Long,
            topK: Int,
            excluded: ByteBuffer,
            ids: ByteBuffer,
            scores: ByteBuffer,
            parameters: String
        ): Unit = {
          closed.get() shouldBe 0
          searches.incrementAndGet()
          if (pipelined && id == 7L && queries.getFloat(0) == 20f) {
            secondStarted.countDown()
            firstPacked.await(10, TimeUnit.SECONDS) shouldBe true
          }
          (0 until queryRows.toInt).foreach { query =>
            val value = queries.getFloat(query * layout.rowBytes).toInt
            (0 until topK).foreach { slot =>
              val at = query * topK + slot
              ids.putLong(at * 8, (value + slot) % rows)
              scores.putFloat(at * 4, slot + id.toFloat / 10f)
            }
          }
        }
        def close(): Unit = {
          closed.incrementAndGet() shouldBe 1
          closes.incrementAndGet()
          ()
        }
      }
      val held = SegmentSearch
        .hold(
          Seq(7L, 9L),
          id => SegmentSearch.Index(id, target(id), new BitSet()),
          1024L,
          layout,
          MetricType.L2,
          callsInFlight = 2
        )
        .toOption
        .get
      def search(values: Vector[Float]): TopKMerger = {
        val matrix = QueryMatrix.ofFloats(
          values.map(value => Array(value, 0f)),
          layout,
          allocator
        )
        try {
          val (merger, counters) = held.search(
            matrix,
            k,
            MetricType.L2,
            Map.empty,
            allocator,
            threads = 2
          )
          counters.segments shouldBe 2
          merger
        } finally matrix.close()
      }
      var released = false
      def release(): Unit = if (!released) {
        released = true
        held.close()
        allocator.getAllocatedMemory shouldBe 0L
        allocator.close()
      }
      val pipeline =
        if (pipelined)
          Some(
            new SearchGroupPipeline(groups.iterator, search, () => release())
          )
        else None
      val mergers = pipeline.getOrElse(groups.iterator.map(search))
      val result =
        try {
          val packed = mergers.zipWithIndex.map { case (merger, index) =>
            if (pipelined && index == 0)
              secondStarted.await(10, TimeUnit.SECONDS) shouldBe true
            val result = SearchThreads
              .shards(merger.queries, 2) { (_, from, until) =>
                (from until until).map { query =>
                  merger.sort(query)
                  merger.takePacked(query, encoding).toVector
                }
              }
              .flatten
              .toVector
            if (index == 0) firstPacked.countDown()
            result
          }.toVector
          searches.get() shouldBe groups.size * 2
          packed
        } finally {
          firstPacked.countDown()
          try pipeline.foreach(_.close())
          finally release()
        }
      closes.get() shouldBe 2
      result
    }

    val dictionary = CandidateDictionary(Seq(7L, 9L))
    val encoding = CandidateEncoding(dictionary, Seq(7L, 9L))
    val serial = run(pipelined = false, encoding = encoding)
    serial.map(_.size) shouldBe Vector(2, 3, 1)
    serial.flatten.foreach(_.size shouldBe k * CandidateBytes.Width)
    run(pipelined = true, encoding = encoding) shouldBe serial
  }
}
