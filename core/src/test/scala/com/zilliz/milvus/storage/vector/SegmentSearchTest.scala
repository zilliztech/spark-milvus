package com.zilliz.milvus.storage.vector

import java.lang.{Float => JavaFloat}
import java.nio.ByteBuffer
import java.util.BitSet

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}

/** Source ownership and read costs without a Milvus reader or native index. */
class SegmentSearchTest extends AnyFunSuite with Matchers {
  private val layout = VectorLayout(VectorElementType.Float32, 2)
  private val function = new RankingFunction {
    def score(query: Array[Float], row: Array[Float]): JavaFloat =
      JavaFloat.valueOf((query(0).toDouble + row(1).toDouble).toFloat)
  }

  private final class Batch(
      value: Float,
      offset: Long,
      allocator: RootAllocator,
      failClassification: Boolean = false
  ) extends VectorBatch {
    private val owned = VectorBatchFixture.floats(
      Array(Array(Float.PositiveInfinity, value)),
      1,
      2,
      offset,
      allocator
    )
    var closes = 0
    def base: KnowhereBuffers.Base = owned.base
    def excluded: BitSet = {
      if (failClassification)
        throw new IllegalStateException("classification failed")
      owned.excluded
    }
    def firstRow: Long = owned.firstRow
    def rows: Int = owned.rows
    def close(): Unit = {
      closes += 1
      require(closes == 1, "A returned batch is closed once")
      owned.close()
    }
  }

  private final class Stream(val batches: Seq[Batch], failAfter: Int = -1)
      extends VectorBatch.Stream {
    var handed = 0
    var closes = 0
    var samples = 0
    def next(): Option[VectorBatch] = {
      require(closes == 0)
      if (handed == failAfter) throw new IllegalStateException("read failed")
      if (handed == batches.size) None
      else {
        val batch = batches(handed)
        handed += 1
        Some(batch)
      }
    }
    def readCost: VectorBatch.ReadCost = {
      require(closes == 0, "Read cost is sampled before closing the stream")
      samples += 1
      VectorBatch.ReadCost(handed * 13L, handed * 17L)
    }
    def close(): Unit = {
      closes += 1
      require(closes == 1, "A source is closed once")
      batches.drop(handed).foreach(_.close())
    }
  }

  test("held and streamed neutral inputs agree and sample each read once") {
    val allocator = new RootAllocator()
    val groups = Seq(0f, 10f).map(value =>
      QueryMatrix.ofFloats(Seq(Array(value, 0f)), layout, allocator)
    )
    def run(hold: Boolean): Seq[Seq[Candidate]] = {
      val stream = new Stream(
        Seq(
          new Batch(2f, 4L, allocator),
          new Batch(1f, 5L, allocator)
        )
      )
      def open(id: Long): SegmentSearch.Source = SegmentSearch.Exact(id, stream)
      val mergers = if (hold) {
        val held = SegmentSearch
          .hold(
            Seq(7L),
            open,
            1024L,
            layout,
            MetricType.L2,
            Some(function)
          )
          .toOption
          .get
        try {
          held.read shouldBe VectorBatch.ReadCost(26L, 34L)
          stream.batches.foreach(_.closes shouldBe 0)
          groups.map { queries =>
            val (merger, counters) = held.search(
              queries,
              2,
              MetricType.L2,
              Map.empty,
              allocator
            )
            counters.read shouldBe VectorBatch.ReadCost.Zero
            merger
          }
        } finally held.close()
      } else {
        val (found, counters) = SegmentSearch.runGroups(
          Seq(7L),
          open,
          groups,
          2,
          MetricType.L2,
          Map.empty,
          allocator,
          function = Some(function)
        )
        counters.read shouldBe VectorBatch.ReadCost(26L, 34L)
        found
      }
      stream.samples shouldBe 1
      stream.closes shouldBe 1
      stream.batches.foreach(_.closes shouldBe 1)
      mergers.map { merger =>
        merger.sort(0)
        merger.results(0)
      }
    }
    try {
      val streamed = run(hold = false)
      run(hold = true) shouldBe streamed
      streamed.head.map(_.rowOffset) shouldBe Seq(5L, 4L)
    } finally {
      groups.foreach(_.close())
      allocator.close()
    }
  }

  test(
    "hold overflow closes returned and unread batches with its sampled cost"
  ) {
    val allocator = new RootAllocator()
    val stream = new Stream(
      Seq(
        new Batch(2f, 4L, allocator),
        new Batch(1f, 5L, allocator)
      )
    )
    try {
      val result = SegmentSearch.hold(
        Seq(7L),
        id => SegmentSearch.Exact(id, stream),
        1L,
        layout,
        MetricType.L2,
        Some(function)
      )
      result.left.toOption.get.read shouldBe VectorBatch.ReadCost(13L, 17L)
      stream.samples shouldBe 1
      stream.closes shouldBe 1
      stream.batches.foreach(_.closes shouldBe 1)
    } finally allocator.close()
  }

  test("a failed hold releases the batches already returned by its stream") {
    val allocator = new RootAllocator()
    val stream = new Stream(
      Seq(
        new Batch(2f, 4L, allocator),
        new Batch(1f, 5L, allocator)
      ),
      failAfter = 1
    )
    try {
      val failure = intercept[IllegalStateException] {
        SegmentSearch.hold(
          Seq(7L),
          id => SegmentSearch.Exact(id, stream),
          1024L,
          layout,
          MetricType.L2,
          Some(function)
        )
      }
      failure.getMessage shouldBe "read failed"
      stream.closes shouldBe 1
      stream.batches.foreach(_.closes shouldBe 1)
    } finally allocator.close()
  }

  test("index targets map rows and remain open until collection finishes") {
    val allocator = new RootAllocator()
    val matrix = QueryMatrix.ofFloats(Seq(Array(0f, 0f)), layout, allocator)
    var targetCloses = 0
    val target = new IndexSearch.Target {
      def segmentId: Long = 7L
      def rows: Long = 2L
      def dimension: Int = 2
      def metric: MetricType = MetricType.L2
      def indexType: String = "FLAT"
      def family: String = "FLAT"
      def bytes: Long = 64L
      def rowOf(label: Long): Long = {
        require(targetCloses == 0, "Collection borrows an open target")
        label + 40L
      }
      def labelsOf(excluded: BitSet): BitSet = {
        excluded.get(41) shouldBe true
        val labels = new BitSet()
        labels.set(1)
        labels
      }
      def search(
          queries: ByteBuffer,
          queryRows: Long,
          topK: Int,
          excluded: ByteBuffer,
          ids: ByteBuffer,
          scores: ByteBuffer,
          parameters: String
      ): Unit = {
        require(targetCloses == 0)
        topK shouldBe 1
        (excluded.get(0) & 2) shouldBe 2
        ids.putLong(0, 0L)
        scores.putFloat(0, 4f)
      }
      def close(): Unit = {
        targetCloses += 1
        require(targetCloses == 1)
      }
    }
    val excluded = new BitSet()
    excluded.set(41)
    try {
      val (mergers, _) = SegmentSearch.runGroups(
        Seq(7L),
        id => SegmentSearch.Index(id, target, excluded),
        Seq(matrix),
        2,
        MetricType.L2,
        Map.empty,
        allocator
      )
      mergers.head.results(0) shouldBe Seq(Candidate(0, 7L, 40L, 4.0f))
      targetCloses shouldBe 1
    } finally {
      matrix.close()
      allocator.close()
    }
  }

  test("a failed classification releases the batch just handed to a hold") {
    val allocator = new RootAllocator()
    val stream = new Stream(
      Seq(
        new Batch(2f, 4L, allocator, failClassification = true)
      )
    )
    try {
      val failure = intercept[IllegalStateException] {
        SegmentSearch.hold(
          Seq(7L),
          id => SegmentSearch.Exact(id, stream),
          1024L,
          layout,
          MetricType.L2,
          Some(function)
        )
      }
      failure.getMessage shouldBe "classification failed"
      stream.closes shouldBe 1
      stream.batches.head.closes shouldBe 1
    } finally allocator.close()
  }

  test("a ranking failure drains work before the held source is closed") {
    val allocator = new RootAllocator()
    val matrix = QueryMatrix.ofFloats(Seq(Array(0f, 0f)), layout, allocator)
    val stream = new Stream(Seq(new Batch(2f, 4L, allocator)))
    val failing = new RankingFunction {
      def score(query: Array[Float], row: Array[Float]): JavaFloat = {
        stream.closes shouldBe 0
        throw new IllegalStateException("ranking failed")
      }
    }
    try {
      val held = SegmentSearch
        .hold(
          Seq(7L),
          id => SegmentSearch.Exact(id, stream),
          1024L,
          layout,
          MetricType.L2,
          Some(failing)
        )
        .toOption
        .get
      try {
        val failure = intercept[IllegalStateException] {
          held.search(matrix, 1, MetricType.L2, Map.empty, allocator)
        }
        failure.getMessage shouldBe "ranking failed"
      } finally held.close()
      stream.closes shouldBe 1
      stream.batches.head.closes shouldBe 1
    } finally {
      matrix.close()
      allocator.close()
    }
  }
}
