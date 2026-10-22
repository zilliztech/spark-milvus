package com.zilliz.milvus.storage.vector

import java.lang.{Float => JavaFloat}
import java.nio.ByteOrder
import java.util.{BitSet, Random}
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}

/** The compaction a batch with excluded rows goes through before the batched
  * distance entry, which takes no bitmap, and the L2 the batched entry only
  * nominates candidates for.
  */
class ExactScanTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var native = true

  override def beforeAll(): Unit =
    try NativeVectorLibrary.load()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        native = false
      case _: RuntimeException => native = false
    }

  /** L2 as the connector's kernel computes it, the squared distance summed in
    * Double, then rounded to the Float score used by every selection stage.
    */
  private object SquaredDistance extends RankingFunction {
    override def score(
        query: Array[Float],
        row: Array[Float]
    ): JavaFloat = {
      var sum = 0.0
      var at = 0
      while (at < query.length) {
        val difference = query(at).toDouble - row(at).toDouble
        sum += difference * difference
        at += 1
      }
      JavaFloat.valueOf(sum.toFloat)
    }
  }

  /** Searches one batch for every query under L2, ranked by the function, and
    * gives each query's kept (row, score) pairs and the rescans counted.
    */
  private def searchL2(
      vectors: Array[Array[Float]],
      queries: Seq[Array[Float]],
      k: Int,
      allocator: RootAllocator
  ): (Seq[Seq[(Long, Float)]], Long) = {
    val dimension = vectors.head.length
    val layout = VectorLayout(VectorElementType.Float32, dimension)
    val batch =
      VectorBatchFixture.floats(
        vectors,
        vectors.length,
        dimension,
        0L,
        allocator
      )
    try {
      val matrix = QueryMatrix.ofFloats(queries, layout, allocator)
      try {
        def collected(
            collector: Option[ExecutorService],
            threads: Int
        ): (Seq[Seq[(Long, Float)]], Long) = {
          val merger = new TopKMerger(queries.size, k, MetricType.L2)
          var rescans = 0L
          ExactScan.batch(
            batch,
            matrix,
            7L,
            k,
            MetricType.L2,
            allocator,
            merger,
            step => rescans += step.rescans,
            Some(
              EngineRange.Batch
                .of(batch, layout, MetricType.L2, SquaredDistance)
            ),
            collector,
            threads
          )
          (
            queries.indices.map(query =>
              merger.sort(query).results(query).map(c => (c.rowOffset, c.score))
            ),
            rescans
          )
        }
        val sequential = collected(None, 1)
        val collector = Executors.newSingleThreadExecutor()
        try {
          val parallel = collected(Some(collector), 4)
          parallel shouldBe sequential
          parallel
        } finally SearchThreads.shutdown(collector)
      } finally matrix.close()
    } finally batch.close()
  }

  private val rowBytes = 8

  private def withAllocator(body: RootAllocator => Unit): Unit = {
    val allocator = new RootAllocator()
    try body(allocator)
    finally allocator.close()
  }

  /** Row `i` holds the two floats (i, -i). */
  private def rows(allocator: RootAllocator, count: Int) = {
    val buffer = allocator.buffer(count.toLong * rowBytes)
    (0 until count).foreach { row =>
      buffer.setFloat(row.toLong * rowBytes, row.toFloat)
      buffer.setFloat(row.toLong * rowBytes + 4L, -row.toFloat)
    }
    buffer
  }

  private def excluded(rows: Int*): BitSet = {
    val set = new BitSet()
    rows.foreach(set.set)
    set
  }

  test("the visible rows come out in order, each mapped to its batch row") {
    withAllocator { allocator =>
      val source = rows(allocator, 8)
      val compacted = ExactScan.compact(
        source.nioBuffer(0, 8 * rowBytes).order(ByteOrder.nativeOrder()),
        8,
        excluded(0, 3, 4, 7),
        rowBytes,
        allocator
      )
      try {
        compacted.rows shouldBe 4
        compacted.batchRows.toSeq shouldBe Seq(1, 2, 5, 6)
        compacted.buffer.capacity() shouldBe 4L * rowBytes
        (0 until 4).foreach { row =>
          compacted.buffer.getFloat(row.toLong * rowBytes) shouldBe
            compacted.batchRows(row).toFloat
          compacted.buffer.getFloat(row.toLong * rowBytes + 4L) shouldBe
            -compacted.batchRows(row).toFloat
        }
      } finally {
        compacted.close()
        source.close()
      }
    }
  }

  test("a run of kept rows at either end is copied whole") {
    withAllocator { allocator =>
      val source = rows(allocator, 5)
      val compacted = ExactScan.compact(
        source.nioBuffer(0, 5 * rowBytes).order(ByteOrder.nativeOrder()),
        5,
        excluded(2),
        rowBytes,
        allocator
      )
      try {
        compacted.batchRows.toSeq shouldBe Seq(0, 1, 3, 4)
        compacted.buffer.getFloat(3L * rowBytes) shouldBe 4f
      } finally {
        compacted.close()
        source.close()
      }
    }
  }

  test("a batch with every row excluded is refused") {
    withAllocator { allocator =>
      val source = rows(allocator, 2)
      try
        an[IllegalArgumentException] should be thrownBy ExactScan.compact(
          source.nioBuffer(0, 2 * rowBytes).order(ByteOrder.nativeOrder()),
          2,
          excluded(0, 1),
          rowBytes,
          allocator
        )
      finally source.close()
    }
  }

  test("an L2 batch asks for spare candidates only while it may need them") {
    // Once a batch is merged, about k times its rows over all the rows scanned
    // are among the best k: k for a first batch, k / 2 for a second as large.
    ExactScan.nominated(100, 10000, 0L) shouldBe 138
    ExactScan.nominated(100, 10000, 10000L) shouldBe 100
    ExactScan.nominated(100, 10000, 1000000L) shouldBe 100
    // A small k keeps a few to spare for longer.
    ExactScan.nominated(10, 10000, 10000L) shouldBe 20
    ExactScan.nominated(10, 10000, 90000L) shouldBe 12
    // At most k + min(k, 64).
    ExactScan.nominated(1, 10000, 0L) shouldBe 2
    ExactScan.nominated(10, 10000, 0L) shouldBe 20
    ExactScan.nominated(1000, 10000, 0L) shouldBe 1064
  }

  test("L2 far from the origin keeps the rows the batched entry cannot rank") {
    assume(native, "the Knowhere native library is not on this machine")
    withAllocator { allocator =>
      // Six rows near a point at 10^6 in each of 16 dimensions, at squared
      // distances 25, 25, 0.25, 2500, 3600 and 4900 from it: |x|^2 + |y|^2 -
      // 2x.y in float32 is 1.6 * 10^13 give or take 10^6, and cannot tell
      // them apart.
      val center = Array.fill(16)(1000000f)
      def moved(at: Int, by: Float) = {
        val vector = center.clone()
        vector(at) += by
        vector
      }
      val vectors = Array(
        moved(0, 5f),
        moved(1, 5f),
        moved(2, 0.5f),
        moved(3, 50f),
        moved(4, 60f),
        moved(5, 70f)
      )
      // Below 20 queries faiss computes each pair directly, from 20 on as
      // |x|^2 + |y|^2 - 2x.y; the bound cannot show either kept, so every query
      // is searched again pair by pair.
      for (count <- Seq(19, 20, 64)) withClue(s"$count queries: ") {
        val (kept, rescans) =
          searchL2(vectors, Seq.fill(count)(center), k = 2, allocator)
        kept.distinct shouldBe Seq(Seq((2L, 0.25f), (0L, 25.0f)))
        rescans shouldBe count.toLong
      }
    }
  }

  test("the queries searched again are read where they lie in the matrix") {
    assume(native, "the Knowhere native library is not on this machine")
    withAllocator { allocator =>
      // The six rows of the test above and two near the origin. A query at the
      // origin is shown its two without a second search; a query at the far
      // point is not, and is searched again.
      val center = Array.fill(16)(1000000f)
      val origin = new Array[Float](16)
      def moved(from: Array[Float], at: Int, by: Float) = {
        val vector = from.clone()
        vector(at) += by
        vector
      }
      val vectors = Array(
        moved(center, 0, 5f),
        moved(center, 1, 5f),
        moved(center, 2, 0.5f),
        moved(center, 3, 50f),
        moved(center, 4, 60f),
        moved(center, 5, 70f),
        moved(origin, 0, 1f),
        moved(origin, 1, 100f)
      )
      // Every third query is at the origin, so the others are searched again
      // two at a time, from places along the matrix.
      val queries = (0 until 24).map(at => if (at % 3 == 2) origin else center)
      val (kept, rescans) = searchL2(vectors, queries, k = 2, allocator)
      rescans shouldBe 16L
      kept.zip(queries).foreach { case (rows, query) =>
        if (query eq origin) rows shouldBe Seq((6L, 1.0f), (7L, 10000.0f))
        else rows shouldBe Seq((2L, 0.25f), (0L, 25.0f))
      }
    }
  }

  test("a query whose worst kept row scored NaN takes the next batch whole") {
    assume(native, "the Knowhere native library is not on this machine")
    withAllocator { allocator =>
      val layout = VectorLayout(VectorElementType.Float32, 2)
      // The function scores the NaN row NaN, which ranks after every number:
      // with k = 2 the query keeps both rows of the first batch, NaN worst.
      val first = VectorBatchFixture.floats(
        Array(Array(Float.NaN, 0f), Array(1f, 0f)),
        2,
        2,
        0L,
        allocator
      )
      val second = VectorBatchFixture.floats(
        Array(Array(0.5f, 0f), Array(3f, 0f), Array(4f, 0f)),
        3,
        2,
        2L,
        allocator
      )
      val matrix = QueryMatrix.ofFloats(Seq(Array(0f, 0f)), layout, allocator)
      try {
        val merger = new TopKMerger(1, 2, MetricType.L2)
        Seq(first, second).foreach { batch =>
          ExactScan.batch(
            batch,
            matrix,
            7L,
            2,
            MetricType.L2,
            allocator,
            merger,
            _ => (),
            Some(
              EngineRange.Batch
                .of(batch, layout, MetricType.L2, SquaredDistance)
            )
          )
        }
        merger.sort(0).results(0).map(c => (c.rowOffset, c.score)) shouldBe
          Seq((2L, 0.25f), (1L, 1.0f))
      } finally {
        matrix.close()
        second.close()
        first.close()
      }
    }
  }

  test("L2 the batched entry ranks well is kept without a second search") {
    assume(native, "the Knowhere native library is not on this machine")
    withAllocator { allocator =>
      val random = new Random(27L)
      def vector() = Array.fill(32)(random.nextFloat())
      val vectors = Array.fill(2000)(vector())
      val queries = Seq.fill(64)(vector())
      val (kept, rescans) = searchL2(vectors, queries, k = 10, allocator)
      rescans shouldBe 0L
      val exact = queries.map(query =>
        vectors.indices
          .map(row =>
            (row.toLong, SquaredDistance.score(query, vectors(row)).floatValue)
          )
          .sortBy { case (row, score) => (score, row) }
          .take(10)
      )
      kept.map(_.map(_._1)) shouldBe exact.map(_.map(_._1))
      // Knowhere's sums pair by pair, within float rounding of the exact ones.
      kept.flatten.zip(exact.flatten).foreach { case ((_, got), (_, want)) =>
        got shouldBe want +- want * 1e-5f
      }
    }
  }
}
