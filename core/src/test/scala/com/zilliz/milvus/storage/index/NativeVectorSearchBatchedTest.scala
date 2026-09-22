package com.zilliz.milvus.storage.index

import java.nio.ByteOrder
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.Random

import org.apache.arrow.memory.{ArrowBuf, RootAllocator}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.vector.{NativeVectorLibrary, NativeVectorSearch}

import io.knowhere.DType

/** The batched distance entry answers the same queries as Knowhere's per-query
  * brute force: the contract of vector-search.html section 2.2 for float32,
  * checked against the real library on both sides of faiss's BLAS threshold
  * (decision 27).
  */
class NativeVectorSearchBatchedTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var available = true

  override def beforeAll(): Unit =
    try NativeVectorLibrary.load()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError =>
        available = false
      case _: RuntimeException => available = false
    }

  private def buffers(body: RootAllocator => Unit): Unit = {
    assume(available, "the Knowhere native library is not on this machine")
    val allocator = new RootAllocator()
    try body(allocator)
    finally allocator.close()
  }

  private def randomFloats(
      allocator: RootAllocator,
      count: Int,
      random: Random
  ): ArrowBuf = {
    val buffer = allocator.buffer(count.toLong * 4L)
    (0 until count).foreach { index =>
      buffer.setFloat(index.toLong * 4L, random.nextGaussian().toFloat)
    }
    buffer
  }

  private def bytes(buffer: ArrowBuf, length: Long) =
    buffer.nioBuffer(0, length.toInt).order(ByteOrder.nativeOrder())

  private def ids(buffer: ArrowBuf, queries: Int, k: Int): Seq[Seq[Long]] =
    (0 until queries).map { query =>
      (0 until k).map(slot => buffer.getLong((query.toLong * k + slot) * 8L))
    }

  private def scores(buffer: ArrowBuf, queries: Int, k: Int): Seq[Seq[Float]] =
    (0 until queries).map { query =>
      (0 until k).map(slot => buffer.getFloat((query.toLong * k + slot) * 4L))
    }

  /** Both entries over the same random base and queries; the sets of ids per
    * query must agree, and the scores of the same ids must agree to float
    * rounding. Random gaussian values make ties improbable.
    */
  private def agree(metric: String, queries: Int, rows: Int, k: Int): Unit =
    buffers { allocator =>
      val dimension = 24
      val random = new Random(7L + queries)
      val base = randomFloats(allocator, rows * dimension, random)
      val query = randomFloats(allocator, queries * dimension, random)
      val perQueryIds = allocator.buffer(queries.toLong * k * 8L)
      val perQueryScores = allocator.buffer(queries.toLong * k * 4L)
      val batchedIds = allocator.buffer(queries.toLong * k * 8L)
      val batchedScores = allocator.buffer(queries.toLong * k * 4L)
      try {
        val parameters = s"""{"metric_type":"$metric"}"""
        NativeVectorSearch.bruteForce(
          DType.FLOAT32,
          bytes(base, base.capacity()),
          rows.toLong,
          bytes(query, query.capacity()),
          queries.toLong,
          dimension,
          k,
          null,
          bytes(perQueryIds, perQueryIds.capacity()),
          bytes(perQueryScores, perQueryScores.capacity()),
          parameters
        )
        NativeVectorSearch.bruteForceBatched(
          DType.FLOAT32,
          bytes(base, base.capacity()),
          rows.toLong,
          bytes(query, query.capacity()),
          queries.toLong,
          dimension,
          k,
          bytes(batchedIds, batchedIds.capacity()),
          bytes(batchedScores, batchedScores.capacity()),
          parameters
        )
        val expectedIds = ids(perQueryIds, queries, k)
        val actualIds = ids(batchedIds, queries, k)
        val expectedScores = scores(perQueryScores, queries, k)
        val actualScores = scores(batchedScores, queries, k)
        (0 until queries).foreach { q =>
          withClue(s"$metric query $q: ") {
            actualIds(q).toSet shouldBe expectedIds(q).toSet
            val expectedById = expectedIds(q).zip(expectedScores(q)).toMap
            actualIds(q).zip(actualScores(q)).foreach { case (id, score) =>
              score shouldBe expectedById(id) +- 1e-3f
            }
          }
        }
      } finally {
        batchedScores.close()
        batchedIds.close()
        perQueryScores.close()
        perQueryIds.close()
        query.close()
        base.close()
      }
    }

  test("L2 agrees with the per-query entry above the BLAS threshold") {
    agree("L2", queries = 64, rows = 300, k = 5)
  }

  test("L2 agrees with the per-query entry below the BLAS threshold") {
    agree("L2", queries = 3, rows = 300, k = 5)
  }

  test("IP and COSINE agree with the per-query entry") {
    agree("IP", queries = 40, rows = 200, k = 4)
    agree("COSINE", queries = 40, rows = 200, k = 4)
  }

  test("callers on several threads at once get what one caller gets") {
    buffers { allocator =>
      val dimension = 32
      val rows = 2000
      val queries = 48
      val k = 8
      val random = new Random(11L)
      val base = randomFloats(allocator, rows * dimension, random)
      val query = randomFloats(allocator, queries * dimension, random)
      def search(): (Seq[Seq[Long]], Seq[Seq[Float]]) = {
        val outIds = allocator.buffer(queries.toLong * k * 8L)
        val outScores = allocator.buffer(queries.toLong * k * 4L)
        try {
          NativeVectorSearch.bruteForceBatched(
            DType.FLOAT32,
            bytes(base, base.capacity()),
            rows.toLong,
            bytes(query, query.capacity()),
            queries.toLong,
            dimension,
            k,
            bytes(outIds, outIds.capacity()),
            bytes(outScores, outScores.capacity()),
            """{"metric_type":"L2"}"""
          )
          (ids(outIds, queries, k), scores(outScores, queries, k))
        } finally {
          outScores.close()
          outIds.close()
        }
      }
      try {
        val expected = search()
        val results =
          new ConcurrentLinkedQueue[(Seq[Seq[Long]], Seq[Seq[Float]])]()
        val failures = new ConcurrentLinkedQueue[Throwable]()
        val threads = (0 until 6).map { _ =>
          new Thread(() =>
            try (0 until 20).foreach(_ => results.add(search()))
            catch { case failure: Throwable => failures.add(failure) }
          )
        }
        threads.foreach(_.start())
        threads.foreach(_.join())
        failures.isEmpty shouldBe true
        results.size shouldBe 120
        results.forEach(result => result shouldBe expected)
      } finally {
        query.close()
        base.close()
      }
    }
  }

  test("fewer rows than k leave the tail at -1") {
    buffers { allocator =>
      val dimension = 2
      val base = allocator.buffer(2L * dimension * 4L)
      base.setFloat(0L, 0f); base.setFloat(4L, 0f)
      base.setFloat(8L, 3f); base.setFloat(12L, 4f)
      val query = allocator.buffer(dimension * 4L)
      query.setFloat(0L, 0f); query.setFloat(4L, 0f)
      val k = 4
      val outIds = allocator.buffer(k * 8L)
      val outScores = allocator.buffer(k * 4L)
      try {
        NativeVectorSearch.bruteForceBatched(
          DType.FLOAT32,
          bytes(base, base.capacity()),
          2L,
          bytes(query, query.capacity()),
          1L,
          dimension,
          k,
          bytes(outIds, outIds.capacity()),
          bytes(outScores, outScores.capacity()),
          """{"metric_type":"L2"}"""
        )
        ids(outIds, 1, k).head shouldBe Seq(0L, 1L, -1L, -1L)
        scores(outScores, 1, k).head.take(2) shouldBe Seq(0f, 25f)
      } finally {
        outScores.close()
        outIds.close()
        query.close()
        base.close()
      }
    }
  }

  test("a metric the batched entry does not take is refused") {
    buffers { allocator =>
      val base = allocator.buffer(8L)
      val query = allocator.buffer(8L)
      val outIds = allocator.buffer(8L)
      val outScores = allocator.buffer(4L)
      try
        an[Exception] should be thrownBy NativeVectorSearch.bruteForceBatched(
          DType.FLOAT32,
          bytes(base, 8L),
          1L,
          bytes(query, 8L),
          1L,
          2,
          1,
          bytes(outIds, 8L),
          bytes(outScores, 4L),
          """{"metric_type":"HAMMING"}"""
        )
      finally {
        outScores.close()
        outIds.close()
        query.close()
        base.close()
      }
    }
  }
}
