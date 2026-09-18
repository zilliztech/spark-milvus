package com.zilliz.milvus.storage.index

import java.nio.ByteOrder

import org.apache.arrow.memory.{ArrowBuf, RootAllocator}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.vector.{NativeVectorLibrary, NativeVectorSearch}

import io.knowhere.DType

/** One call answers a group of queries: the contract of vector-search.html
  * section 2.2, checked against the real library.
  */
class NativeVectorSearchTest
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

  private def floats(allocator: RootAllocator, values: Seq[Float]): ArrowBuf = {
    val buffer = allocator.buffer(values.size.toLong * 4L)
    values.zipWithIndex.foreach { case (value, index) =>
      buffer.setFloat(index.toLong * 4L, value)
    }
    buffer
  }

  private def bytes(buffer: ArrowBuf, length: Long) =
    buffer.nioBuffer(0, length.toInt).order(ByteOrder.nativeOrder())

  test("two queries come back in one call, each with its own top K") {
    buffers { allocator =>
      val dimension = 2
      val rows = 4
      val topK = 2
      val base =
        floats(allocator, Seq(0f, 0f, 1f, 0f, 10f, 0f, 11f, 0f))
      val queries = floats(allocator, Seq(0.1f, 0f, 10.2f, 0f))
      val ids = allocator.buffer(2L * topK * 8L)
      val distances = allocator.buffer(2L * topK * 4L)
      try {
        NativeVectorSearch.bruteForce(
          DType.FLOAT32,
          bytes(base, rows.toLong * dimension * 4L),
          rows,
          bytes(queries, 2L * dimension * 4L),
          2,
          dimension,
          topK,
          null,
          bytes(ids, 2L * topK * 8L),
          bytes(distances, 2L * topK * 4L),
          """{"metric_type":"L2"}"""
        )
        val returned = (0 until 2 * topK).map(index => ids.getLong(index * 8L))
        returned.take(topK) shouldBe Seq(0L, 1L)
        returned.drop(topK) shouldBe Seq(2L, 3L)
        distances.getFloat(0L) should be < distances.getFloat(4L)
      } finally {
        distances.close()
        ids.close()
        queries.close()
        base.close()
      }
    }
  }

  test("an excluded row is never returned") {
    buffers { allocator =>
      val dimension = 2
      val rows = 3
      val topK = 2
      val base = floats(allocator, Seq(0f, 0f, 1f, 0f, 2f, 0f))
      val queries = floats(allocator, Seq(0f, 0f))
      val excluded = allocator.buffer(1L)
      excluded.setByte(0L, 1) // Row 0, the nearest one.
      val ids = allocator.buffer(topK.toLong * 8L)
      val distances = allocator.buffer(topK.toLong * 4L)
      try {
        NativeVectorSearch.bruteForce(
          DType.FLOAT32,
          bytes(base, rows.toLong * dimension * 4L),
          rows,
          bytes(queries, dimension.toLong * 4L),
          1,
          dimension,
          topK,
          bytes(excluded, 1L),
          bytes(ids, topK.toLong * 8L),
          bytes(distances, topK.toLong * 4L),
          """{"metric_type":"L2"}"""
        )
        (0 until topK).map(index => ids.getLong(index * 8L)) shouldBe Seq(
          1L,
          2L
        )
      } finally {
        distances.close()
        ids.close()
        excluded.close()
        queries.close()
        base.close()
      }
    }
  }
}
