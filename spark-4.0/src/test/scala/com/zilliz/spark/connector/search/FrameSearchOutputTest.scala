package com.zilliz.spark.connector.search

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.vector.{CarriedCandidates, TopKMerger}

/** The heap-owned answers remain usable after the search's native resources
  * close, and encoding retains only the answer currently being consumed.
  */
class FrameSearchOutputTest extends AnyFunSuite with Matchers {

  private def row(value: Long, bytes: Int = 16): UnsafeRow = {
    val result = new UnsafeRow(1)
    result.pointTo(new Array[Byte](bytes), bytes)
    result.setLong(0, value)
    result
  }

  private def values(packed: Array[Byte]): Seq[(Long, Double, Long)] = {
    val found = Vector.newBuilder[(Long, Double, Long)]
    CarriedCandidates.foreach(packed) { (_, offset, score, at, length) =>
      val current = new UnsafeRow(1)
      current.pointTo(packed, Platform.BYTE_ARRAY_OFFSET + at, length)
      found += ((offset, score, current.getLong(0)))
    }
    found.result()
  }

  test(
    "answers preserve query ids, rank and shared rows while skipping empty queries"
  ) {
    val merger = new TopKMerger(3, 2, MetricType.L2)
    merger.add(0, 4L, 2L, 2.0)
    merger.add(0, 4L, 1L, 1.0)
    merger.add(2, 4L, 1L, 0.0)
    val kept = new java.util.HashMap[java.lang.Long, UnsafeRow]()
    kept.put(1L, row(101L))
    kept.put(2L, row(102L))
    val group =
      SearchQueries.Group(Array(17L, 99L, 23L), Array.emptyByteArray, 0)

    val answers = FrameSearch.carriedAnswers(merger, group, kept, 88L)
    val first = answers.next()
    first._1 shouldBe 17L
    first._2.length shouldBe 88
    values(first._2) shouldBe Seq((1L, 1.0, 101L), (2L, 2.0, 102L))
    val second = answers.next()
    second._1 shouldBe 23L
    values(second._2) shouldBe Seq((1L, 0.0, 101L))
    answers.hasNext shouldBe false
  }

  test(
    "a later oversized answer is checked only when consumed and includes its header"
  ) {
    val merger = new TopKMerger(2, 1, MetricType.L2)
    merger.add(0, 4L, 1L, 1.0)
    merger.add(1, 4L, 2L, 2.0)
    val kept = new java.util.HashMap[java.lang.Long, UnsafeRow]()
    kept.put(1L, row(101L))
    kept.put(2L, row(102L, 64))
    val group = SearchQueries.Group(Array(17L, 99L), Array.emptyByteArray, 0)

    // The second row alone fits, but its 28-byte candidate header does not.
    // Creating and pulling the first answer must not encode the second one.
    val answers = FrameSearch.carriedAnswers(merger, group, kept, 64L)
    values(answers.next()._2) shouldBe Seq((1L, 1.0, 101L))
    intercept[IllegalArgumentException](
      answers.next()
    ).getMessage should include(
      "query 99 encodes 92 candidate bytes, exceeding the answer limit of 64"
    )
  }
}
