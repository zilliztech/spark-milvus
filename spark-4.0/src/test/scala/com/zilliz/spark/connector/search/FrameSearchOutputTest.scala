package com.zilliz.spark.connector.search

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.lang.{Long => JavaLong}
import java.nio.ByteBuffer
import java.util.{Arrays, HashMap}
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.vector.{
  Candidate,
  CarriedCandidates,
  TopKMerger
}

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
    "answers preserve query ids, rank and owned rows while skipping empty queries"
  ) {
    val merger = new TopKMerger(3, 2, MetricType.L2)
    merger.add(0, 4L, 2L, 2.0)
    merger.add(0, 4L, 1L, 1.0)
    merger.add(2, 4L, 1L, 0.0)
    val kept = new HashMap[JavaLong, UnsafeRow]()
    kept.put(1L, row(101L))
    kept.put(2L, row(102L))
    val group =
      SearchQueries.Group(Array(17L, 99L, 23L), Array.emptyByteArray, 0)

    val answers = FrameSearch.carriedAnswers(merger, group, kept, 88L)
    val first = answers.next()
    first._1 shouldBe 17L
    first._2.length shouldBe 88
    values(first._2) shouldBe Seq((1L, 1.0, 101L), (2L, 2.0, 102L))
    val output = FrameSearch.carried(first._1, first._2, 1).toVector
    Arrays.fill(first._2, 0.toByte)
    output.map { case (id, base) => id -> base.getLong(0) } shouldBe
      Seq(17L -> 101L, 17L -> 102L)
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
    val kept = new HashMap[JavaLong, UnsafeRow]()
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

  test(
    "ranked answers match independent protocol bytes across repeated reads"
  ) {
    val entries = Vector(
      Candidate(0, 2L, 10L, Double.NaN) -> row(10L),
      Candidate(0, 1L, 20L, 3.0) -> row(20L, 24),
      Candidate(0, 0L, 30L, 3.0) -> row(30L, 72),
      Candidate(0, 1L, 40L, -0.0) -> row(40L, 1048),
      Candidate(0, 1L, 50L, +0.0) -> row(50L),
      Candidate(0, 0L, 60L, Double.NegativeInfinity) -> row(60L)
    )
    val byOffset = entries.map { case (candidate, base) =>
      candidate.rowOffset -> (candidate, base)
    }.toMap
    val kept = new HashMap[JavaLong, UnsafeRow]()
    entries.foreach { case (candidate, base) =>
      kept.put(candidate.rowOffset, base)
    }
    val group = SearchQueries.Group(Array(17L), Array.emptyByteArray, 0)
    for {
      metric <- Seq(MetricType.L2, MetricType.IP, MetricType.Cosine)
      k <- Seq(4, 9)
    } {
      val merger = new TopKMerger(1, k, metric)
      entries.reverse.foreach { case (candidate, _) => merger.add(candidate) }
      // Explicit independent order includes NaN, signed zero and place ties.
      val order =
        if (metric == MetricType.L2)
          Vector(60L, 40L, 50L, 30L, 20L, 10L)
        else Vector(10L, 30L, 20L, 50L, 40L, 60L)
      val selected = order.take(k).map(byOffset)
      val output = new ByteArrayOutputStream()
      val oracle = new DataOutputStream(output)
      selected.foreach { case (candidate, base) =>
        oracle.writeLong(candidate.segmentId)
        oracle.writeLong(candidate.rowOffset)
        oracle.writeDouble(candidate.score)
        oracle.writeInt(base.getSizeInBytes)
        oracle.write(base.getBytes)
      }
      oracle.close()
      val expected = output.toByteArray
      val first = FrameSearch
        .carriedAnswers(merger, group, kept, expected.length)
        .next()
      first._1 shouldBe 17L
      first._2.toSeq shouldBe expected.toSeq
      val second = FrameSearch
        .carriedAnswers(merger, group, kept, expected.length)
        .next()
      second._2.toSeq shouldBe expected.toSeq
      (first._2 eq second._2) shouldBe false
      FrameSearch
        .carried(17L, first._2, 1)
        .map { case (query, base) =>
          query -> base.getLong(0)
        }
        .toVector shouldBe order.take(k).map(17L -> _)
      merger.size shouldBe selected.size
      intercept[IllegalStateException](merger.add(0, 0L, 100L, 0.0))
    }
  }

  test(
    "empty and zero-column answers keep exact budget checks and allow retry"
  ) {
    val group = SearchQueries.Group(Array(17L), Array.emptyByteArray, 0)
    val kept = new HashMap[JavaLong, UnsafeRow]()
    val empty = new TopKMerger(1, 2, MetricType.L2)
    FrameSearch.carriedAnswers(empty, group, kept, 0L).hasNext shouldBe false
    FrameSearch.carried(17L, Array.emptyByteArray, 0).hasNext shouldBe false
    val base = new UnsafeRow(0)
    base.pointTo(Array.emptyByteArray, 0)
    val merger = new TopKMerger(1, 2, MetricType.L2)
    merger.add(0, 1L, 2L, -0.0)
    kept.put(2L, base)
    Seq(0L, 27L).foreach { budget =>
      intercept[IllegalArgumentException](
        FrameSearch.carriedAnswers(merger, group, kept, budget).next()
      ).getMessage should include("encodes 28 candidate bytes")
      merger.size shouldBe 1
    }
    val packed = FrameSearch.carriedAnswers(merger, group, kept, 28L).next()._2
    packed.length shouldBe 28
    val decoded = FrameSearch.carried(17L, packed, 0).toVector
    decoded.size shouldBe 1
    decoded.head._1 shouldBe 17L
    decoded.head._2.numFields shouldBe 0
  }

  test("a missing retained row fails visibly and does not consume candidates") {
    val group = SearchQueries.Group(Array(17L), Array.emptyByteArray, 0)
    val kept = new HashMap[JavaLong, UnsafeRow]()
    val merger = new TopKMerger(1, 1, MetricType.L2)
    merger.add(0, 1L, 2L, 3.0)
    intercept[NullPointerException](
      FrameSearch.carriedAnswers(merger, group, kept, 44L).next()
    )
    merger.size shouldBe 1
    kept.put(2L, row(102L))
    val packed = FrameSearch.carriedAnswers(merger, group, kept, 44L).next()._2
    values(packed) shouldBe Seq((2L, 3.0, 102L))
  }

  test(
    "partially consumed rows own their bytes independently of later copies"
  ) {
    val records = Vector(
      Candidate(0, 1L, 1L, 0.0) -> row(101L),
      Candidate(0, 1L, 2L, 1.0) -> row(102L, 64),
      Candidate(0, 1L, 3L, 2.0) -> row(103L, 1048)
    )
    val original = CarriedCandidates.writeSorted(
      records.map { case (candidate, base) => candidate -> base.getBytes }
    )
    val packed = original.clone()
    val rows = FrameSearch.carried(17L, packed, 1)
    val first = rows.next()
    // Payload mutation after copying must not affect the owned first row.
    packed(28 + 8) = (packed(28 + 8) ^ 0x7f).toByte
    first._2.getLong(0) shouldBe 101L
    val rest = rows.toVector
    Arrays.fill(packed, 0.toByte)
    val retained = Vector(first) ++ rest
    retained.map(_._2.getLong(0)) shouldBe Vector(101L, 102L, 103L)
    first._2.setLong(0, 999L)
    rest.map(_._2.getLong(0)) shouldBe Vector(102L, 103L)
    FrameSearch.carried(17L, original, 1).map(_._2.getLong(0)).toVector shouldBe
      Vector(101L, 102L, 103L)
  }

  test("a truncated suffix header fails before a prefix can be consumed") {
    val records = Vector(
      Candidate(0, 1L, 1L, 0.0) -> row(101L).getBytes,
      Candidate(0, 1L, 2L, 1.0) -> row(102L).getBytes
    )
    val packed = CarriedCandidates.writeSorted(records)
    val truncated = Arrays.copyOf(packed, 28 + records.head._2.length + 13)
    intercept[IllegalArgumentException](
      FrameSearch.carried(17L, truncated, 1)
    ).getMessage should include("Incomplete carried candidate header")
    // A purely lazy decoder would let take(1) conceal that bad suffix.
    CarriedCandidates
      .mapRows(truncated) { (at, length) =>
        val view = new UnsafeRow(1)
        view.pointTo(truncated, Platform.BYTE_ARRAY_OFFSET + at, length)
        view.copy()
      }
      .next()
      .getLong(0) shouldBe 101L
  }

  test("an invalid row region fails before the iterator handoff") {
    val records = Vector(
      Candidate(0, 1L, 1L, 0.0) -> row(101L).getBytes,
      Candidate(0, 1L, 2L, 1.0) -> row(102L).getBytes
    )
    val packed = CarriedCandidates.writeSorted(records)
    val second = 28 + records.head._2.length
    ByteBuffer.wrap(packed).putInt(second + 24, 1 << 20)
    def failure(check: => Any): Option[Throwable] =
      try { check; None }
      catch { case NonFatal(error) => Some(error) }
    // The complete encoded region is checked before any invalid row is copied
    // or the iterator can expose a valid prefix.
    val old = failure {
      CarriedCandidates.foreach(packed) { (_, _, _, at, length) =>
        val view = new UnsafeRow(1)
        view.pointTo(packed, Platform.BYTE_ARRAY_OFFSET + at, length)
      }
    }
    val current = failure(FrameSearch.carried(17L, packed, 1))
    current.isDefined shouldBe old.isDefined
    old.zip(current).foreach { case (before, after) =>
      after.getClass shouldBe before.getClass
      after.getMessage shouldBe before.getMessage
    }
  }
}
