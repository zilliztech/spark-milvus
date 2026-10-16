package com.zilliz.milvus.storage.vector

import java.lang.{Float => JavaFloat}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType

/** The packed form a first-stage task sends on: what it holds, and that merging
  * packed answers gives what merging candidates would have given.
  */
class CandidateBytesTest extends AnyFunSuite with Matchers {
  private val segmentIds = (0L to 104L)
  private val dictionary = CandidateDictionary(segmentIds)
  private val encoding = CandidateEncoding(dictionary, segmentIds)

  private def places(packed: Array[Byte]): Vector[(Long, Long, Float)] = {
    val read = Vector.newBuilder[(Long, Long, Float)]
    CandidateBytes.foreach(packed, dictionary)((segment, offset, score) =>
      read += ((segment, offset, score))
    )
    read.result()
  }

  private def candidate(segment: Long, offset: Long, score: Float) =
    Candidate(0, segment, offset, score)

  test("a candidate takes three fields and nothing else") {
    CandidateBytes.Width shouldBe 12
    val packed =
      CandidateBytesFixture.pack(
        Seq(candidate(101L, 5L, 1.5f)),
        MetricType.L2,
        encoding
      )
    packed.length shouldBe 12
    CandidateBytes.count(packed) shouldBe 1
    CandidateBytes.segmentId(
      packed,
      0,
      dictionary
    ) shouldBe 101L
    CandidateBytes.rowOffset(packed, 0) shouldBe 5L
    CandidateBytes.score(packed, 0) shouldBe 1.5f
  }

  test("column packing preserves the supplied order and range") {
    val packed = CandidateBytes.writeColumns(
      Array(100L, 101L, 102L, 103L),
      Array(0L, 1L, 2L, 3L),
      Array(0.0f, 3.0f, 1.0f, 2.0f),
      base = 1,
      size = 3,
      encoding = encoding
    )
    places(packed) shouldBe Vector(
      (101L, 1L, 3.0f),
      (102L, 2L, 1.0f),
      (103L, 3L, 2.0f)
    )
  }

  test("an empty answer packs to no bytes and merges away") {
    CandidateBytes.count(CandidateBytes.Empty) shouldBe 0
    val one =
      CandidateBytesFixture.pack(
        Seq(candidate(101L, 5L, 1.5f)),
        MetricType.L2,
        encoding
      )
    places(
      CandidateBytes.merge(
        one,
        CandidateBytes.Empty,
        10,
        MetricType.L2
      )
    ) shouldBe
      places(one)
    places(
      CandidateBytes.merge(
        CandidateBytes.Empty,
        one,
        10,
        MetricType.L2
      )
    ) shouldBe
      places(one)
  }

  test("a merge keeps the best k and drops the rest") {
    val left =
      CandidateBytesFixture.pack(
        Seq(candidate(101L, 1L, 1.0f), candidate(101L, 2L, 4.0f)),
        MetricType.L2,
        encoding
      )
    val right =
      CandidateBytesFixture.pack(
        Seq(candidate(102L, 3L, 2.0f), candidate(102L, 4L, 5.0f)),
        MetricType.L2,
        encoding
      )
    places(
      CandidateBytes.merge(left, right, 3, MetricType.L2)
    ) shouldBe Vector(
      (101L, 1L, 1.0f),
      (102L, 3L, 2.0f),
      (101L, 2L, 4.0f)
    )
  }

  test("a side longer than k is cut to k") {
    val long = CandidateBytesFixture.pack(
      (1 to 5).map(index => candidate(101L, index.toLong, index.toFloat)),
      MetricType.L2,
      encoding
    )
    places(
      CandidateBytes.merge(
        long,
        CandidateBytes.Empty,
        2,
        MetricType.L2
      )
    )
      .map(_._2) shouldBe Vector(1L, 2L)
  }

  test("ties break on segment then row, so a merge is order-independent") {
    val left =
      CandidateBytesFixture.pack(
        Seq(candidate(102L, 9L, 1.0f)),
        MetricType.L2,
        encoding
      )
    val right = CandidateBytesFixture.pack(
      Seq(candidate(101L, 9L, 1.0f), candidate(102L, 8L, 1.0f)),
      MetricType.L2,
      encoding
    )
    val forward = places(
      CandidateBytes.merge(left, right, 3, MetricType.L2)
    )
    forward shouldBe Vector(
      (101L, 9L, 1.0f),
      (102L, 8L, 1.0f),
      (102L, 9L, 1.0f)
    )
    places(
      CandidateBytes.merge(right, left, 3, MetricType.L2)
    ) shouldBe forward
  }

  test("merging packed answers agrees with merging candidates, in any order") {
    val random = new Random(7)
    val sides = (0 until 5).map(segment =>
      (0 until 8).map(row =>
        candidate(100L + segment, row.toLong, random.nextInt(50).toFloat)
      )
    )
    Seq(MetricType.L2, MetricType.Cosine).foreach { metric =>
      val expected = {
        val merger = new TopKMerger(1, 6, metric)
        sides.flatten.foreach(merger.add)
        places(merger.sort(0).takePacked(0, encoding))
      }
      val packed = sides.map(CandidateBytesFixture.pack(_, metric, encoding))
      val straight =
        places(
          packed.reduce(
            CandidateBytes.merge(_, _, 6, metric)
          )
        )
      straight shouldBe expected
      val shuffled = places(
        random
          .shuffle(packed)
          .reduce(CandidateBytes.merge(_, _, 6, metric))
      )
      shuffled shouldBe straight
    }
  }

  test("a width that is not a candidate is refused") {
    the[IllegalArgumentException] thrownBy CandidateBytes.count(
      new Array[Byte](23)
    )
    the[IllegalArgumentException] thrownBy CandidateBytes.merge(
      CandidateBytes.Empty,
      CandidateBytes.Empty,
      0,
      MetricType.L2
    )
    // A metric the search does not rank by has no MetricType, so it cannot
    // reach the packing at all.
    MetricType.fromName("EUCLID") shouldBe None
  }

  test(
    "multiway merging matches an independent sorted oracle for every metric"
  ) {
    val random = new Random(811)
    val scores = Vector(
      Float.NaN,
      JavaFloat.intBitsToFloat(0x7fc00042),
      Float.NegativeInfinity,
      -0.0f,
      0.0f,
      Float.PositiveInfinity,
      1.0f,
      -1.0f
    )
    MetricType.values.foreach { metric =>
      (0 until 20).foreach { trial =>
        val entries = Vector.tabulate(trial % 12) { run =>
          Vector.tabulate(random.nextInt(24)) { row =>
            candidate(
              run % 3,
              run * 100L + row,
              scores(random.nextInt(scores.size))
            )
          }
        }
        val runs = entries.map(CandidateBytesFixture.pack(_, metric, encoding))
        val originals = runs.map(_.clone())
        Seq(1, 7, 37, Int.MaxValue).foreach { k =>
          val expected = CandidateBytesFixture.pack(
            entries.flatten.sorted(Candidate.ranking(metric)).take(k),
            metric,
            encoding
          )
          CandidateBytes
            .merge(runs, k, metric)
            .toSeq shouldBe expected.toSeq
          CandidateBytes
            .merge(random.shuffle(runs), k, metric)
            .toSeq shouldBe
            expected.toSeq
          val partials =
            runs
              .grouped(3)
              .map(CandidateBytes.merge(_, k, metric))
              .toVector
          CandidateBytes
            .merge(partials, k, metric)
            .toSeq shouldBe expected.toSeq
        }
        runs.zip(originals).foreach { case (run, original) =>
          run.toSeq shouldBe original.toSeq
        }
      }
    }
  }

  test(
    "multiway shortcuts accept mutable runs and reuse a complete single run"
  ) {
    val packed = CandidateBytesFixture.pack(
      (0 until 3).map(row => candidate(1L, row, row.toFloat)),
      MetricType.L2,
      encoding
    )
    val inputs = ArrayBuffer(CandidateBytes.Empty, packed)
    (CandidateBytes.merge(
      inputs,
      Int.MaxValue,
      MetricType.L2
    ) eq packed) shouldBe true
    CandidateBytes.count(
      CandidateBytes.merge(inputs, 2, MetricType.L2)
    ) shouldBe 2
    CandidateBytes
      .merge(Vector.empty, 1, MetricType.L2)
      .length shouldBe 0
    the[IllegalArgumentException] thrownBy
      CandidateBytes.merge(Vector.empty, 0, MetricType.L2)
    Seq(
      Vector(new Array[Byte](23)),
      Vector(packed, new Array[Byte](23), packed)
    )
      .foreach { malformed =>
        the[IllegalArgumentException] thrownBy
          CandidateBytes.merge(
            malformed,
            1,
            MetricType.L2
          )
      }
  }

  test(
    "packed output lengths are checked before Int conversion or allocation"
  ) {
    CandidateBytes.checkedLength(Int.MaxValue.toLong) shouldBe Int.MaxValue
    Seq(-1L, Int.MaxValue.toLong + 1L).foreach { bytes =>
      the[IllegalArgumentException] thrownBy CandidateBytes.checkedLength(bytes)
    }
    the[IllegalArgumentException] thrownBy CandidateBytes.writeColumns(
      Array.emptyLongArray,
      Array.emptyLongArray,
      Array.emptyFloatArray,
      0,
      Int.MaxValue,
      encoding = encoding
    )
  }
}
