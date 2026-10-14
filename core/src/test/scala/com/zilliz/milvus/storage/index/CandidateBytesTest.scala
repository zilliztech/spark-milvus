package com.zilliz.milvus.storage.index

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType

/** The packed form a first-stage task sends on: what it holds, and that merging
  * packed answers gives what merging candidates would have given.
  */
class CandidateBytesTest extends AnyFunSuite with Matchers {

  private def places(packed: Array[Byte]): Vector[(Long, Long, Double)] = {
    val read = Vector.newBuilder[(Long, Long, Double)]
    CandidateBytes.foreach(packed)((segment, offset, score) =>
      read += ((segment, offset, score))
    )
    read.result()
  }

  private def candidate(segment: Long, offset: Long, score: Double) =
    Candidate(0, segment, offset, score)

  test("a candidate takes three fields and nothing else") {
    CandidateBytes.Width shouldBe 24
    val packed =
      CandidateBytesFixture.pack(Seq(candidate(101L, 5L, 1.5)), MetricType.L2)
    packed.length shouldBe 24
    CandidateBytes.count(packed) shouldBe 1
    CandidateBytes.segmentId(packed, 0) shouldBe 101L
    CandidateBytes.rowOffset(packed, 0) shouldBe 5L
    CandidateBytes.score(packed, 0) shouldBe 1.5
  }

  test("column packing preserves the supplied order and range") {
    val packed = CandidateBytes.writeColumns(
      Array(100L, 101L, 102L, 103L),
      Array(0L, 1L, 2L, 3L),
      Array(0.0, 3.0, 1.0, 2.0),
      base = 1,
      size = 3
    )
    places(packed) shouldBe Vector(
      (101L, 1L, 3.0),
      (102L, 2L, 1.0),
      (103L, 3L, 2.0)
    )
  }

  test("an empty answer packs to no bytes and merges away") {
    CandidateBytes.count(CandidateBytes.Empty) shouldBe 0
    val one =
      CandidateBytesFixture.pack(Seq(candidate(101L, 5L, 1.5)), MetricType.L2)
    places(
      CandidateBytes.merge(one, CandidateBytes.Empty, 10, MetricType.L2)
    ) shouldBe
      places(one)
    places(
      CandidateBytes.merge(CandidateBytes.Empty, one, 10, MetricType.L2)
    ) shouldBe
      places(one)
  }

  test("a merge keeps the best k and drops the rest") {
    val left =
      CandidateBytesFixture.pack(
        Seq(candidate(101L, 1L, 1.0), candidate(101L, 2L, 4.0)),
        MetricType.L2
      )
    val right =
      CandidateBytesFixture.pack(
        Seq(candidate(102L, 3L, 2.0), candidate(102L, 4L, 5.0)),
        MetricType.L2
      )
    places(CandidateBytes.merge(left, right, 3, MetricType.L2)) shouldBe Vector(
      (101L, 1L, 1.0),
      (102L, 3L, 2.0),
      (101L, 2L, 4.0)
    )
  }

  test("a side longer than k is cut to k") {
    val long = CandidateBytesFixture.pack(
      (1 to 5).map(index => candidate(101L, index.toLong, index.toDouble)),
      MetricType.L2
    )
    places(CandidateBytes.merge(long, CandidateBytes.Empty, 2, MetricType.L2))
      .map(_._2) shouldBe Vector(1L, 2L)
  }

  test("ties break on segment then row, so a merge is order-independent") {
    val left =
      CandidateBytesFixture.pack(Seq(candidate(102L, 9L, 1.0)), MetricType.L2)
    val right = CandidateBytesFixture.pack(
      Seq(candidate(101L, 9L, 1.0), candidate(102L, 8L, 1.0)),
      MetricType.L2
    )
    val forward = places(CandidateBytes.merge(left, right, 3, MetricType.L2))
    forward shouldBe Vector((101L, 9L, 1.0), (102L, 8L, 1.0), (102L, 9L, 1.0))
    places(CandidateBytes.merge(right, left, 3, MetricType.L2)) shouldBe forward
  }

  test("merging packed answers agrees with merging candidates, in any order") {
    val random = new Random(7)
    val sides = (0 until 5).map(segment =>
      (0 until 8).map(row =>
        candidate(100L + segment, row.toLong, random.nextInt(50).toDouble)
      )
    )
    Seq(MetricType.L2, MetricType.Cosine).foreach { metric =>
      val expected = {
        val merger = new TopKMerger(1, 6, metric)
        sides.flatten.foreach(merger.add)
        places(merger.sort(0).takePacked(0))
      }
      val packed = sides.map(CandidateBytesFixture.pack(_, metric))
      val straight =
        places(packed.reduce(CandidateBytes.merge(_, _, 6, metric)))
      straight shouldBe expected
      val shuffled = places(
        random.shuffle(packed).reduce(CandidateBytes.merge(_, _, 6, metric))
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
}
