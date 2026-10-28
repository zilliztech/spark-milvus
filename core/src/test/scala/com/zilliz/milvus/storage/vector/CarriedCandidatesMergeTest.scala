package com.zilliz.milvus.storage.vector

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.lang.{Float => JavaFloat, Long => JavaLong}
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType

/** Independent protocol and ordering checks for carried-row merging. */
class CarriedCandidatesMergeTest extends AnyFunSuite with Matchers {
  import CarriedCandidatesMergeTest.Entry

  private val metrics = Seq(MetricType.L2, MetricType.IP, MetricType.Cosine)

  private def entry(
      unit: Long,
      offset: Long,
      score: Float,
      width: Int
  ): Entry =
    Entry(
      unit,
      offset,
      JavaFloat.floatToRawIntBits(score),
      Vector.tabulate(width)(at => (offset + at * 13L).toByte)
    )

  private def before(left: Entry, right: Entry, metric: MetricType): Boolean = {
    val score = JavaFloat.compare(
      JavaFloat.intBitsToFloat(left.scoreBits),
      JavaFloat.intBitsToFloat(right.scoreBits)
    )
    if (score != 0) {
      if (metric == MetricType.L2) score < 0 else score > 0
    } else {
      val unit = JavaLong.compare(left.unit, right.unit)
      if (unit != 0) unit < 0 else left.offset < right.offset
    }
  }

  // Deliberately independent of CarriedCandidates and TopKMerger's encoders.
  private def encoded(entries: Seq[Entry]): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    entries.foreach { value =>
      out.writeInt(value.unit.toInt)
      out.writeInt(value.offset.toInt)
      out.writeInt(value.scoreBits)
      out.writeInt(value.row.size)
      out.write(value.row.toArray)
    }
    out.close()
    bytes.toByteArray
  }

  private def check(
      left: Seq[Entry],
      right: Seq[Entry],
      k: Int,
      metric: MetricType
  ): Unit = {
    val leftBytes = encoded(left.sortWith(before(_, _, metric)))
    val rightBytes = encoded(right.sortWith(before(_, _, metric)))
    val leftBefore = leftBytes.toVector
    val rightBefore = rightBytes.toVector
    val expected = encoded(
      (left ++ right).sortWith(before(_, _, metric)).take(k)
    )
    CarriedCandidates.merge(leftBytes, rightBytes, k, metric).toVector shouldBe
      expected.toVector
    CarriedCandidates.merge(rightBytes, leftBytes, k, metric).toVector shouldBe
      expected.toVector
    leftBytes.toVector shouldBe leftBefore
    rightBytes.toVector shouldBe rightBefore
  }

  test(
    "a sparse answer fits an arbitrarily large positive k without k-sized scratch"
  ) {
    val left = Seq(entry(0L, 0L, 3.0f, 0), entry(0L, 1L, 1.0f, 16))
    val right = Seq(entry(1L, 0L, 2.0f, 4096))
    metrics.foreach { metric =>
      Seq(1, 2, 100000, Int.MaxValue).foreach { k =>
        check(left, right, k, metric)
      }
      check(Seq.empty, right, Int.MaxValue, metric)
      check(left, Seq.empty, Int.MaxValue, metric)
      check(Seq.empty, Seq.empty, Int.MaxValue, metric)
    }
  }

  test(
    "zero-length rows, signed zero, infinities and NaN payloads retain exact bytes"
  ) {
    val entries = Seq(
      entry(3L, 0L, -0.0f, 0),
      entry(1L, 0L, 0.0f, 0),
      entry(2L, 0L, Float.NegativeInfinity, 1),
      entry(2L, 1L, Float.PositiveInfinity, 16),
      entry(0L, 1L, 7.0f, 127),
      entry(0L, 0L, 7.0f, 0),
      Entry(1L, 2L, 0x7fc00042, Vector(42.toByte)),
      Entry(0L, 2L, 0xffc00017, Vector.empty)
    )
    metrics.foreach { metric =>
      Seq(1, 4, 8, 100000).foreach { k =>
        check(entries.take(3), entries.drop(3), k, metric)
      }
    }
  }

  test(
    "random valid answers match a full-sort oracle for sparse and full top-k"
  ) {
    val random = new Random(0x4652414d45L)
    val scores = Vector(
      Float.NaN,
      Float.NegativeInfinity,
      Float.PositiveInfinity,
      -0.0f,
      0.0f,
      1.0f
    )
    val widths = Vector(0, 1, 16, 127, 1024)
    (0 until 300).foreach { iteration =>
      val entries = Vector.tabulate(random.nextInt(80)) { at =>
        val score =
          if (random.nextBoolean()) scores(random.nextInt(scores.size))
          else random.nextInt(41).toFloat / 3.0f - 7.0f
        entry(
          0x7ffffffdL + (at % 7).toLong,
          at.toLong,
          score,
          widths(random.nextInt(widths.size))
        )
      }
      val shuffled = random.shuffle(entries)
      val split = random.nextInt(entries.size + 1)
      val k = Vector(1, 2, 10, 1000, 100000)(random.nextInt(5))
      check(
        shuffled.take(split),
        shuffled.drop(split),
        k,
        metrics(iteration % metrics.size)
      )
    }
  }

  test(
    "ties compare both address fields as unsigned across the signed Int boundary"
  ) {
    val entries = Vector(
      entry(0x7fffffffL, 0xffffffffL, 1.0f, 1),
      entry(0x80000000L, 0L, 1.0f, 2),
      entry(0xffffffffL, 0x7fffffffL, 1.0f, 3),
      entry(0xffffffffL, 0x80000000L, 1.0f, 4),
      entry(0xffffffffL, 0xffffffffL, 1.0f, 5)
    )
    metrics.foreach { metric =>
      check(entries.take(2), entries.drop(2), 5, metric)
      val runs = entries.reverse.map(value => encoded(Seq(value)))
      CarriedCandidates.merge(runs, 5, metric).toVector shouldBe encoded(
        entries
      ).toVector
    }
  }

  test("invalid k and truncated encoded records still fail visibly") {
    Seq(0, -1, Int.MinValue).foreach { k =>
      intercept[IllegalArgumentException] {
        CarriedCandidates.merge(
          Array.emptyByteArray,
          Array.emptyByteArray,
          k,
          MetricType.L2
        )
      }.getMessage should include("topK must be positive")
    }
    Seq(Array[Byte](1), new Array[Byte](15)).foreach { truncated =>
      intercept[IllegalArgumentException] {
        CarriedCandidates.merge(
          truncated,
          Array.emptyByteArray,
          1,
          MetricType.L2
        )
      }.getMessage should include("Incomplete carried candidate header")
    }
  }
}

private object CarriedCandidatesMergeTest {
  final case class Entry(
      unit: Long,
      offset: Long,
      scoreBits: Int,
      row: Vector[Byte]
  )
}
