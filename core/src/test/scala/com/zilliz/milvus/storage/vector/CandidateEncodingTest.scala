package com.zilliz.milvus.storage.vector

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}
import java.lang.{Double => JavaDouble, Float => JavaFloat}
import java.nio.ByteBuffer
import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType

class CandidateEncodingTest extends AnyFunSuite with Matchers {
  private val segmentIds = Seq(Long.MinValue, -7L, 0L, Long.MaxValue)
  private val dictionary = CandidateDictionary(segmentIds.reverse)
  private val encoding = CandidateEncoding(dictionary, segmentIds)

  private def fields(candidates: Seq[Candidate]): Seq[(Long, Long, Int)] =
    candidates.map(candidate =>
      (
        candidate.segmentId,
        candidate.rowOffset,
        JavaFloat.floatToIntBits(candidate.score)
      )
    )

  private def decoded(bytes: Array[Byte]): Seq[(Long, Long, Int)] = {
    val result = Vector.newBuilder[(Long, Long, Int)]
    CandidateBytes.foreach(bytes, dictionary) { (segment, row, score) =>
      result += ((segment, row, JavaFloat.floatToIntBits(score)))
    }
    result.result()
  }

  private def restored[A](value: A): A = {
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(value)
    finally output.close()
    val input = new ObjectInputStream(
      new ByteArrayInputStream(bytes.toByteArray)
    )
    try input.readObject().asInstanceOf[A]
    finally input.close()
  }

  test(
    "the fixed 12-byte protocol contains two UInt32 addresses and one Float32 score"
  ) {
    CandidateBytes.Width shouldBe 12
    CandidateBytes.MaxRows shouldBe 4294967296L
    val bytes = CandidateBytes.writeColumns(
      Array(Long.MaxValue),
      Array(0xffffffffL),
      Array(1.5f),
      0,
      1,
      encoding
    )
    // Independent big-endian oracle: ordinal 3, maximum row offset, score 1.5f.
    bytes.toVector shouldBe Vector[Byte](
      0,
      0,
      0,
      3,
      -1,
      -1,
      -1,
      -1,
      0x3f,
      0xc0.toByte,
      0,
      0
    )
    CandidateBytes.count(bytes) shouldBe 1
    CandidateBytes.segmentId(bytes, 0, dictionary) shouldBe Long.MaxValue
    CandidateBytes.rowOffset(bytes, 0) shouldBe 0xffffffffL
    CandidateBytes.score(bytes, 0) shouldBe 1.5f
    CandidateBytes.count(new Array[Byte](60)) shouldBe 5
    intercept[IllegalArgumentException](
      CandidateBytes.count(new Array[Byte](11))
    )
  }

  test("encoding retains Float values, signed zeros and raw NaN payloads") {
    val scores = Array(
      JavaFloat.MIN_VALUE,
      JavaFloat.MAX_VALUE,
      -JavaFloat.MAX_VALUE,
      -0.0f,
      0.0f,
      JavaFloat.intBitsToFloat(0x7fc00042),
      JavaFloat.intBitsToFloat(0xffc00031),
      Float.NegativeInfinity,
      Float.PositiveInfinity
    )
    val bytes = CandidateBytes.writeColumns(
      Array.fill(scores.length)(-7L),
      Array.tabulate(scores.length)(_.toLong),
      scores,
      0,
      scores.length,
      encoding
    )
    scores.indices.foreach { at =>
      JavaFloat.floatToRawIntBits(CandidateBytes.score(bytes, at)) shouldBe
        JavaFloat.floatToRawIntBits(scores(at))
    }
  }

  test(
    "Float rounding precedes local top-k and preserves the global merge order"
  ) {
    val close = 1.0 + math.pow(2.0, -40)
    close.toFloat shouldBe 1.0f
    val huge = math.pow(2.0, 140)
    huge.toFloat shouldBe Float.PositiveInfinity
    // The better Double score has the worse address. Rounding must happen
    // before local selection so ties use the same address at every stage.
    val raw = Vector(
      Vector((0L, 1.0), (-7L, close)),
      Vector((0L, huge), (-7L, huge * 4.0)),
      Vector((0L, JavaDouble.MIN_VALUE), (-7L, 0.0)),
      Vector((0L, -JavaDouble.MIN_VALUE), (-7L, -0.0)),
      Vector((0L, Double.NaN), (-7L, Double.NaN)),
      Vector((0L, -huge), (-7L, -huge * 4.0))
    )
    val sources = raw.zipWithIndex.map { case (run, at) =>
      run.map { case (segment, score) =>
        Candidate(0, segment, at.toLong, score.toFloat)
      }
    }
    for (metric <- MetricType.values; k <- Seq(1, 3, 8, 20)) {
      val expected =
        fields(sources.flatten.sorted(Candidate.ranking(metric)).take(k))
      val inputs = sources.map { source =>
        val local = new TopKMerger(1, k, metric)
        source.foreach(local.add)
        local.sort(0).takePacked(0, encoding)
      }
      decoded(
        inputs.reduce(CandidateBytes.merge(_, _, k, metric))
      ) shouldBe expected
      decoded(CandidateBytes.merge(inputs, k, metric)) shouldBe expected
      val state = new CandidateRuns(k, metric, CandidateRuns.Plain, 3, 100L)
      inputs.reverse.foreach(state.append)
      decoded(state.finish()) shouldBe expected
      val local = new TopKMerger(1, k, metric)
      sources.flatten.reverse.foreach(local.add)
      decoded(local.sort(0).takePacked(0, encoding)) shouldBe expected
    }
    val local = new TopKMerger(1, 1, MetricType.L2)
    sources.head.foreach(local.add)
    local.results(0).head.segmentId shouldBe -7L
  }

  test(
    "dictionary ordinals are global signed-Long order and survive serialization"
  ) {
    val made = CandidateDictionary(Seq(Long.MaxValue, -7L, Long.MinValue, -7L))
    val copy = restored(made)
    copy.size shouldBe 3
    (0 until copy.size).map(at => copy.segmentId(at.toLong)) shouldBe
      Seq(Long.MinValue, -7L, Long.MaxValue)
    copy.ordinal(-7L) shouldBe 1
    Seq(-1L, 3L, 0xffffffffL, 1L << 32).foreach { at =>
      intercept[IllegalArgumentException](copy.segmentId(at))
    }
    intercept[IllegalArgumentException](copy.ordinal(100L))
    CandidateDictionary(Seq.empty).size shouldBe 0
  }

  test(
    "each task keeps global ordinals even when its segment subset and order differ"
  ) {
    val first = restored(CandidateEncoding(dictionary, Seq(Long.MaxValue, -7L)))
    val second = restored(CandidateEncoding(dictionary, Seq(0L, Long.MinValue)))
    val left = CandidateBytesFixture.pack(
      Seq(Candidate(0, Long.MaxValue, 0L, 1.0f), Candidate(0, -7L, 0L, 1.0f)),
      MetricType.L2,
      first
    )
    val right = CandidateBytesFixture.pack(
      Seq(Candidate(0, 0L, 0L, 1.0f), Candidate(0, Long.MinValue, 0L, 1.0f)),
      MetricType.L2,
      second
    )
    ByteBuffer.wrap(left).getInt(0) shouldBe 1
    ByteBuffer.wrap(left).getInt(12) shouldBe 3
    ByteBuffer.wrap(right).getInt(0) shouldBe 0
    ByteBuffer.wrap(right).getInt(12) shouldBe 2
    decoded(CandidateBytes.merge(left, right, 4, MetricType.L2))
      .map(_._1) shouldBe segmentIds
  }

  test(
    "encoding rejects out-of-range rows and segments outside the task subset"
  ) {
    val local = CandidateEncoding(dictionary, Seq(-7L, 0L))
    def pack(id: Long, row: Long): Array[Byte] = CandidateBytes.writeColumns(
      Array(id),
      Array(row),
      Array(1.0f),
      0,
      1,
      local
    )
    Seq(0L, 0x7fffffffL, 0x80000000L, 0xffffffffL).foreach { row =>
      CandidateBytes.rowOffset(pack(-7L, row), 0) shouldBe row
    }
    Seq(Long.MinValue, -1L, CandidateBytes.MaxRows, Long.MaxValue).foreach {
      row =>
        intercept[IllegalArgumentException](pack(-7L, row))
    }
    // A globally known identity is still rejected when it is not this task's source.
    intercept[IllegalArgumentException](pack(Long.MaxValue, 0L))
    intercept[IllegalArgumentException](pack(1L, 0L))
    intercept[IllegalArgumentException](CandidateEncoding(dictionary, Seq(1L)))
    val invalid = pack(-7L, 0L)
    ByteBuffer.wrap(invalid).putInt(0, -1)
    intercept[IllegalArgumentException](
      CandidateBytes.segmentId(invalid, 0, dictionary)
    )
    intercept[IllegalArgumentException](
      CandidateBytes.foreach(invalid, dictionary)((_, _, _) => ())
    )
  }

  test(
    "pairwise, multiway and batched merges retain signed segment and unsigned row order"
  ) {
    val random = new Random(2187L)
    val scores = Vector(
      Float.NaN,
      Float.NegativeInfinity,
      -0.0f,
      0.0f,
      1.0f,
      2.0f,
      Float.PositiveInfinity
    )
    val rows = Vector(0L, 0x7fffffffL, 0x80000000L, 0xffffffffL)
    val sources = Vector.tabulate(11) { run =>
      Vector.tabulate(9) { row =>
        Candidate(
          0,
          segmentIds(run % segmentIds.size),
          rows(row % rows.size),
          scores(random.nextInt(scores.size))
        )
      }
    }
    for (metric <- MetricType.values) {
      val inputs = sources.map(CandidateBytesFixture.pack(_, metric, encoding))
      val saved = inputs.map(_.clone())
      Seq(1, 7, 31, 200).foreach { k =>
        val expected =
          fields(sources.flatten.sorted(Candidate.ranking(metric)).take(k))
        decoded(
          inputs.reduce(CandidateBytes.merge(_, _, k, metric))
        ) shouldBe expected
        decoded(
          CandidateBytes.merge(random.shuffle(inputs), k, metric)
        ) shouldBe expected
        for (batch <- Seq(2, 4, 8); target <- Seq(1L, 1024L, Long.MaxValue)) {
          val state =
            new CandidateRuns(k, metric, CandidateRuns.Plain, batch, target)
          random.shuffle(inputs).foreach(state.append)
          val answer = state.finish()
          decoded(answer) shouldBe expected
          state.finish() should be theSameInstanceAs answer
        }
        val merger = new TopKMerger(1, k, metric)
        sources.flatten.foreach(merger.add)
        decoded(merger.sort(0).takePacked(0, encoding)) shouldBe expected
      }
      inputs.zip(saved).foreach { case (actual, original) =>
        actual.toSeq shouldBe original.toSeq
      }
    }
  }

  test("single-run shortcuts preserve ownership and malformed widths fail") {
    val one = CandidateBytesFixture.pack(
      Seq(Candidate(0, -7L, 0L, 1.0f)),
      MetricType.L2,
      encoding
    )
    CandidateBytes.merge(
      Vector(one),
      1,
      MetricType.L2
    ) should be theSameInstanceAs one
    CandidateBytes.merge(
      one,
      CandidateBytes.Empty,
      1,
      MetricType.L2
    ) should be theSameInstanceAs one
    CandidateBytes.merge(Vector.empty, 1, MetricType.L2) shouldBe empty
    intercept[IllegalArgumentException](
      CandidateBytes.count(new Array[Byte](CandidateBytes.Width - 1))
    )
  }
}
