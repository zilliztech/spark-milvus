package com.zilliz.milvus.storage.vector

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.lang.{Float => JavaFloat}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.ByteBuffer
import java.util.{Arrays, BitSet}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType

/** Candidates that carry their rows, as a DataFrame input's first stage sends
  * them and its merge joins them, and the batches such an input builds from
  * float vectors (design document architecture/dataframe-api.html section 4).
  */
class CarriedCandidatesTest extends AnyFunSuite with Matchers {

  private def carried(
      packed: Array[Byte]
  ): Vector[(Long, Long, Float, String)] = {
    val read = Vector.newBuilder[(Long, Long, Float, String)]
    CarriedCandidates.foreach(packed) { (unit, row, score, at, length) =>
      read += ((unit, row, score, new String(packed, at, length, UTF_8)))
    }
    read.result()
  }

  private def answer(
      metric: MetricType,
      entries: (Long, Long, Float, String)*
  ): Array[Byte] = {
    val merger = new TopKMerger(1, math.max(1, entries.size), metric)
    val rows = entries.map { case (unit, row, score, text) =>
      merger.add(Candidate(0, unit, row, score))
      (unit, row) -> text.getBytes(UTF_8)
    }.toMap
    CarriedCandidates.writeSorted(
      merger
        .sort(0)
        .results(0)
        .map(candidate =>
          candidate -> rows((candidate.segmentId, candidate.rowOffset))
        )
    )
  }

  test("encoding keeps the supplied order and each associated row") {
    val ascending = Vector(
      (2L, 8L, 1.0f, "first"),
      (1L, 9L, 2.0f, ""),
      (0L, 7L, 3.0f, "last")
    )
    // These are already ordered for L2 and IP respectively. Encoding needs
    // neither the metric nor a second sort to preserve either answer.
    Seq(ascending, ascending.reverse).foreach { ordered =>
      val packed = CarriedCandidates.writeSorted(
        ordered.map { case (unit, row, score, text) =>
          Candidate(0, unit, row, score) -> text.getBytes(UTF_8)
        }
      )
      carried(packed) shouldBe ordered
    }
  }

  test("carried addresses use unsigned 32-bit fields and a 16-byte header") {
    val candidate = Candidate(0, 0xffffffffL, 0x80000000L, 1.5f)
    val row = Array[Byte](42)
    val packed = CarriedCandidates.writeSorted(Vector(candidate -> row))
    packed.toVector shouldBe Vector[Byte](
      -1,
      -1,
      -1,
      -1,
      0x80.toByte,
      0,
      0,
      0,
      0x3f,
      0xc0.toByte,
      0,
      0,
      0,
      0,
      0,
      1,
      42
    )
    var actual = Vector.empty[(Long, Long, Float, Int, Int)]
    CarriedCandidates.foreach(packed) {
      (partition, offset, score, from, length) =>
        actual :+= ((partition, offset, score, from, length))
    }
    actual shouldBe Vector((0xffffffffL, 0x80000000L, 1.5f, 16, 1))
    CarriedCandidates.encodedSize(Vector(candidate))(_ =>
      row.length
    ) shouldBe 17L
  }

  test(
    "both carried writers reject addresses outside UInt32 before narrowing"
  ) {
    val invalid = Seq(Long.MinValue, -1L, 1L << 32, Long.MaxValue)
    val candidates = invalid.flatMap(address =>
      Seq(Candidate(0, address, 0L, 1.0f), Candidate(0, 0L, address, 1.0f))
    )
    candidates.foreach { candidate =>
      intercept[IllegalArgumentException] {
        CarriedCandidates.writeSorted(Vector(candidate -> Array.emptyByteArray))
      }.getMessage should include("outside UInt32")
      intercept[IllegalArgumentException] {
        CarriedCandidates.writeSorted(Vector(candidate), 16)(_ =>
          Array.emptyByteArray
        )
      }.getMessage should include("outside UInt32")
    }
  }

  test("an empty sorted answer writes no bytes and merges away") {
    val empty = CarriedCandidates.writeSorted(Vector.empty)
    empty.length shouldBe 0
    carried(empty) shouldBe Vector.empty
    val one = answer(MetricType.L2, (1L, 2L, 3.0f, "row"))
    CarriedCandidates.merge(empty, one, 2, MetricType.L2).toSeq shouldBe
      one.toSeq
    CarriedCandidates.merge(one, empty, 2, MetricType.L2).toSeq shouldBe
      one.toSeq
    CarriedCandidates.merge(empty, empty, 2, MetricType.L2).length shouldBe 0
  }

  test("each candidate keeps its row, best first") {
    val packed = answer(
      MetricType.L2,
      (1L, 7L, 2.0f, "seven"),
      (0L, 3L, 0.5f, ""),
      (2L, 1L, 1.0f, "a row")
    )
    carried(packed) shouldBe Vector(
      (0L, 3L, 0.5f, ""),
      (2L, 1L, 1.0f, "a row"),
      (1L, 7L, 2.0f, "seven")
    )
  }

  test("two answers merge into the best k, rows and all") {
    val left = answer(MetricType.L2, (0L, 1L, 1.0f, "l1"), (0L, 2L, 3.0f, "l3"))
    val right =
      answer(MetricType.L2, (1L, 1L, 2.0f, "r2"), (1L, 2L, 4.0f, "r4"))
    carried(CarriedCandidates.merge(left, right, 3, MetricType.L2))
      .map(_._4) shouldBe
      Vector("l1", "r2", "l3")
    carried(
      CarriedCandidates.merge(left, Array.emptyByteArray, 5, MetricType.L2)
    )
      .map(_._4) shouldBe Vector("l1", "l3")
  }

  test(
    "a similarity ranks the larger first, NaN above every number, ties by place"
  ) {
    val left =
      answer(MetricType.IP, (0L, 5L, 1.0f, "tie-late"), (0L, 9L, -1.0f, "low"))
    val right =
      answer(
        MetricType.IP,
        (0L, 2L, 1.0f, "tie-early"),
        (1L, 0L, Float.NaN, "nan")
      )
    carried(CarriedCandidates.merge(left, right, 4, MetricType.IP))
      .map(_._4) shouldBe
      Vector("nan", "tie-early", "tie-late", "low")
  }

  test(
    "a batch borrows only its buffer region and leaves memory with its owner"
  ) {
    val allocator = new RootAllocator()
    try {
      val buffer = allocator.buffer(32L)
      try {
        buffer.setFloat(8L, 1f)
        buffer.setFloat(12L, 2f)
        val excluded = new BitSet(2)
        excluded.set(1)
        val view = buffer.nioBuffer(0L, 32)
        view.position(8)
        view.limit(24)
        val batch = VectorBatch.borrow(view, 2, 100L, excluded)
        view.position(0)
        batch.rows shouldBe 2
        batch.firstRow shouldBe 100L
        batch.excluded.get(1) shouldBe true
        batch.visibleRows shouldBe 1
        batch.base.borrowed shouldBe true
        batch.base.buffer.capacity() shouldBe 16
        batch.base.buffer.getFloat(0) shouldBe 1f
        batch.base.buffer.getFloat(4) shouldBe 2f
        batch.close()
        allocator.getAllocatedMemory shouldBe 32L
        buffer.setFloat(8L, 3f)
        batch.base.buffer.getFloat(0) shouldBe 3f
      } finally buffer.close()
      allocator.getAllocatedMemory shouldBe 0L
    } finally allocator.close()
  }

  test(
    "multiway selection preserves every ranked record and its variable row"
  ) {
    val random = new Random(812)
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
      val ranking = Candidate.ranking(metric)
      (0 until 20).foreach { trial =>
        val entries = Vector.tabulate(trial % 12) { run =>
          Vector
            .tabulate(random.nextInt(24)) { row =>
              val candidate = Candidate(
                0,
                run % 3,
                run * 100L + row,
                scores(random.nextInt(scores.size))
              )
              val payload = new Array[Byte](random.nextInt(200))
              random.nextBytes(payload)
              candidate -> payload
            }
            .sortWith((a, b) => ranking.compare(a._1, b._1) < 0)
        }
        val runs = entries.map(CarriedCandidates.writeSorted)
        val originals = runs.map(_.clone())
        Seq(1, 7, 37, Int.MaxValue).foreach { k =>
          val expected = CarriedCandidates.writeSorted(
            entries.flatten
              .sortWith((a, b) => ranking.compare(a._1, b._1) < 0)
              .take(k)
          )
          CarriedCandidates.merge(runs, k, metric).toSeq shouldBe expected.toSeq
          CarriedCandidates
            .merge(random.shuffle(runs), k, metric)
            .toSeq shouldBe
            expected.toSeq
          val partials =
            runs.grouped(3).map(CarriedCandidates.merge(_, k, metric)).toVector
          CarriedCandidates
            .merge(partials, k, metric)
            .toSeq shouldBe expected.toSeq
        }
        runs.zip(originals).foreach { case (run, original) =>
          run.toSeq shouldBe original.toSeq
        }
      }
    }
  }

  test("single runs are reused or cut at a complete carried record boundary") {
    val packed =
      answer(MetricType.L2, (0L, 1L, 1.0f, ""), (0L, 2L, 2.0f, "longer"))
    val inputs = ArrayBuffer(Array.emptyByteArray, packed)
    (CarriedCandidates.merge(
      inputs,
      Int.MaxValue,
      MetricType.L2
    ) eq packed) shouldBe true
    (CarriedCandidates.merge(
      packed,
      Array.emptyByteArray,
      Int.MaxValue,
      MetricType.L2
    ) eq packed) shouldBe true
    carried(CarriedCandidates.merge(inputs, 1, MetricType.L2)) shouldBe
      Vector((0L, 1L, 1.0f, ""))
    CarriedCandidates.merge(Vector.empty, 1, MetricType.L2).length shouldBe 0
    the[IllegalArgumentException] thrownBy
      CarriedCandidates.merge(Vector.empty, 0, MetricType.L2)
  }

  test(
    "all merge paths reject malformed records before comparing their heads"
  ) {
    val good = answer(MetricType.L2, (0L, 1L, 1.0f, "row"))
    val negative = ByteBuffer.allocate(16).putInt(12, -1).array()
    val overflow = ByteBuffer.allocate(16).putInt(12, Int.MaxValue).array()
    val truncated = ByteBuffer.allocate(17).putInt(12, 2).array()
    Seq(new Array[Byte](11), new Array[Byte](15), negative, overflow, truncated)
      .foreach { bad =>
        the[IllegalArgumentException] thrownBy
          CarriedCandidates.foreach(bad)((_, _, _, _, _) => ())
        the[IllegalArgumentException] thrownBy
          CarriedCandidates.merge(Vector(bad), 1, MetricType.L2)
        the[IllegalArgumentException] thrownBy
          CarriedCandidates.merge(good, bad, 1, MetricType.L2)
        the[IllegalArgumentException] thrownBy
          CarriedCandidates.merge(Vector(good, bad, good), 1, MetricType.L2)
      }
  }

  test("the unvisited tail after the kth winner is not read") {
    val first = answer(MetricType.L2, (0L, 1L, 0.0f, "first"))
    val badTail = first ++ Array[Byte](1)
    val second = answer(MetricType.L2, (1L, 1L, 1.0f, "second"))
    val third = answer(MetricType.L2, (2L, 1L, 2.0f, "third"))
    Seq(
      Vector(badTail),
      Vector(badTail, second),
      Vector(badTail, second, third)
    )
      .foreach { runs =>
        CarriedCandidates
          .merge(runs, 1, MetricType.L2)
          .toSeq shouldBe first.toSeq
        the[IllegalArgumentException] thrownBy
          CarriedCandidates.merge(runs, 2, MetricType.L2)
      }
  }

  test(
    "large carried output lengths fail without overflowing or allocating the output"
  ) {
    // Reuse one payload to test a total above Int.MaxValue with modest input memory.
    val row = new Array[Byte](1024 * 1024)
    val record = Candidate(0, 0L, 0L, 0.0f) -> row
    the[IllegalArgumentException] thrownBy
      CarriedCandidates.writeSorted(Vector.fill(2048)(record))
    val packed = CarriedCandidates.writeSorted(Vector(record))
    the[IllegalArgumentException] thrownBy
      CarriedCandidates.merge(Vector.fill(2048)(packed), 2048, MetricType.L2)
  }

  test("pre-sized encoding agrees with an independent protocol writer") {
    val records = Vector(
      Candidate(0, 2L, 1L, Float.NegativeInfinity) -> Array.emptyByteArray,
      Candidate(0, 1L, 2L, -0.0f) -> Array[Byte](7),
      Candidate(0, 3L, 3L, +0.0f) -> Array.tabulate[Byte](7)(_.toByte),
      Candidate(0, 1L, 4L, 3.0f) -> Array.tabulate[Byte](65)(_.toByte),
      Candidate(0, 1L, 5L, 3.0f) -> Array.tabulate[Byte](1024)(_.toByte),
      Candidate(0, 2L, 6L, Float.NaN) -> Array[Byte](-1)
    )
    val candidates = records.map(_._1)
    val rows = records.map { case (candidate, row) =>
      candidate.rowOffset -> row
    }.toMap
    val output = new ByteArrayOutputStream()
    val oracle = new DataOutputStream(output)
    records.foreach { case (candidate, row) =>
      oracle.writeInt(candidate.segmentId.toInt)
      oracle.writeInt(candidate.rowOffset.toInt)
      oracle.writeFloat(candidate.score)
      oracle.writeInt(row.length)
      oracle.write(row)
    }
    oracle.close()
    val expected = output.toByteArray
    var measured = 0
    val bytes = CarriedCandidates.encodedSize(candidates) { candidate =>
      measured += 1
      rows(candidate.rowOffset).length
    }
    bytes shouldBe expected.length.toLong
    measured shouldBe candidates.size
    def encode(): Array[Byte] =
      CarriedCandidates.writeSorted(candidates, bytes.toInt)(candidate =>
        rows(candidate.rowOffset)
      )
    val packed = encode()
    packed.toSeq shouldBe expected.toSeq
    CarriedCandidates.writeSorted(records).toSeq shouldBe expected.toSeq
    val repeated = encode()
    repeated.toSeq shouldBe expected.toSeq
    (packed eq repeated) shouldBe false
    CarriedCandidates
      .mapRows(packed) { (at, length) =>
        Arrays.copyOfRange(packed, at, at + length).toVector
      }
      .toVector shouldBe records.map(_._2.toVector)
  }

  test("encoded sizes use Long before the caller checks the array limit") {
    val candidates = Vector(
      Candidate(0, 1L, 2L, 0.0f),
      Candidate(0, 1L, 3L, 1.0f)
    )
    CarriedCandidates.encodedSize(candidates)(_ => Int.MaxValue) shouldBe
      2L * (16L + Int.MaxValue.toLong)
    CarriedCandidates.encodedSize(candidates.take(1))(_ =>
      Int.MaxValue - 16
    ) shouldBe Int.MaxValue.toLong
    CarriedCandidates.encodedSize(Vector.empty)(_ =>
      fail("empty answer requested a row size")
    ) shouldBe 0L
    CarriedCandidates
      .writeSorted(Vector.empty[Candidate], 0)(_ =>
        fail("empty answer requested row bytes")
      )
      .length shouldBe 0
  }

  test(
    "row mapping is pulled in order and repeated reads leave bytes unchanged"
  ) {
    val packed = answer(
      MetricType.L2,
      (0L, 1L, 0.0f, "first"),
      (0L, 2L, 1.0f, ""),
      (0L, 3L, 2.0f, "last")
    )
    val original = packed.clone()
    var calls = 0
    def read(): Iterator[String] = CarriedCandidates.mapRows(packed) {
      (at, length) =>
        calls += 1
        new String(packed, at, length, UTF_8)
    }
    val rows = read()
    calls shouldBe 0
    rows.hasNext shouldBe true
    calls shouldBe 0
    rows.next() shouldBe "first"
    calls shouldBe 1
    rows.toVector shouldBe Vector("", "last")
    calls shouldBe 3
    rows.hasNext shouldBe false
    intercept[NoSuchElementException](rows.next())
    read().toVector shouldBe Vector("first", "", "last")
    packed.toSeq shouldBe original.toSeq
    val empty = CarriedCandidates.mapRows(Array.emptyByteArray) { (_, _) =>
      fail("empty answer requested a row")
    }
    empty.hasNext shouldBe false
    intercept[NoSuchElementException](empty.next())
  }

  test("row callbacks expose failures without changing encoded input") {
    val candidate = Candidate(0, 1L, 2L, 3.0f)
    val candidates = Vector(candidate)
    val row = "row".getBytes(UTF_8)
    val bytes = CarriedCandidates.encodedSize(candidates)(_ => row.length)
    val failure = new IllegalStateException("missing row")
    intercept[IllegalStateException](
      CarriedCandidates.writeSorted(candidates, bytes.toInt)(_ => throw failure)
    ) should be theSameInstanceAs failure
    val packed =
      CarriedCandidates.writeSorted(candidates, bytes.toInt)(_ => row)
    val original = packed.clone()
    val rows = CarriedCandidates.mapRows(packed)((_, _) => throw failure)
    intercept[IllegalStateException](
      rows.next()
    ) should be theSameInstanceAs failure
    rows.hasNext shouldBe false
    packed.toSeq shouldBe original.toSeq
    CarriedCandidates
      .mapRows(packed) { (at, length) =>
        new String(packed, at, length, UTF_8)
      }
      .toVector shouldBe Vector("row")
  }
}
