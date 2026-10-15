package com.zilliz.milvus.storage.vector

import java.lang.{Double => JavaDouble}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.ByteBuffer
import java.util.BitSet
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType

/** Candidates that carry their rows, as a DataFrame input's first stage sends
  * them and its merge joins them, and the batches such an input builds from
  * float vectors (docs/design/architecture/dataframe-api.html section 4).
  */
class CarriedCandidatesTest extends AnyFunSuite with Matchers {

  private def carried(
      packed: Array[Byte]
  ): Vector[(Long, Long, Double, String)] = {
    val read = Vector.newBuilder[(Long, Long, Double, String)]
    CarriedCandidates.foreach(packed) { (unit, row, score, at, length) =>
      read += ((unit, row, score, new String(packed, at, length, UTF_8)))
    }
    read.result()
  }

  private def answer(
      metric: MetricType,
      entries: (Long, Long, Double, String)*
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
      (2L, 8L, 1.0, "first"),
      (1L, 9L, 2.0, ""),
      (0L, 7L, 3.0, "last")
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

  test("an empty sorted answer writes no bytes and merges away") {
    val empty = CarriedCandidates.writeSorted(Vector.empty)
    empty.length shouldBe 0
    carried(empty) shouldBe Vector.empty
    val one = answer(MetricType.L2, (1L, 2L, 3.0, "row"))
    CarriedCandidates.merge(empty, one, 2, MetricType.L2).toSeq shouldBe
      one.toSeq
    CarriedCandidates.merge(one, empty, 2, MetricType.L2).toSeq shouldBe
      one.toSeq
    CarriedCandidates.merge(empty, empty, 2, MetricType.L2).length shouldBe 0
  }

  test("each candidate keeps its row, best first") {
    val packed = answer(
      MetricType.L2,
      (1L, 7L, 2.0, "seven"),
      (0L, 3L, 0.5, ""),
      (2L, 1L, 1.0, "a row")
    )
    carried(packed) shouldBe Vector(
      (0L, 3L, 0.5, ""),
      (2L, 1L, 1.0, "a row"),
      (1L, 7L, 2.0, "seven")
    )
  }

  test("two answers merge into the best k, rows and all") {
    val left = answer(MetricType.L2, (0L, 1L, 1.0, "l1"), (0L, 2L, 3.0, "l3"))
    val right = answer(MetricType.L2, (1L, 1L, 2.0, "r2"), (1L, 2L, 4.0, "r4"))
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
      answer(MetricType.IP, (0L, 5L, 1.0, "tie-late"), (0L, 9L, -1.0, "low"))
    val right =
      answer(
        MetricType.IP,
        (0L, 2L, 1.0, "tie-early"),
        (1L, 0L, Double.NaN, "nan")
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
      Double.NaN,
      JavaDouble.longBitsToDouble(0x7ff8000000000042L),
      Double.NegativeInfinity,
      -0.0,
      0.0,
      Double.PositiveInfinity,
      1.0,
      -1.0
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
      answer(MetricType.L2, (0L, 1L, 1.0, ""), (0L, 2L, 2.0, "longer"))
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
      Vector((0L, 1L, 1.0, ""))
    CarriedCandidates.merge(Vector.empty, 1, MetricType.L2).length shouldBe 0
    the[IllegalArgumentException] thrownBy
      CarriedCandidates.merge(Vector.empty, 0, MetricType.L2)
  }

  test(
    "all merge paths reject malformed records before comparing their heads"
  ) {
    val good = answer(MetricType.L2, (0L, 1L, 1.0, "row"))
    val negative = ByteBuffer.allocate(28).putInt(24, -1).array()
    val overflow = ByteBuffer.allocate(28).putInt(24, Int.MaxValue).array()
    val truncated = ByteBuffer.allocate(29).putInt(24, 2).array()
    Seq(new Array[Byte](23), new Array[Byte](27), negative, overflow, truncated)
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
    val first = answer(MetricType.L2, (0L, 1L, 0.0, "first"))
    val badTail = first ++ Array[Byte](1)
    val second = answer(MetricType.L2, (1L, 1L, 1.0, "second"))
    val third = answer(MetricType.L2, (2L, 1L, 2.0, "third"))
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
    val record = Candidate(0, 0L, 0L, 0.0) -> row
    the[IllegalArgumentException] thrownBy
      CarriedCandidates.writeSorted(Vector.fill(2048)(record))
    val packed = CarriedCandidates.writeSorted(Vector(record))
    the[IllegalArgumentException] thrownBy
      CarriedCandidates.merge(Vector.fill(2048)(packed), 2048, MetricType.L2)
  }
}
