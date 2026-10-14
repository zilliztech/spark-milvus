package com.zilliz.milvus.storage.vector

import java.nio.charset.StandardCharsets.UTF_8
import java.util.BitSet

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
}
