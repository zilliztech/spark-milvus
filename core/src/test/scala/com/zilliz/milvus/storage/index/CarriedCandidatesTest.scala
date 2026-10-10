package com.zilliz.milvus.storage.index

import java.nio.charset.StandardCharsets.UTF_8

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
  ) =
    CarriedCandidates.of(
      entries.map { case (unit, row, score, text) =>
        Candidate(0, unit, row, score) -> text.getBytes(UTF_8)
      },
      metric
    )

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
    "a batch of float vectors excludes its null rows and starts where it is told"
  ) {
    val allocator = new RootAllocator()
    try {
      val batch = VectorBatch.ofFloats(
        Array(Array(1f, 2f), null, Array(3f, 4f), null),
        3,
        2,
        100L,
        allocator
      )
      try {
        batch.rows shouldBe 3
        batch.firstRow shouldBe 100L
        batch.excluded.get(1) shouldBe true
        batch.visibleRows shouldBe 2
        batch.base.buffer.getFloat(0) shouldBe 1f
        batch.base.buffer.getFloat(2 * 2 * 4 + 4) shouldBe 4f
      } finally batch.close()
    } finally allocator.close()
  }
}
