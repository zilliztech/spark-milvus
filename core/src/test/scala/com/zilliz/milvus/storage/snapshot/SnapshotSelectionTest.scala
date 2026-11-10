package com.zilliz.milvus.storage.snapshot

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SnapshotSelectionTest extends AnyFunSuite with Matchers {
  test(
    "selection returns input positions without confusing repeated ids across partitions"
  ) {
    val selected = SnapshotSelection.select(
      "fixed",
      Seq(20L, 21L),
      Seq(
        SnapshotSelection.Segment(30L, 20L, hasData = true),
        SnapshotSelection.Segment(30L, 21L, hasData = true),
        SnapshotSelection.Segment(50L, -1L, hasData = false)
      ),
      Seq(21L),
      Seq(30L)
    )
    selected.segmentPositions shouldBe Set(1, 2)
    selected.partitionIds shouldBe Seq(21L)
  }

  test(
    "an empty selection preserves declared partitions and every input position"
  ) {
    val selected = SnapshotSelection.select(
      "fixed",
      Seq(20L, 21L),
      Seq(SnapshotSelection.Segment(30L, 20L, hasData = true)),
      Seq.empty,
      Seq.empty
    )
    selected.segmentPositions shouldBe Set(0)
    selected.partitionIds shouldBe Seq(20L, 21L)
  }
}
