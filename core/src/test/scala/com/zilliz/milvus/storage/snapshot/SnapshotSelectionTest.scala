package com.zilliz.milvus.storage.snapshot

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SnapshotSelectionTest extends AnyFunSuite with Matchers {
  test(
    "selection returns every input position in the selected partition and global deletes"
  ) {
    val selected = SnapshotSelection.select(
      "fixed",
      Seq(20L, 21L),
      Seq(
        SnapshotSelection.Segment(20L, hasData = true),
        SnapshotSelection.Segment(21L, hasData = true),
        SnapshotSelection.Segment(-1L, hasData = false),
        SnapshotSelection.Segment(21L, hasData = true),
        SnapshotSelection.Segment(20L, hasData = false)
      ),
      Seq(21L)
    )
    selected.segmentPositions shouldBe Set(1, 2, 3)
    selected.partitionIds shouldBe Seq(21L)
  }

  test(
    "an empty selection preserves declared partitions and every input position"
  ) {
    val selected = SnapshotSelection.select(
      "fixed",
      Seq(20L, 21L),
      Seq(SnapshotSelection.Segment(20L, hasData = true)),
      Seq.empty
    )
    selected.segmentPositions shouldBe Set(0)
    selected.partitionIds shouldBe Seq(20L, 21L)
  }
}
