package com.zilliz.milvus.storage.snapshot

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.CollectionSchema

class SnapshotTest extends AnyFunSuite with Matchers {

  private val data20a = Segment.v2(
    id = 30L,
    partitionId = 20L,
    rows = 1L,
    columnGroups = Seq(V2ColumnGroup(Seq(100L), Seq("p20-a"), Seq(1L)))
  )
  private val data20b = Segment.v2(
    id = 31L,
    partitionId = 20L,
    rows = 1L,
    columnGroups = Seq(V2ColumnGroup(Seq(100L), Seq("p20-b"), Seq(1L)))
  )
  private val data21 = Segment.v2(
    id = 40L,
    partitionId = 21L,
    rows = 1L,
    columnGroups = Seq(V2ColumnGroup(Seq(100L), Seq("p21"), Seq(1L)))
  )
  private val l0For20 = Segment.v2(
    id = 50L,
    partitionId = 20L,
    rows = 0L,
    columnGroups = Seq.empty,
    deltaLogs = Seq(DeltaLogFile(1L, "delete-20", 1L))
  )
  private val globalL0 = Segment.v2(
    id = 51L,
    partitionId = -1L,
    rows = 0L,
    columnGroups = Seq.empty,
    deltaLogs = Seq(DeltaLogFile(2L, "delete-all", 1L))
  )

  private def snapshot: Snapshot = Snapshot(
    name = "fixed",
    collectionId = 10L,
    createdAt = Some(1L),
    schema = CollectionSchema(name = "c"),
    partitionIds = Seq(20L, 21L),
    segments = Seq(data20a, data20b, data21, l0For20, globalL0),
    origin = SnapshotOrigin.Options,
    bucket = ""
  )

  test("multi-value partition selection preserves snapshot order") {
    val selected = snapshot.narrow(Seq(21L, 20L), Seq.empty)

    selected.partitionIds shouldBe Seq(20L, 21L)
    selected.segments shouldBe snapshot.segments
  }

  test("segment selection retains partition and collection-wide deletes") {
    val selected = snapshot.narrow(Seq.empty, Seq(31L, 40L))

    selected.partitionIds shouldBe Seq(20L, 21L)
    selected.segments shouldBe Seq(data20b, data21, l0For20, globalL0)
  }

  test("partition and segment selectors intersect") {
    val selected = snapshot.narrow(Seq(20L), Seq(31L))

    selected.partitionIds shouldBe Seq(20L)
    selected.segments shouldBe Seq(data20b, l0For20, globalL0)
  }

  test("unknown partition and segment ids fail planning") {
    val partitionError = intercept[IllegalArgumentException] {
      snapshot.narrow(Seq(99L), Seq.empty)
    }
    partitionError.getMessage should include("99")

    val segmentError = intercept[IllegalArgumentException] {
      snapshot.narrow(Seq(20L), Seq(40L))
    }
    segmentError.getMessage should include("40")
    segmentError.getMessage should include("20")
  }
}
