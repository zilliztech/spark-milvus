package com.zilliz.milvus.storage.snapshot

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.CollectionSchema

class SnapshotTest extends AnyFunSuite with Matchers {

  private def data(id: Long, partition: Long): Segment =
    Segment(
      id,
      partition,
      3,
      Some(1L),
      SegmentLayout.Manifest(s"segments/$id", 1L),
      DeleteFiles.InManifest
    )
  private val data20a = data(30L, 20L)
  private val data20b = data(31L, 20L)
  private val data21 = data(40L, 21L)

  private def snapshot: Snapshot = Snapshot(
    name = "fixed",
    collectionId = 10L,
    createdAt = Some(1L),
    schema = CollectionSchema(name = "c"),
    partitionIds = Seq(20L, 21L),
    segments = Seq(data20a, data20b, data21),
    origin = SnapshotOrigin.Options,
    bucket = ""
  )

  test("multi-value partition selection preserves snapshot order") {
    val selected = snapshot.narrow(Seq(21L, 20L), Seq.empty)

    selected.partitionIds shouldBe Seq(20L, 21L)
    selected.segments shouldBe snapshot.segments
  }

  test("segment selection preserves snapshot order and partitions") {
    val selected = snapshot.narrow(Seq.empty, Seq(31L, 40L))

    selected.partitionIds shouldBe Seq(20L, 21L)
    selected.segments shouldBe Seq(data20b, data21)
  }

  test("optimizer segment retention keeps the selected segments") {
    val selected = snapshot.retainDataSegments(Set(31L))

    selected.partitionIds shouldBe Seq(20L)
    selected.segments shouldBe Seq(data20b)

    val none = snapshot.retainDataSegments(Set.empty)
    none.partitionIds shouldBe empty
    none.segments shouldBe empty
  }

  test("partition and segment selectors intersect") {
    val selected = snapshot.narrow(Seq(20L), Seq(31L))

    selected.partitionIds shouldBe Seq(20L)
    selected.segments shouldBe Seq(data20b)
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
