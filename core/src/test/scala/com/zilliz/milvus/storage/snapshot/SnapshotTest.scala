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

  test("optimizer segment retention keeps the selected segments") {
    val selected = snapshot.retainDataSegments(Set(31L))

    selected.partitionIds shouldBe Seq(20L)
    selected.segments shouldBe Seq(data20b)

    val none = snapshot.retainDataSegments(Set.empty)
    none.partitionIds shouldBe empty
    none.segments shouldBe empty
  }

  test(
    "segment pruning retains partition and collection-wide deletes in snapshot order"
  ) {
    val deletes20 = data(50L, 20L).copy(isDeleteOnly = true)
    val deletes21 = data(51L, 21L).copy(isDeleteOnly = true)
    val deletesAll = data(52L, -1L).copy(isDeleteOnly = true)
    val withDeletes = snapshot.copy(segments =
      Seq(data20a, deletes20, data21, deletes21, deletesAll, data20b)
    )

    val selected = withDeletes.retainDataSegments(Set(30L, 31L))

    selected.partitionIds shouldBe Seq(20L)
    selected.segments shouldBe Seq(data20a, deletes20, deletesAll, data20b)
  }

}
