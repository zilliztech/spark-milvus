package com.zilliz.milvus.storage.read.plan

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.delete.MilvusDeletePlan
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import com.zilliz.milvus.storage.snapshot.SegmentLayout

class InputSpecTest extends AnyFunSuite with Matchers {

  private def roundTrip[A](value: A): A = {
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    try out.writeObject(value)
    finally out.close()
    val in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    try in.readObject().asInstanceOf[A]
    finally in.close()
  }

  private val group = V2ColumnGroup(
    fieldIds = Seq(100L, 0L, 1L),
    filePaths =
      Seq("files/insert_log/1/2/3/100/0", "files/insert_log/1/2/3/100/1"),
    fileRowCounts = Seq(3000L, 1500L),
    slotFieldId = 100L
  )

  private def packedSpec = InputSpec(
    segmentId = 451L,
    partitionId = 7L,
    layout = SegmentLayout.ColumnGroups(Seq(group)),
    schemaBytes = Array[Byte](1, 2, 3),
    properties = Map("fs.storage_type" -> "remote", "fs.bucket_name" -> "b")
  )

  // Shipping this object to an executor is the whole reason it exists, so a
  // field that cannot survive the trip is a defect, not an inconvenience.
  test("a packed spec survives serialization") {
    val copy = roundTrip(packedSpec)
    copy.segmentId shouldBe 451L
    copy.partitionId shouldBe 7L
    copy.properties shouldBe packedSpec.properties
    copy.dataFiles shouldBe group.filePaths
    copy.expectedRows shouldBe Some(4500L)
  }

  test("a manifest spec survives serialization") {
    val spec = packedSpec.copy(
      layout =
        SegmentLayout.Manifest("files/insert_log/1/2/3", readVersion = 11L)
    )
    val copy = roundTrip(spec)
    copy.layout shouldBe SegmentLayout.Manifest("files/insert_log/1/2/3", 11L)
  }

  test("a materialized delete plan survives serialization") {
    val spec = packedSpec.copy(
      deletes = DeleteSource.Materialized(
        MilvusDeletePlan.fromLongPks(Map(1L -> 100L, 2L -> 200L))
      )
    )
    val copy = roundTrip(spec)
    copy.appliesDeletes shouldBe true
    copy.deletes match {
      case DeleteSource.Materialized(plan) =>
        plan.containsLongPk(1L, 99L) shouldBe true
        plan.containsLongPk(3L, 99L) shouldBe false
      case other => fail(s"expected a materialized plan, got $other")
    }
  }

  test("delta log files survive serialization") {
    val spec = packedSpec.copy(
      deletes = DeleteSource.Files(
        Seq("files/_delta/1", "files/_delta/2"),
        Seq(10L, 20L)
      )
    )
    roundTrip(spec).deletes shouldBe spec.deletes
  }

  // A manifest read does not know its row count on the driver, and the reader
  // uses expectedRows as its short-read guard. Reporting zero there would turn
  // "unknown" into "empty".
  test("a manifest layout states no row count and no files") {
    val spec =
      packedSpec.copy(layout = SegmentLayout.Manifest("files/insert_log/1/2/3"))
    spec.expectedRows shouldBe None
    spec.dataFiles shouldBe empty
  }

  test("appliesDeletes is false when the source says nothing is deleted") {
    packedSpec.appliesDeletes shouldBe false
    packedSpec
      .copy(deletes = DeleteSource.Materialized(MilvusDeletePlan.empty))
      .appliesDeletes shouldBe false
    packedSpec
      .copy(deletes = DeleteSource.Files(Seq.empty, Seq.empty))
      .appliesDeletes shouldBe false
  }

  test("delta log paths and entry counts have to be parallel") {
    an[IllegalArgumentException] should be thrownBy
      DeleteSource.Files(Seq("a", "b"), Seq(1L))
  }

  test("a plan sums the row counts its partitions state") {
    val plan = ReadPlan(Seq(packedSpec, packedSpec))
    plan.totalRows shouldBe Some(9000L)
    plan.partitionsApplyingDeletes shouldBe 0
  }

  // One unknown makes the total unknown: a partial sum would be read as the
  // table's size and fed to the optimizer.
  test("a plan reports no total when any partition cannot state one") {
    val plan = ReadPlan(
      Seq(
        packedSpec,
        packedSpec.copy(layout =
          SegmentLayout.Manifest("files/insert_log/1/2/4")
        )
      )
    )
    plan.totalRows shouldBe None
  }

  test("an empty plan totals zero rather than unknown") {
    ReadPlan(Seq.empty).totalRows shouldBe Some(0L)
    ReadPlan(Seq.empty).isEmpty shouldBe true
  }

  test("a plan counts the partitions that will evaluate deletes") {
    val withDeletes = packedSpec.copy(
      deletes =
        DeleteSource.Materialized(MilvusDeletePlan.fromLongPks(Map(1L -> 1L)))
    )
    ReadPlan(
      Seq(packedSpec, withDeletes, withDeletes)
    ).partitionsApplyingDeletes shouldBe 2
  }
}
