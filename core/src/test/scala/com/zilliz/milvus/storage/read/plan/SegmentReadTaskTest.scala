package com.zilliz.milvus.storage.read.plan

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.snapshot.{SegmentIndex, SegmentIndexes}
import com.zilliz.milvus.storage.snapshot.DeltaLogFile
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup

class SegmentReadTaskTest extends AnyFunSuite with Matchers {

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

  private def v2Task = SegmentReadTask(
    segmentId = 451L,
    partitionId = 7L,
    layout = SegmentLayout.ColumnGroups(Seq(group)),
    schemaBytes = Array[Byte](1, 2, 3),
    properties = Map("fs.storage_type" -> "remote", "fs.bucket_name" -> "b")
  )

  // Shipping this object to an executor is the whole reason it exists, so a
  // field that cannot survive the trip is a defect, not an inconvenience.
  test("a V2 task survives serialization") {
    val copy = roundTrip(v2Task)
    copy.segmentId shouldBe 451L
    copy.partitionId shouldBe 7L
    copy.properties shouldBe v2Task.properties
    copy.dataFiles shouldBe group.filePaths
    copy.expectedRows shouldBe Some(4500L)
  }

  test("a manifest task survives serialization") {
    val task = v2Task.copy(
      layout =
        SegmentLayout.Manifest("files/insert_log/1/2/3", readVersion = 11L)
    )
    val copy = roundTrip(task)
    copy.layout shouldBe SegmentLayout.Manifest("files/insert_log/1/2/3", 11L)
  }

  test(
    "persisted index metadata and snapshot rows survive task serialization"
  ) {
    val index = SegmentIndex(
      1L,
      7L,
      451L,
      101L,
      469076449917763071L,
      469076449917967340L,
      "v_hnsw",
      Map("index_type" -> "HNSW", "metric_type" -> "COSINE"),
      Vector("files/index_files/build/1/7/451/_mem.index.bin"),
      4500L,
      4096L,
      1L,
      Some(10),
      None
    )
    val task = v2Task.copy(
      layout = SegmentLayout.Manifest("files/insert_log/1/7/451", 11L),
      indexes = SegmentIndexes.Available(Vector(index)),
      snapshotRows = Some(4500L)
    )
    val copy = roundTrip(task)
    copy.indexes shouldBe task.indexes
    copy.expectedRows shouldBe Some(4500L)
    copy.readVersionOrLatest shouldBe 11L
  }

  test("a materialized delete plan survives serialization") {
    val task = v2Task.copy(
      deletes = DeleteSource.Materialized(
        DeletePlan.fromLongPks(Map(1L -> 100L, 2L -> 200L))
      )
    )
    val copy = roundTrip(task)
    copy.appliesDeletes shouldBe true
    copy.deletes match {
      case DeleteSource.Materialized(plan) =>
        plan.containsLongPk(1L, 99L) shouldBe true
        plan.containsLongPk(3L, 99L) shouldBe false
      case other => fail(s"expected a materialized plan, got $other")
    }
  }

  test("delta log files survive serialization") {
    val task = v2Task.copy(
      deletes = DeleteSource.Files(
        Seq(
          DeltaLogFile(1L, "files/_delta/1", 10L),
          DeltaLogFile(2L, "files/_delta/2", 20L)
        )
      )
    )
    roundTrip(task).deletes shouldBe task.deletes
  }

  // A manifest read does not know its row count on the driver, and the reader
  // uses expectedRows as its short-read guard. Reporting zero there would turn
  // "unknown" into "empty".
  test("a manifest layout states no row count and no files") {
    val task =
      v2Task.copy(layout = SegmentLayout.Manifest("files/insert_log/1/2/3"))
    task.expectedRows shouldBe None
    task.dataFiles shouldBe empty
  }

  test("appliesDeletes is false when the source says nothing is deleted") {
    v2Task.appliesDeletes shouldBe false
    v2Task
      .copy(deletes = DeleteSource.Materialized(DeletePlan.empty))
      .appliesDeletes shouldBe false
    v2Task
      .copy(deletes = DeleteSource.Files(Seq.empty))
      .appliesDeletes shouldBe false
  }

  test("a plan sums the row counts its partitions state") {
    val plan = ReadPlan(Seq(v2Task, v2Task))
    plan.totalRows shouldBe Some(9000L)
    plan.partitionsApplyingDeletes shouldBe 0
  }

  // One unknown makes the total unknown: a partial sum would be read as the
  // table's size and fed to the optimizer.
  test("a plan reports no total when any partition cannot state one") {
    val plan = ReadPlan(
      Seq(
        v2Task,
        v2Task.copy(layout = SegmentLayout.Manifest("files/insert_log/1/2/4"))
      )
    )
    plan.totalRows shouldBe None
  }

  test("an empty plan totals zero rather than unknown") {
    ReadPlan(Seq.empty).totalRows shouldBe Some(0L)
    ReadPlan(Seq.empty).isEmpty shouldBe true
  }

  test("a plan counts the partitions that will evaluate deletes") {
    val withDeletes = v2Task.copy(
      deletes = DeleteSource.Materialized(DeletePlan.fromLongPks(Map(1L -> 1L)))
    )
    ReadPlan(
      Seq(v2Task, withDeletes, withDeletes)
    ).partitionsApplyingDeletes shouldBe 2
  }
}
