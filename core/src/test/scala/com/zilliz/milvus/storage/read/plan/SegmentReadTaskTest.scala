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
import com.zilliz.milvus.storage.io.{FailingObjectStore, ObjectStore}
import com.zilliz.milvus.storage.read.exec.{DeletePlans, PhysicalColumnGroup}
import com.zilliz.milvus.storage.snapshot.{SegmentIndex, SegmentIndexes}
import com.zilliz.milvus.storage.snapshot.DeltaLogFile
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import io.milvus.grpc.schema.{DataType, FieldSchema}

object SegmentReadTaskTest {
  final case class TestDeleteReader(key: Long) extends DeleteSource.Reader {
    override def read(
        primaryKey: FieldSchema,
        bucket: String,
        store: ObjectStore
    ): DeletePlan = {
      require(primaryKey.fieldID == 100L && bucket == "b" && store != null)
      DeletePlan.fromLongPks(Map(key -> 9L))
    }
  }
}

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
    val task = v2Task.copy(limits = ReadLimits(1024, 8388608L, 67108864L))
    val copy = roundTrip(task)
    copy.segmentId shouldBe 451L
    copy.partitionId shouldBe 7L
    copy.properties shouldBe task.properties
    copy.limits shouldBe task.limits
    copy.dataFiles shouldBe group.filePaths
    copy.expectedRows shouldBe Some(4500L)
    copy.provenPhysicalRows shouldBe Some(4500L)
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
    "a deferred format decoder survives task serialization without an open store"
  ) {
    val copy = roundTrip(
      v2Task.copy(deletes =
        DeleteSource.Deferred(
          SegmentReadTaskTest.TestDeleteReader(42L)
        )
      )
    )
    copy.appliesDeletes shouldBe true
    val primaryKey = FieldSchema(
      fieldID = 100L,
      dataType = DataType.Int64,
      isPrimaryKey = true
    )
    val store = new FailingObjectStore(new java.io.IOException("unused"))
    val plan = DeletePlans.of(copy, Some(primaryKey), store)
    plan.containsLongPk(42L, 8L) shouldBe true
    plan.containsLongPk(43L, 8L) shouldBe false
    an[UnsupportedOperationException] should be thrownBy copy.deletePlan
  }

  test(
    "resolved physical groups retain row bounds and names after serialization"
  ) {
    val physical = SegmentLayout.PhysicalColumnGroups(
      Seq(
        PhysicalColumnGroup(Seq("key"), Seq("data/a", "data/b"), Seq(3L, 2L))
      ),
      "{\"fields\":[]}",
      Map(100L -> "key"),
      Map("id" -> "key")
    )
    val task = roundTrip(v2Task.copy(layout = physical))
    task.layout shouldBe physical
    task.dataFiles shouldBe Seq("data/a", "data/b")
    task.expectedRows shouldBe Some(5L)
    task.provenPhysicalRows shouldBe Some(5L)
    task.readVersionOrLatest shouldBe -1L
    task
      .copy(layout =
        physical.copy(groups =
          physical.groups :+
            PhysicalColumnGroup(Seq("value"), Seq("data/value"), Seq(4L))
        )
      )
      .provenPhysicalRows shouldBe None
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

  test("a persisted index's row count sizes a segment the snapshot did not") {
    val indexed = v2Task
      .copy(
        segmentId = 7L,
        layout = SegmentLayout.Manifest("segments/7", 3L)
      )
      .copy(indexes =
        SegmentIndexes.Available(
          Vector(
            SegmentIndex(
              collectionId = 1L,
              partitionId = 1L,
              segmentId = 7L,
              fieldId = 100L,
              indexId = 1L,
              buildId = 1L,
              name = "v",
              parameters = Map("index_type" -> "HNSW"),
              filePaths = Vector("index/7"),
              rowCount = 25L,
              serializedSize = 1L,
              indexVersion = 1L,
              currentIndexVersion = None,
              indexStorePathVersion = None
            )
          )
        )
      )

    indexed.knownRows shouldBe Some(25L)
    indexed.copy(indexes = SegmentIndexes.Unknown).knownRows shouldBe None
    indexed.expectedRows shouldBe None
    indexed.provenPhysicalRows shouldBe None
    indexed.copy(snapshotRows = Some(30L)).knownRows shouldBe Some(30L)
  }

  test(
    "physical address bounds use complete snapshot counts without narrowing"
  ) {
    val manifest =
      v2Task.copy(layout = SegmentLayout.Manifest("segments/451", 3L))
    Seq(0L, 4500L, 1L << 32, (1L << 32) + 1L, Long.MaxValue).foreach { rows =>
      manifest.copy(snapshotRows = Some(rows)).provenPhysicalRows shouldBe Some(
        rows
      )
    }
    manifest.provenPhysicalRows shouldBe None
    v2Task.copy(snapshotRows = Some(-1L)).provenPhysicalRows shouldBe None
    v2Task.copy(snapshotRows = Some(5000L)).provenPhysicalRows shouldBe Some(
      5000L
    )
  }

  test("physical address bounds require complete and agreeing column groups") {
    def rows(groups: V2ColumnGroup*): Option[Long] =
      v2Task
        .copy(layout = SegmentLayout.ColumnGroups(groups))
        .provenPhysicalRows

    val sameTotal = group.copy(
      fieldIds = Seq(102L),
      filePaths = Seq("other-column"),
      fileRowCounts = Seq(4500L)
    )
    rows(group, sameTotal) shouldBe Some(4500L)
    rows(group, sameTotal.copy(fileRowCounts = Seq(4501L))) shouldBe None
    rows(group.copy(fileRowCounts = Seq.empty)) shouldBe None
    rows(group.copy(fileRowCounts = Seq(4500L))) shouldBe None
    rows(group.copy(fileRowCounts = Seq(4500L, 0L, 0L))) shouldBe None
    rows(group.copy(fileRowCounts = Seq(4501L, -1L))) shouldBe None
    rows(group.copy(fileRowCounts = Seq(Long.MaxValue, 1L))) shouldBe None
    rows(group.copy(fileRowCounts = Seq(Long.MaxValue, 0L))) shouldBe Some(
      Long.MaxValue
    )
    rows(
      group.copy(filePaths = Seq.empty, fileRowCounts = Seq.empty)
    ) shouldBe Some(0L)
    rows() shouldBe None
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
