package com.zilliz.milvus.storage.read.plan

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.exec.PhysicalColumnGroup
import com.zilliz.milvus.storage.snapshot.{
  DeleteFiles,
  DeltaLogFile,
  Segment,
  SegmentLayout,
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin
}
import com.zilliz.milvus.storage.snapshot.json.ManifestItemJson
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** `ReadPlan.of`: the planned read shapes.
  */
class ReadPlanTest extends AnyFunSuite with Matchers {

  private val schema = CollectionSchema(
    name = "c",
    fields = Seq(
      FieldSchema(
        fieldID = 100,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      )
    )
  )

  private val v3Props = Map("fs.bucket_name" -> "raw")
  private val properties: Int => Map[String, String] = _ => v3Props

  private def snapshotOf(
      v3: Seq[ManifestItemJson] = Seq.empty
  ): Snapshot =
    SnapshotCatalog
      .fromLists(
        name = "s",
        collectionId = 10L,
        createdAt = None,
        partitionIds = Seq(20L),
        schemaBytes = schema.toByteArray,
        v3Items = v3,
        bucket = "canonical",
        origin = SnapshotOrigin.Options
      )
      .fold(e => throw e, identity)

  private val v3Item =
    ManifestItemJson(
      30L,
      """{"ver":7,"base_path":"files/insert_log/10/20/30"}"""
    )

  private def log(id: Long) = DeltaLogFile(id, s"files/delta/$id", 5L)

  private def snapshotAt(version: Long, rows: Option[Long] = None): Snapshot = {
    val initial = snapshotOf(v3 = Seq(v3Item))
    initial.copy(segments =
      initial.segments.map(
        _.copy(
          layout = SegmentLayout.Manifest("files/insert_log/10/20/30", version),
          rows = rows
        )
      )
    )
  }

  test("a V3 segment becomes a manifest task with the raw properties") {
    val plan = ReadPlan.of(
      snapshotOf(v3 = Seq(v3Item)),
      properties,
      applyDeletes = true
    )
    plan.specs.map(_.segmentId) shouldBe Seq(30L)
    val task = plan.specs.head
    task.layout shouldBe SegmentLayout.Manifest("files/insert_log/10/20/30", 7L)
    task.partitionId shouldBe 20L
    task.properties shouldBe v3Props
    task.deletes shouldBe DeleteSource.None
    task.schemaBytes shouldBe schema.toByteArray
  }

  test(
    "a read version resolved by the listing overrides the one the snapshot lists"
  ) {
    val plan = ReadPlan.of(
      snapshotAt(-1L),
      properties,
      applyDeletes = true,
      deletes = DeleteFileListing.empty.copy(v3ReadVersions = Map(30L -> 11L))
    )
    plan.specs.head.readVersionOrLatest shouldBe 11L
  }

  test("an unpinned V3 segment cannot fall back to the executor's latest") {
    val base = "files/insert_log/10/20/30"
    val error = intercept[IllegalStateException] {
      ReadPlan.of(
        snapshotAt(-1L),
        properties,
        applyDeletes = false
      )
    }
    error.getMessage should include("segment 30")
    error.getMessage should include(base)
    error.getMessage should include("pinned manifest version")
  }

  test("version zero data segments produce no task or storage properties") {
    Seq(None, Some(0L)).foreach { rows =>
      val snapshot = snapshotAt(0L, rows)
      snapshot.dataSegments.map(_.id) shouldBe Seq(30L)
      val plan = ReadPlan.of(
        snapshot,
        _ => fail("version zero must not request storage properties"),
        applyDeletes = true
      )
      plan.isEmpty shouldBe true
      plan.totalRows shouldBe Some(0L)
    }
  }

  test("version zero skips only the initial empty data segment") {
    val snapshot = snapshotAt(0L)
    val populated = snapshotOf(v3 = Seq(v3Item)).segments.head.copy(id = 31L)
    val plan = ReadPlan.of(
      snapshot.copy(segments = snapshot.segments :+ populated),
      properties,
      applyDeletes = true
    )
    plan.specs.map(_.segmentId) shouldBe Seq(31L)
    plan.specs.head.readVersionOrLatest shouldBe 7L
  }

  test("version zero rejects a positive snapshot or listed row count") {
    Seq(
      (snapshotAt(0L, Some(1L)), DeleteFileListing.empty),
      (snapshotAt(0L), DeleteFileListing.empty.copy(v3Rows = Map(30L -> 1L)))
    ).foreach { case (snapshot, listing) =>
      val error = intercept[IllegalArgumentException] {
        ReadPlan.of(
          snapshot,
          properties,
          applyDeletes = true,
          deletes = listing
        )
      }
      error.getMessage should include("segment 30")
      error.getMessage should include(
        "manifest version 0 but a positive row count"
      )
    }
  }

  test(
    "a task carries its resolved L0 files followed by its own delete files"
  ) {
    val listing = DeleteFileListing(
      v3BySegment = Map(30L -> Seq(log(3)), 31L -> Seq(log(4))),
      inheritedBySegment = Map(
        30L -> Seq(log(1), log(2)),
        31L -> Seq(log(1), log(2)),
        99L -> Seq(log(9))
      ),
      v3ReadVersions = Map.empty
    )
    val plan = ReadPlan.of(
      snapshotOf(v3 =
        Seq(
          v3Item,
          ManifestItemJson(
            31L,
            """{"ver":7,"base_path":"files/insert_log/10/20/31"}"""
          )
        )
      ),
      properties,
      applyDeletes = true,
      deletes = listing
    )
    plan.specs.map(_.deletes) shouldBe Seq(
      DeleteSource.Files(Seq(log(1), log(2), log(3))),
      DeleteSource.Files(Seq(log(1), log(2), log(4)))
    )
    plan.partitionsApplyingDeletes shouldBe 2
  }

  test("a segment with no delete file of any kind carries DeleteSource.None") {
    val listing = DeleteFileListing(
      v3BySegment = Map.empty,
      inheritedBySegment = Map(99L -> Seq(log(9))), // another segment
      v3ReadVersions = Map.empty
    )
    val plan = ReadPlan.of(
      snapshotOf(v3 = Seq(v3Item)),
      properties,
      applyDeletes = true,
      deletes = listing
    )
    plan.specs.head.deletes shouldBe DeleteSource.None
  }

  test("applyDeletes=false ships no delete file") {
    val listing = DeleteFileListing(
      v3BySegment = Map(30L -> Seq(log(3)), 31L -> Seq(log(4))),
      inheritedBySegment = Map(30L -> Seq(log(2)), 31L -> Seq(log(2))),
      v3ReadVersions = Map.empty
    )
    val plan = ReadPlan.of(
      snapshotOf(v3 =
        Seq(
          v3Item,
          ManifestItemJson(
            31L,
            """{"ver":7,"base_path":"files/insert_log/10/20/31"}"""
          )
        )
      ),
      properties,
      applyDeletes = false,
      deletes = listing
    )
    plan.specs.map(_.deletes) shouldBe Seq(DeleteSource.None, DeleteSource.None)
    plan.partitionsApplyingDeletes shouldBe 0
  }

  test("requested field ids are carried by every storage-line task") {
    val plan = ReadPlan.of(
      snapshotOf(v3 =
        Seq(
          v3Item,
          ManifestItemJson(
            31L,
            """{"ver":7,"base_path":"files/insert_log/10/20/31"}"""
          )
        )
      ),
      properties,
      applyDeletes = false,
      neededFieldIds = Seq(103L, 100L)
    )

    plan.specs.map(_.neededFieldIds) shouldBe Seq(
      Seq(103L, 100L),
      Seq(103L, 100L)
    )
  }

  test("resource limits are carried by every storage-line task") {
    val limits = ReadLimits(1024, 8388608L, 67108864L)
    val plan = ReadPlan.of(
      snapshotOf(v3 =
        Seq(
          v3Item,
          ManifestItemJson(
            31L,
            """{"ver":7,"base_path":"files/insert_log/10/20/31"}"""
          )
        )
      ),
      properties,
      applyDeletes = false,
      limits = limits
    )

    plan.specs.map(_.limits) shouldBe Seq(limits, limits)
  }

  test(
    "the properties of a line are asked for only when a segment of that line exists"
  ) {
    var asked = Seq.empty[Int]
    val counting: Int => Map[String, String] = { v =>
      asked = asked :+ v
      properties(v)
    }
    ReadPlan.of(
      snapshotOf(v3 =
        Seq(
          v3Item,
          ManifestItemJson(
            33L,
            """{"ver":7,"base_path":"files/insert_log/10/20/33"}"""
          )
        )
      ),
      counting,
      applyDeletes = true
    )
    asked shouldBe Seq(3)
  }

  test("a layout that contradicts the storage version is an error") {
    val wrong = Segment(
      id = 40L,
      partitionId = 20L,
      storageVersion = 3,
      rows = None,
      layout = SegmentLayout.PhysicalColumnGroups(
        Seq(PhysicalColumnGroup(Seq("100"), Seq("data.parquet"), Seq(1L))),
        "{\"fields\":[]}",
        Map(100L -> "100"),
        Map.empty
      ),
      deletes = DeleteFiles.Empty
    )
    val snapshot = snapshotOf().copy(segments = Seq(wrong))
    val e = intercept[IllegalStateException](
      ReadPlan.of(snapshot, properties, applyDeletes = true)
    )
    e.getMessage should include(
      "storage_version 3 but carries a column-group layout"
    )
  }
}
