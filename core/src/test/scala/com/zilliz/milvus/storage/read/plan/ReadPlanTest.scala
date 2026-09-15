package com.zilliz.milvus.storage.read.plan

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.snapshot.{
  DeltaLogFile,
  Segment,
  SegmentLayout,
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin,
  V2ColumnGroup
}
import com.zilliz.milvus.storage.snapshot.json.ManifestItemJson
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** `ReadPlan.of`: the shapes docs/design/architecture/read.html 5.1 and work
  * items 12 and 13 name.
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
  private val v2Props = Map("fs.bucket_name" -> "canonical")
  private val properties: Int => Map[String, String] = {
    case 2 => v2Props
    case _ => v3Props
  }

  private def snapshotOf(
      v3: Seq[ManifestItemJson] = Seq.empty,
      v2: Seq[Segment] = Seq.empty
  ): Snapshot =
    SnapshotCatalog
      .fromLists(
        name = "s",
        collectionId = 10L,
        createdAt = None,
        partitionIds = Seq(20L),
        schemaBytes = schema.toByteArray,
        v3Items = v3,
        v2Segments = v2,
        bucket = "canonical",
        origin = SnapshotOrigin.Options
      )
      .fold(e => throw e, identity)

  private val v3Item =
    ManifestItemJson(
      30L,
      """{"ver":7,"base_path":"files/insert_log/10/20/30"}"""
    )

  private def v2Segment(id: Long, groups: Seq[V2ColumnGroup]): Segment =
    Segment.v2(id = id, partitionId = 20L, rows = 1L, columnGroups = groups)

  private val group = V2ColumnGroup(
    fieldIds = Seq(100L),
    filePaths = Seq("files/insert_log/10/20/31/100/1.parquet"),
    fileRowCounts = Seq(1L)
  )

  private def log(id: Long) = DeltaLogFile(id, s"files/delta/$id", 5L)

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
      snapshotOf(v3 = Seq(ManifestItemJson(30L, "files/insert_log/10/20/30"))),
      properties,
      applyDeletes = true,
      deletes = DeleteFileListing.empty.copy(v3ReadVersions = Map(30L -> 11L))
    )
    plan.specs.head.readVersionOrLatest shouldBe 11L
  }

  test(
    "a task names the delete files it applies: the collection's L0 files, its partition's, then its own"
  ) {
    val listing = DeleteFileListing(
      v3BySegment = Map(30L -> Seq(log(3))),
      v2BySegment = Map(31L -> Seq(log(4))),
      inheritedByPartition = Map(
        -1L -> Seq(log(1)),
        20L -> Seq(log(2)),
        21L -> Seq(log(9))
      ),
      v3ReadVersions = Map.empty
    )
    val plan = ReadPlan.of(
      snapshotOf(v3 = Seq(v3Item), v2 = Seq(v2Segment(31L, Seq(group)))),
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
      v2BySegment = Map.empty,
      inheritedByPartition = Map(21L -> Seq(log(9))), // another partition
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
      v3BySegment = Map(30L -> Seq(log(3))),
      v2BySegment = Map(31L -> Seq(log(4))),
      inheritedByPartition = Map(20L -> Seq(log(2))),
      v3ReadVersions = Map.empty
    )
    val plan = ReadPlan.of(
      snapshotOf(v3 = Seq(v3Item), v2 = Seq(v2Segment(31L, Seq(group)))),
      properties,
      applyDeletes = false,
      deletes = listing
    )
    plan.specs.map(_.deletes) shouldBe Seq(DeleteSource.None, DeleteSource.None)
    plan.partitionsApplyingDeletes shouldBe 0
  }

  test(
    "a V2 segment becomes a column-group task with the canonical properties"
  ) {
    val plan = ReadPlan.of(
      snapshotOf(v2 = Seq(v2Segment(31L, Seq(group)))),
      properties,
      applyDeletes = true
    )
    val task = plan.specs.head
    task.layout shouldBe SegmentLayout.ColumnGroups(Seq(group))
    task.properties shouldBe v2Props
    plan.totalRows shouldBe Some(1L)
  }

  test("a delete-only V2 segment is no task, and V3 tasks come first") {
    val l0 = Segment.v2(
      id = 32L,
      partitionId = 20L,
      rows = 0L,
      columnGroups = Seq.empty,
      deltaLogs = Seq(DeltaLogFile(1L, "files/delta_log/10/20/32/1", 5L))
    )
    val plan = ReadPlan.of(
      snapshotOf(v3 = Seq(v3Item), v2 = Seq(l0, v2Segment(31L, Seq(group)))),
      properties,
      applyDeletes = true
    )
    plan.specs.map(_.segmentId) shouldBe Seq(30L, 31L)
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
      snapshotOf(v2 =
        Seq(v2Segment(31L, Seq(group)), v2Segment(33L, Seq(group)))
      ),
      counting,
      applyDeletes = true
    )
    asked shouldBe Seq(2)
  }

  test("a layout that contradicts the storage version is an error") {
    val wrong = Segment(
      id = 40L,
      partitionId = 20L,
      storageVersion = 3,
      rows = None,
      layout = SegmentLayout.ColumnGroups(Seq(group)),
      deletes = com.zilliz.milvus.storage.snapshot.DeleteFiles.Listed(Seq.empty)
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
