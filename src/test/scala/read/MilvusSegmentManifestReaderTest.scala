package com.zilliz.spark.connector.read

import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[MilvusSegmentManifestReader]] against a real milvus-produced
  * per-segment AVRO.
  *
  * The fixture `src/test/data/seg_manifest.avro` was captured from local minio:
  * `a-bucket/files/snapshots/465602255560377587/manifests/465602255561974530/465602255560578628.avro`
  * It describes one StorageV2 segment of the `hello_spark_milvus` collection
  * with schema:
  *   - fieldID 100: ID (Int64, primary key)
  *   - fieldID 101: varchar (VarChar)
  *   - fieldID 102: embeddings (FloatVector dim=768)
  *   - fieldID 0: RowID (Int64, system)
  *   - fieldID 1: Timestamp (Int64, system)
  *
  * Its segment-level `group_field_id_list` (pulled from any one of the 3
  * parquet files in /files/insert_log/.../{0,1,102}/) is `"100,0,1;101;102"`.
  */
class MilvusSegmentManifestReaderTest extends AnyFunSuite with Matchers {

  private val avroBytes: Array[Byte] =
    Files.readAllBytes(Paths.get("src/test/data/seg_manifest.avro"))

  // Pre-captured from the parquet kv-metadata — avoids needing minio at test time.
  private val expectedGroupFieldIdList: Seq[Seq[Long]] =
    Seq(Seq(100L, 0L, 1L), Seq(101L), Seq(102L))

  test("parse decodes one ManifestEntry with expected top-level fields") {
    val result = MilvusSegmentManifestReader.parse(avroBytes)
    result shouldBe a[Right[_, _]]

    val entry = result.toOption.get
    entry.segmentId shouldBe 465602255560578628L
    entry.partitionId shouldBe 465602255560377588L
    entry.numOfRows shouldBe 20480L
    entry.storageVersion shouldBe 2L // StorageV2
  }

  test("parse exposes binlog entries as (slotFieldId, binlogs)") {
    val entry = MilvusSegmentManifestReader.parse(avroBytes).toOption.get
    entry.binlogFiles should have size 3

    val slots = entry.binlogFiles.map(_.slotFieldId)
    slots shouldBe Seq(0L, 1L, 102L)

    // Each group has exactly one parquet file in this fixture.
    entry.binlogFiles.foreach { afb =>
      afb.binlogs should have size 1
    }

    val firstBinlog = entry.binlogFiles.head.binlogs.head
    firstBinlog.logId shouldBe 465602255560688629L
    firstBinlog.logPath shouldBe
      "files/insert_log/465602255560377587/465602255560377588/465602255560578628/0/465602255560688629"
    firstBinlog.entriesNum should be > 0L
  }

  test(
    "toV2SegmentInfo joins AVRO binlog order with group_field_id_list positionally"
  ) {
    val entry = MilvusSegmentManifestReader.parse(avroBytes).toOption.get
    val result =
      MilvusSegmentManifestReader.toV2SegmentInfo(
        entry,
        expectedGroupFieldIdList
      )
    result shouldBe a[Right[_, _]]

    val seg = result.toOption.get
    seg.segmentId shouldBe 465602255560578628L
    seg.storageVersion shouldBe 2L
    seg.columnGroups should have size 3

    // Group 0: /0/ directory carries real fields {100, 0, 1}.
    seg.columnGroups(0).fieldIds shouldBe Seq(100L, 0L, 1L)
    seg.columnGroups(0).filePaths shouldBe Seq(
      "files/insert_log/465602255560377587/465602255560377588/465602255560578628/0/465602255560688629"
    )
    seg.columnGroups(0).fileRowCounts.size shouldBe 1
    seg.columnGroups(0).fileRowCounts.head should be > 0L

    // Group 1: /1/ directory carries only varchar (fid=101).
    seg.columnGroups(1).fieldIds shouldBe Seq(101L)
    seg.columnGroups(1).filePaths shouldBe Seq(
      "files/insert_log/465602255560377587/465602255560377588/465602255560578628/1/465602255560688630"
    )

    // Group 2: /102/ directory carries only embeddings (fid=102).
    seg.columnGroups(2).fieldIds shouldBe Seq(102L)
    seg.columnGroups(2).filePaths shouldBe Seq(
      "files/insert_log/465602255560377587/465602255560377588/465602255560578628/102/465602255560688631"
    )
  }

  test("toV2SegmentInfo rejects non-StorageV2 entries") {
    val entry = MilvusSegmentManifestReader.parse(avroBytes).toOption.get
    val bogus = entry.copy(storageVersion = 0L) // StorageV1
    val result = MilvusSegmentManifestReader.toV2SegmentInfo(bogus, Seq.empty)
    result shouldBe a[Left[_, _]]
    result.left.toOption.get.getMessage should include("storageVersion=2")
  }

  test(
    "serialize/deserialize round-trip keeps fieldIds as unboxed-safe Longs"
  ) {
    // Regression: Jackson erases Seq[Long] → Seq[Object] and small JSON
    // integers come back as java.lang.Integer, causing a ClassCastException
    // when downstream code maps over the Seq (Scala emits unboxToLong). The
    // DTO stores JsonNode and converts via JsonTypeConverter.toLong on the
    // way back to hide this from callers.
    val seg = V2SegmentInfo(
      segmentId = 465602255560578628L,
      partitionId = 465602255560377588L,
      numOfRows = 20480L,
      storageVersion = 2L,
      columnGroups = Seq(
        V2ColumnGroup(
          fieldIds = Seq(100L, 0L, 1L),
          filePaths = Seq("files/insert_log/.../0/100"),
          fileRowCounts = Seq(20480L)
        ),
        V2ColumnGroup(
          fieldIds = Seq(103L),
          filePaths = Seq("p/103"),
          fileRowCounts = Seq(20480L)
        )
      )
    )
    val json = MilvusSnapshotReader.serializeV2Segments(Seq(seg))
    val roundTripped =
      MilvusSnapshotReader.deserializeV2Segments(json).toOption.get
    roundTripped should have size 1
    val got = roundTripped.head
    got.segmentId shouldBe seg.segmentId
    got.columnGroups(0).fieldIds shouldBe Seq(100L, 0L, 1L)
    got.columnGroups(0).fileRowCounts shouldBe Seq(20480L)
    // Exercise the exact pattern that failed at runtime — mapping over the
    // rehydrated Seq[Long]. Must not throw ClassCastException.
    val asStrings = got.columnGroups(0).fieldIds.map(_.toString)
    asStrings shouldBe Seq("100", "0", "1")
    val rcStrings = got.columnGroups(0).fileRowCounts.map(_.toString)
    rcStrings shouldBe Seq("20480")
  }

  test("toV2SegmentInfo fails when group count disagrees with AVRO") {
    val entry = MilvusSegmentManifestReader.parse(avroBytes).toOption.get
    // AVRO has 3 binlog groups; feed a 2-group list.
    val result = MilvusSegmentManifestReader.toV2SegmentInfo(
      entry,
      Seq(Seq(100L, 0L, 1L), Seq(101L))
    )
    result shouldBe a[Left[_, _]]
    result.left.toOption.get.getMessage should include("mismatch")
  }
}
