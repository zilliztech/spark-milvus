package com.zilliz.milvus.storage.manifest

import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[SnapshotSegmentReader]] against a real milvus-produced
  * per-segment AVRO.
  *
  * The fixture `core/src/test/data/seg_manifest.avro` was captured from local
  * minio:
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
class SnapshotSegmentReaderTest extends AnyFunSuite with Matchers {

  test(
    "all manifest versions decode exact index ids, parameters and distinct versions"
  ) {
    val index = SnapshotSegmentFixture.index()
    SnapshotSegmentSchema.supportedVersions.foreach { version =>
      val bytes = SnapshotSegmentFixture.encode(
        version = version,
        indexes = Vector(index)
      )
      val entry =
        SnapshotSegmentReader.parse(bytes, version).fold(throw _, identity)
      val expected =
        if (version == 1) index.copy(indexStorePathVersion = None) else index
      entry.indexFiles shouldBe Some(Vector(expected))
      entry.numOfRows shouldBe 2L
    }
  }

  test("all manifest versions decode the neutral statslog entries") {
    val logs = Seq(
      FieldBinlogEntry(
        100L,
        Seq(
          BinlogEntry(9L, "files/stats_log/100/9", 20L),
          BinlogEntry(7L, "files/stats_log/100/7", 10L)
        )
      )
    )
    SnapshotSegmentSchema.supportedVersions.foreach { version =>
      val entry = SnapshotSegmentReader
        .parse(
          SnapshotSegmentFixture.encode(version = version, statsLogs = logs),
          version
        )
        .toOption
        .get
      entry.statsLogFiles shouldBe logs

    }
  }

  test(
    "an explicitly empty index array is known, and truncated index bytes fail"
  ) {
    val bytes = SnapshotSegmentFixture.encode()
    SnapshotSegmentReader.parse(bytes, 4).toOption.get.indexFiles shouldBe Some(
      Vector.empty
    )
    val indexed = SnapshotSegmentFixture.encode(indexes =
      Vector(SnapshotSegmentFixture.index())
    )
    SnapshotSegmentReader
      .parse(indexed.take(indexed.length / 2), 4)
      .isLeft shouldBe true
  }

  private val avroBytes: Array[Byte] =
    Files.readAllBytes(Paths.get("core/src/test/data/seg_manifest.avro"))

  test("parse decodes one ManifestEntry with expected top-level fields") {
    val result = SnapshotSegmentReader.parse(avroBytes)
    result shouldBe a[Right[_, _]]

    val entry = result.toOption.get
    entry.segmentId shouldBe 465602255560578628L
    entry.partitionId shouldBe 465602255560377588L
    entry.numOfRows shouldBe 20480L
    entry.storageVersion shouldBe 2L // StorageV2
  }

  test("parse exposes binlog entries as (slotFieldId, binlogs)") {
    val entry = SnapshotSegmentReader.parse(avroBytes).toOption.get
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

  /** Milvus v3.0.2 writes format version 5: one boolean more, `is_sorted` and
    * `commit_timestamp` then `manifest_has_index`, before the binlog arrays.
    * Avro is positional, so a version read with the wrong schema is not a
    * slightly wrong record, it is a parse error — which is how a real UAT
    * snapshot from that version showed this version was missing.
    */
  test("a version 5 record decodes, and says where its index is registered") {
    val withFiles = SnapshotSegmentReader
      .parse(
        SnapshotSegmentFixture.encode(
          version = 5,
          segmentId = 77L,
          rows = 4L,
          indexes = Vector(SnapshotSegmentFixture.index(77L, 4L))
        ),
        manifestSchemaVersion = 5
      )
      .toOption
      .get
    withFiles.segmentId shouldBe 77L
    withFiles.numOfRows shouldBe 4L
    withFiles.indexFiles.map(_.size) shouldBe Some(1)
    withFiles.manifestHasIndex shouldBe Some(false)

    val inManifest = SnapshotSegmentReader
      .parse(
        SnapshotSegmentFixture.encode(
          version = 5,
          segmentId = 78L,
          rows = 4L,
          manifestHasIndex = true
        ),
        manifestSchemaVersion = 5
      )
      .toOption
      .get
    inManifest.manifestHasIndex shouldBe Some(true)
    inManifest.indexFiles shouldBe Some(Vector.empty)

    // A version 4 record says nothing about it, which is not an explicit false.
    SnapshotSegmentReader
      .parse(
        SnapshotSegmentFixture.encode(version = 4, segmentId = 79L),
        manifestSchemaVersion = 4
      )
      .toOption
      .get
      .manifestHasIndex shouldBe None

    // Read with the wrong schema, everything after the new field lands in the
    // wrong place: the boolean is taken for the first array's block count.
    val mismatched = SnapshotSegmentReader.parse(
      SnapshotSegmentFixture.encode(
        version = 5,
        segmentId = 80L,
        rows = 4L,
        indexes = Vector(SnapshotSegmentFixture.index(80L, 4L))
      ),
      manifestSchemaVersion = 4
    )
    mismatched.toOption.flatMap(_.indexFiles).map(_.size) should not be Some(1)
  }

}
