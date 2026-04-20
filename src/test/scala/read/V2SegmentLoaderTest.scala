package com.zilliz.spark.connector.read

import java.nio.file.{Files, Path => NPath}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.{
  ExampleParquetWriter,
  GroupWriteSupport
}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.{MessageType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Unit tests for [[V2SegmentLoader.buildV2SegmentInfoFromEntry]].
  *
  * Integration with S3/minio is covered by the backfill E2E test; here we
  * exercise the per-entry schema recovery, partial-empty fail-fast, and
  * enriched error-context branches against local parquet files written by
  * parquet-mr's example writer. That writer honours per-column `id(...)`
  * settings and stamps them into Parquet's native `SchemaElement.field_id` —
  * the same place milvus-storage's arrow-cpp writer puts them.
  */
class V2SegmentLoaderTest extends AnyFunSuite with Matchers {

  /** Write a single-column parquet at `path` carrying the given field id, so
    * `readFieldIdsFromSchema` will return `Seq(fieldId)`.
    */
  private def writeSingleFieldParquet(
      path: NPath,
      columnName: String,
      fieldId: Int
  ): Unit = {
    val schema: MessageType = Types
      .buildMessage()
      .required(PrimitiveTypeName.INT64)
      .id(fieldId)
      .named(columnName)
      .named("milvus_group")

    val conf = new Configuration()
    GroupWriteSupport.setSchema(schema, conf)
    val writer = ExampleParquetWriter
      .builder(new HPath(path.toUri))
      .withType(schema)
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .build()
    try {
      val factory = new SimpleGroupFactory(schema)
      writer.write(factory.newGroup().append(columnName, 1L))
    } finally {
      writer.close()
    }
  }

  /** Write a parquet whose only column has NO `PARQUET:field_id` — simulates a
    * malformed / non-milvus parquet.
    */
  private def writeFieldIdLessParquet(path: NPath): Unit = {
    val schema: MessageType = Types
      .buildMessage()
      .required(PrimitiveTypeName.INT64)
      .named("no_id")
      .named("milvus_group")

    val conf = new Configuration()
    GroupWriteSupport.setSchema(schema, conf)
    val writer = ExampleParquetWriter
      .builder(new HPath(path.toUri))
      .withType(schema)
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .build()
    try {
      val factory = new SimpleGroupFactory(schema)
      writer.write(factory.newGroup().append("no_id", 1L))
    } finally {
      writer.close()
    }
  }

  private def mkTempParquet(prefix: String): NPath = {
    val p = Files.createTempFile(prefix, ".parquet")
    Files.delete(p) // ExampleParquetWriter refuses to overwrite existing files
    p
  }

  private def entry(
      segmentId: Long,
      binlogs: Seq[AvroFieldBinlogEntry],
      storageVersion: Long = 2L
  ): AvroManifestEntry =
    AvroManifestEntry(
      segmentId = segmentId,
      partitionId = 10L,
      numOfRows = 100L,
      storageVersion = storageVersion,
      binlogFiles = binlogs
    )

  test(
    "per-entry recovery returns each parquet's OWN field ids (backfill-safe)"
  ) {
    // Simulates a backfilled segment: two column groups, each written in a
    // different session. Previously the loader sampled ONE footer and reused
    // its `group_field_id_list` for all groups, which mis-attributed the
    // second group's field ids. With per-entry recovery each group now gets
    // its own parquet's field ids.
    val pq0 = mkTempParquet("v2loader-g0-")
    val pq1 = mkTempParquet("v2loader-g1-")
    try {
      writeSingleFieldParquet(pq0, "pk", fieldId = 100)
      writeSingleFieldParquet(pq1, "new_col", fieldId = 105)

      val manifest = entry(
        segmentId = 1001L,
        binlogs = Seq(
          AvroFieldBinlogEntry(
            slotFieldId = 100L,
            binlogs = Seq(AvroBinlogEntry(0L, pq0.toUri.toString, 10L))
          ),
          AvroFieldBinlogEntry(
            slotFieldId = 105L,
            binlogs = Seq(AvroBinlogEntry(0L, pq1.toUri.toString, 10L))
          )
        )
      )

      val result = V2SegmentLoader.buildV2SegmentInfoFromEntry(
        manifest,
        bucket = "",
        new Configuration()
      )

      result shouldBe a[Right[_, _]]
      val Some(seg) = result.toOption.get
      seg.segmentId shouldBe 1001L
      seg.columnGroups.map(_.fieldIds) shouldBe Seq(Seq(100L), Seq(105L))
      seg.columnGroups.map(_.filePaths) shouldBe Seq(
        Seq(pq0.toUri.toString),
        Seq(pq1.toUri.toString)
      )
      seg.columnGroups.map(_.fileRowCounts) shouldBe Seq(Seq(10L), Seq(10L))
    } finally {
      Files.deleteIfExists(pq0)
      Files.deleteIfExists(pq1)
    }
  }

  test(
    "partial-empty binlog entry fails loudly with segmentId + slotFieldId context"
  ) {
    val pq0 = mkTempParquet("v2loader-partial-")
    try {
      writeSingleFieldParquet(pq0, "pk", fieldId = 100)

      val manifest = entry(
        segmentId = 2002L,
        binlogs = Seq(
          AvroFieldBinlogEntry(
            slotFieldId = 100L,
            binlogs = Seq(AvroBinlogEntry(0L, pq0.toUri.toString, 10L))
          ),
          // One entry populated, one empty — the corrupt-manifest case the
          // top-level all-empty guard does NOT cover.
          AvroFieldBinlogEntry(slotFieldId = 77L, binlogs = Seq.empty)
        )
      )

      val result = V2SegmentLoader.buildV2SegmentInfoFromEntry(
        manifest,
        bucket = "",
        new Configuration()
      )

      result shouldBe a[Left[_, _]]
      val err = result.swap.toOption.get
      err shouldBe an[IllegalStateException]
      err.getMessage should include("2002") // segmentId
      err.getMessage should include("77") // slotFieldId of the empty entry
      err.getMessage should include("empty")
    } finally {
      Files.deleteIfExists(pq0)
    }
  }

  test(
    "malformed parquet (no PARQUET:field_id) surfaces as Left with enriched context"
  ) {
    val pq0 = mkTempParquet("v2loader-malformed-")
    try {
      writeFieldIdLessParquet(pq0)

      val manifest = entry(
        segmentId = 3003L,
        binlogs = Seq(
          AvroFieldBinlogEntry(
            slotFieldId = 42L,
            binlogs = Seq(AvroBinlogEntry(0L, pq0.toUri.toString, 10L))
          )
        )
      )

      val result = V2SegmentLoader.buildV2SegmentInfoFromEntry(
        manifest,
        bucket = "",
        new Configuration()
      )

      result shouldBe a[Left[_, _]]
      val msg = result.swap.toOption.get.getMessage
      msg should include("3003") // segmentId
      msg should include("42") // slotFieldId
      msg should include(pq0.toUri.toString) // path
      msg should include("PARQUET:field_id")
    } finally {
      Files.deleteIfExists(pq0)
    }
  }

  test(
    "all-empty binlog_files emits a segment with no column groups (not an error)"
  ) {
    val manifest = entry(
      segmentId = 4004L,
      binlogs = Seq(
        AvroFieldBinlogEntry(slotFieldId = 100L, binlogs = Seq.empty),
        AvroFieldBinlogEntry(slotFieldId = 101L, binlogs = Seq.empty)
      )
    )

    val result = V2SegmentLoader.buildV2SegmentInfoFromEntry(
      manifest,
      bucket = "",
      new Configuration()
    )

    val Some(seg) = result.toOption.get
    seg.segmentId shouldBe 4004L
    seg.columnGroups shouldBe empty
  }

  test("non-StorageV2 entry is skipped (returns Right(None))") {
    val manifest = entry(
      segmentId = 5005L,
      binlogs = Seq.empty,
      storageVersion = 3L // V3
    )

    val result = V2SegmentLoader.buildV2SegmentInfoFromEntry(
      manifest,
      bucket = "",
      new Configuration()
    )

    result shouldBe Right(None)
  }
}
