package com.zilliz.spark.connector.read

import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => NPath}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{
  FSDataInputStream,
  FSDataOutputStream,
  FSInputStream,
  FileStatus,
  FileSystem,
  Path => HPath
}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
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
import org.scalatest.BeforeAndAfterEach

/** Unit tests for [[V2SegmentLoader.buildV2SegmentInfoFromEntry]].
  *
  * Integration with S3/minio is covered by the backfill E2E test; here we
  * exercise the per-entry schema recovery, partial-empty fail-fast, and
  * enriched error-context branches against local parquet files written by
  * parquet-mr's example writer. That writer honours per-column `id(...)`
  * settings and stamps them into Parquet's native `SchemaElement.field_id` —
  * the same place milvus-storage's arrow-cpp writer puts them.
  */
class V2SegmentLoaderTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterEach {

  test("resolvePath uses OSS for Alibaba manifest and nested binlog paths") {
    V2SegmentLoader.resolvePath(
      "files/manifest.avro",
      "managed-bucket",
      "oss"
    ) shouldBe "oss://managed-bucket/files/manifest.avro"
    V2SegmentLoader.resolvePath(
      "s3://managed-bucket/files/manifest.avro",
      "managed-bucket",
      "oss"
    ) shouldBe "oss://managed-bucket/files/manifest.avro"
    V2SegmentLoader.resolvePath(
      "s3a://managed-bucket/files/snapshots/manifest.avro",
      "managed-bucket",
      "oss"
    ) shouldBe "oss://managed-bucket/files/snapshots/manifest.avro"
    V2SegmentLoader.resolvePath(
      "s3a://managed-bucket/files/insert_log/1/2/3/4.parquet",
      "managed-bucket",
      "oss"
    ) shouldBe "oss://managed-bucket/files/insert_log/1/2/3/4.parquet"
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    CloseTrackingV2FileSystem.reset()
  }

  private lazy val manifestSchema: Schema = {
    val in = getClass.getResourceAsStream("/milvus-segment-manifest-v1.avsc")
    val full =
      try new Schema.Parser().parse(in)
      finally in.close()
    val lastFieldIndex = full.getFields.asScala.indexWhere(
      _.name == "deltalog_files"
    )
    val schema = Schema.createRecord(
      full.getName,
      full.getDoc,
      full.getNamespace,
      full.isError
    )
    schema.setFields(
      full.getFields.asScala
        .take(lastFieldIndex + 1)
        .map { f =>
          new Schema.Field(f.name, f.schema, f.doc, f.defaultVal)
        }
        .asJava
    )
    schema
  }

  private def encodeManifest(
      binlogPath: String,
      deltaLogPath: String
  ): Array[Byte] = {
    val record = new GenericData.Record(manifestSchema)
    record.put("segment_id", 1001L)
    record.put("partition_id", 10L)
    record.put("segment_level", 2L)
    record.put("channel_name", "test-channel")
    record.put("num_of_rows", 1L)

    val positionSchema = manifestSchema.getField("start_position").schema
    def position(): GenericRecord = {
      val value = new GenericData.Record(positionSchema)
      value.put("channel_name", "test-channel")
      value.put("msg_id", ByteBuffer.wrap(Array.emptyByteArray))
      value.put("msg_group", "test-group")
      value.put("timestamp", 1L)
      value
    }
    record.put("start_position", position())
    record.put("dml_position", position())
    record.put("storage_version", 2L)
    record.put("is_sorted", false)

    val fieldBinlogSchema =
      manifestSchema.getField("binlog_files").schema.getElementType
    val binlogSchema =
      fieldBinlogSchema.getField("binlogs").schema.getElementType

    def binlog(path: String, logId: Long): GenericRecord = {
      val value = new GenericData.Record(binlogSchema)
      value.put("entries_num", 1L)
      value.put("timestamp_from", 1L)
      value.put("timestamp_to", 1L)
      value.put("log_path", path)
      value.put("log_size", 1L)
      value.put("log_id", logId)
      value.put("memory_size", 1L)
      value
    }

    def fieldBinlog(path: String, logId: Long): GenericRecord = {
      val value = new GenericData.Record(fieldBinlogSchema)
      value.put("field_id", 100L)
      value.put("binlogs", Seq(binlog(path, logId)).asJava)
      value
    }

    record.put("binlog_files", Seq(fieldBinlog(binlogPath, 1L)).asJava)
    record.put("deltalog_files", Seq(fieldBinlog(deltaLogPath, 2L)).asJava)

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
    new GenericDatumWriter[GenericRecord](manifestSchema).write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  private def fileUriAsS3a(path: NPath): String =
    "s3a://" + path.toUri.toString.stripPrefix("file://")

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

  test("loadV2Segments normalizes returned S3A binlog and delta paths") {
    val parquet = mkTempParquet("v2loader-resolved-path-")
    val manifest = Files.createTempFile("v2loader-manifest-", ".avro")
    val deltaLog = manifest.resolveSibling("delete-log.bin")
    try {
      writeSingleFieldParquet(parquet, "pk", fieldId = 100)
      Files.write(
        manifest,
        encodeManifest(fileUriAsS3a(parquet), fileUriAsS3a(deltaLog))
      )

      val result = V2SegmentLoader.loadV2Segments(
        Seq(fileUriAsS3a(manifest)),
        bucket = "",
        hadoopConf = new Configuration(),
        storageScheme = "file"
      )

      result shouldBe a[Right[_, _]]
      val segment = result.toOption.get.head
      segment.columnGroups.head.filePaths shouldBe Seq(parquet.toUri.toString)
      segment.deltaLogs.map(_.logPath) shouldBe Seq(deltaLog.toUri.toString)
    } finally {
      Files.deleteIfExists(manifest)
      Files.deleteIfExists(parquet)
    }
  }

  private def entry(
      segmentId: Long,
      binlogs: Seq[AvroFieldBinlogEntry],
      storageVersion: Long = 2L,
      segmentLevel: Long = 2L,
      partitionId: Long = 10L,
      deltaLogFiles: Seq[AvroFieldBinlogEntry] = Seq.empty
  ): AvroManifestEntry =
    AvroManifestEntry(
      segmentId = segmentId,
      partitionId = partitionId,
      segmentLevel = segmentLevel,
      numOfRows = 100L,
      storageVersion = storageVersion,
      binlogFiles = binlogs,
      deltaLogFiles = deltaLogFiles
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

  test(
    "StorageV2 entry with delete metadata keeps segment info when applyDeletes=true"
  ) {
    val pq0 = mkTempParquet("v2loader-delete-meta-")
    try {
      writeSingleFieldParquet(pq0, "pk", fieldId = 100)

      val manifest = entry(
        segmentId = 5005L,
        binlogs = Seq(
          AvroFieldBinlogEntry(
            slotFieldId = 100L,
            binlogs = Seq(AvroBinlogEntry(0L, pq0.toUri.toString, 10L))
          )
        ),
        deltaLogFiles = Seq(
          AvroFieldBinlogEntry(
            slotFieldId = 100L,
            binlogs = Seq(AvroBinlogEntry(9L, "s3a://bucket/delete-1", 1L))
          )
        )
      )

      val result = V2SegmentLoader.buildV2SegmentInfoFromEntry(
        manifest,
        bucket = "",
        new Configuration(),
        applyDeletes = true
      )

      result shouldBe a[Right[_, _]]
      val Some(seg) = result.toOption.get
      seg.segmentId shouldBe 5005L
      seg.columnGroups.map(_.fieldIds) shouldBe Seq(Seq(100L))
      // V2SegmentInfo keeps the original AVRO paths; the native reader boundary
      // strips + pins the bucket at partition-build time.
      seg.deltaLogs shouldBe Seq(
        V2DeltaLogFile(9L, "s3a://bucket/delete-1", 1L)
      )
    } finally {
      Files.deleteIfExists(pq0)
    }
  }

  test("StorageV2 L0 segment is skipped when applyDeletes=false") {
    val manifest = entry(
      segmentId = 5006L,
      binlogs = Seq(
        AvroFieldBinlogEntry(slotFieldId = 100L, binlogs = Seq.empty)
      ),
      segmentLevel = 1L
    )

    val result = V2SegmentLoader.buildV2SegmentInfoFromEntry(
      manifest,
      bucket = "",
      new Configuration(),
      applyDeletes = false
    )

    result shouldBe Right(None)
  }

  test("StorageV2 data segment keeps reading when applyDeletes=false") {
    val pq0 = mkTempParquet("v2loader-ignore-delete-")
    try {
      writeSingleFieldParquet(pq0, "pk", fieldId = 100)

      val manifest = entry(
        segmentId = 5007L,
        binlogs = Seq(
          AvroFieldBinlogEntry(
            slotFieldId = 100L,
            binlogs = Seq(AvroBinlogEntry(0L, pq0.toUri.toString, 10L))
          )
        ),
        deltaLogFiles = Seq(
          AvroFieldBinlogEntry(
            slotFieldId = 100L,
            binlogs = Seq(AvroBinlogEntry(9L, "s3a://bucket/delete-1", 1L))
          )
        )
      )

      val result = V2SegmentLoader.buildV2SegmentInfoFromEntry(
        manifest,
        bucket = "",
        new Configuration(),
        applyDeletes = false
      )

      result shouldBe a[Right[_, _]]
      val Some(seg) = result.toOption.get
      seg.segmentId shouldBe 5007L
      seg.columnGroups.map(_.fieldIds) shouldBe Seq(Seq(100L))
      // V2SegmentInfo keeps the original AVRO paths; the native reader boundary
      // strips + pins the bucket at partition-build time.
      seg.deltaLogs shouldBe Seq(
        V2DeltaLogFile(9L, "s3a://bucket/delete-1", 1L)
      )
    } finally {
      Files.deleteIfExists(pq0)
    }
  }

  test("non-StorageV2 entry is skipped (returns Right(None))") {
    val manifest = entry(
      segmentId = 5008L,
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

  test("readAllBytes wraps malformed URI with path context") {
    val err = intercept[RuntimeException] {
      V2SegmentLoader.readAllBytes(
        new Configuration(),
        "s3a://bucket/path with spaces/[bad].avro"
      )
    }

    err.getMessage should include("failed to read bytes")
    err.getMessage should include("s3a://bucket/path with spaces/[bad].avro")
  }

  test("resolvePath canonicalizes Milvus-format endpoint-prefixed paths") {
    V2SegmentLoader.resolvePath(
      "s3://eric-spark-minio:9000/milvus-bucket/file/insert_log/10/20/30/100/1",
      "ignored"
    ) shouldBe "s3a://milvus-bucket/file/insert_log/10/20/30/100/1"
  }

  test("resolvePath prefixes bucket for bucket-relative paths") {
    V2SegmentLoader.resolvePath("file/insert_log/10/20/30/100/1", "a-bucket")
      .shouldBe("s3a://a-bucket/file/insert_log/10/20/30/100/1")
  }

  test("resolvePath passes standard s3a paths through unchanged") {
    V2SegmentLoader.resolvePath(
      "s3a://a-bucket/file/insert_log/10/20/30/100/1",
      "ignored"
    ) shouldBe "s3a://a-bucket/file/insert_log/10/20/30/100/1"
  }

  test("resolvePath canonicalizes s3a Milvus-format endpoint-prefixed paths") {
    V2SegmentLoader.resolvePath(
      "s3a://eric-spark-minio:9000/milvus-bucket/file/insert_log/10/20/30/100/1",
      "ignored"
    ) shouldBe "s3a://milvus-bucket/file/insert_log/10/20/30/100/1"
  }

  test("readAllBytes does not swallow fatal errors") {
    val conf = new Configuration()
    conf.set("fs.fatal-v2.impl", classOf[FatalV2FileSystem].getName)

    intercept[OutOfMemoryError] {
      V2SegmentLoader.readAllBytes(conf, "fatal-v2://bucket/manifest.avro")
    }
  }

  test("readAllBytes closes the FileSystem instance when cache is disabled") {
    val conf = new Configuration()
    conf.set(
      "fs.close-tracking-v2.impl",
      classOf[CloseTrackingV2FileSystem].getName
    )
    conf.set("fs.close-tracking-v2.impl.disable.cache", "true")

    V2SegmentLoader.readAllBytes(
      conf,
      "close-tracking-v2://bucket/manifest.avro"
    ) shouldBe "avro".getBytes(StandardCharsets.UTF_8)
    CloseTrackingV2FileSystem.closeCount.get() shouldBe 1
  }
}

class CloseTrackingV2FileSystem extends FileSystem {
  private var uri: URI = _

  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    uri = name
  }

  override def getUri: URI = uri

  override def open(path: HPath, bufferSize: Int): FSDataInputStream = {
    val bytes = "avro".getBytes(StandardCharsets.UTF_8)
    val in = new FSInputStream {
      private var pos = 0

      override def read(): Int = {
        if (pos >= bytes.length) -1
        else {
          val value = bytes(pos) & 0xff
          pos += 1
          value
        }
      }

      override def seek(newPos: Long): Unit = {
        pos = newPos.toInt
      }

      override def getPos: Long = pos

      override def seekToNewSource(targetPos: Long): Boolean = false
    }
    new FSDataInputStream(in)
  }

  override def close(): Unit = {
    CloseTrackingV2FileSystem.closeCount.incrementAndGet()
    super.close()
  }

  override def create(
      path: HPath,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable
  ): FSDataOutputStream = throw new UnsupportedOperationException

  override def append(
      path: HPath,
      bufferSize: Int,
      progress: Progressable
  ): FSDataOutputStream = throw new UnsupportedOperationException

  override def rename(src: HPath, dst: HPath): Boolean =
    throw new UnsupportedOperationException

  override def delete(path: HPath, recursive: Boolean): Boolean =
    throw new UnsupportedOperationException

  override def listStatus(path: HPath): Array[FileStatus] =
    throw new UnsupportedOperationException

  override def setWorkingDirectory(path: HPath): Unit = ()

  override def getWorkingDirectory: HPath = new HPath("/")

  override def mkdirs(path: HPath, permission: FsPermission): Boolean = true

  override def getFileStatus(path: HPath): FileStatus =
    new FileStatus(4L, false, 1, 4L, 0L, path)
}

object CloseTrackingV2FileSystem {
  val closeCount = new AtomicInteger(0)

  def reset(): Unit = closeCount.set(0)
}

class FatalV2FileSystem extends CloseTrackingV2FileSystem {
  override def open(path: HPath, bufferSize: Int): FSDataInputStream =
    throw new OutOfMemoryError("fatal")
}
