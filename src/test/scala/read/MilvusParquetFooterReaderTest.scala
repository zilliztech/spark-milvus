package com.zilliz.spark.connector.read

import java.net.URI
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{
  FSDataInputStream,
  FSDataOutputStream,
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

/** Tests for [[MilvusParquetFooterReader]]. S3-backed reads need minio +
  * hadoop-aws at runtime, but the field-id recovery path can be exercised
  * against a local parquet written by parquet-mr's example writer — that writer
  * honours per-column `id(...)` settings and writes them into Parquet's native
  * `SchemaElement.field_id`, the same place milvus-storage writes them (via
  * arrow-cpp).
  */
class MilvusParquetFooterReaderTest extends AnyFunSuite with Matchers {

  test("parseGroupFieldIdList handles the multi-field sample") {
    val parsed =
      MilvusParquetFooterReader.parseGroupFieldIdList("100,0,1;101;102")
    parsed shouldBe Seq(Seq(100L, 0L, 1L), Seq(101L), Seq(102L))
  }

  test("parseGroupFieldIdList tolerates trailing semicolons") {
    MilvusParquetFooterReader.parseGroupFieldIdList(
      "103;"
    ) shouldBe Seq(Seq(103L))
  }

  test("parseGroupFieldIdList on null/empty returns empty") {
    MilvusParquetFooterReader.parseGroupFieldIdList(null) shouldBe Seq.empty
    MilvusParquetFooterReader.parseGroupFieldIdList("") shouldBe Seq.empty
  }

  test("parseGroupFieldIdList on single group of one field") {
    MilvusParquetFooterReader.parseGroupFieldIdList("103") shouldBe Seq(
      Seq(103L)
    )
  }

  test("parseGroupFieldIdList silently drops empty chunks") {
    // Milvus never emits `;;`, but being lenient avoids spurious failures on
    // trailing or duplicated separators.
    MilvusParquetFooterReader.parseGroupFieldIdList(
      "100,0,1;;102"
    ) shouldBe Seq(
      Seq(100L, 0L, 1L),
      Seq(102L)
    )
  }

  test("readFieldIdsFromSchema returns per-column field ids in schema order") {
    val tmp = Files.createTempFile("milvus-fid-test-", ".parquet")
    Files.delete(tmp)
    val schema: MessageType = Types
      .buildMessage()
      .required(PrimitiveTypeName.INT64)
      .id(100)
      .named("pk")
      .required(PrimitiveTypeName.INT64)
      .id(0)
      .named("row_id")
      .required(PrimitiveTypeName.INT64)
      .id(1)
      .named("ts")
      .named("milvus_group")

    val conf = new Configuration()
    GroupWriteSupport.setSchema(schema, conf)
    val writer = ExampleParquetWriter
      .builder(new HPath(tmp.toUri))
      .withType(schema)
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .build()
    try {
      val factory = new SimpleGroupFactory(schema)
      writer.write(
        factory
          .newGroup()
          .append("pk", 1L)
          .append("row_id", 10L)
          .append("ts", 100L)
      )
    } finally {
      writer.close()
    }

    try {
      val result = MilvusParquetFooterReader.readFieldIdsFromSchema(
        tmp.toUri.toString,
        new Configuration()
      )
      result shouldBe Right(Seq(100L, 0L, 1L))
    } finally {
      Files.deleteIfExists(tmp)
    }
  }

  test("readFieldIdsFromSchema on single-field (backfill-style) parquet") {
    val tmp = Files.createTempFile("milvus-fid-test-bf-", ".parquet")
    Files.delete(tmp)
    val schema: MessageType = Types
      .buildMessage()
      .required(PrimitiveTypeName.INT64)
      .id(105)
      .named("new_field")
      .named("milvus_group")

    val conf = new Configuration()
    GroupWriteSupport.setSchema(schema, conf)
    val writer = ExampleParquetWriter
      .builder(new HPath(tmp.toUri))
      .withType(schema)
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .build()
    try {
      val factory = new SimpleGroupFactory(schema)
      writer.write(factory.newGroup().append("new_field", 42L))
    } finally {
      writer.close()
    }

    try {
      val result = MilvusParquetFooterReader.readFieldIdsFromSchema(
        tmp.toUri.toString,
        new Configuration()
      )
      result shouldBe Right(Seq(105L))
    } finally {
      Files.deleteIfExists(tmp)
    }
  }

  test("readFieldIdsFromSchema returns Left for malformed URI") {
    val result = MilvusParquetFooterReader.readFieldIdsFromSchema(
      "s3a://bucket/path with spaces/[bad].parquet",
      new Configuration()
    )

    result shouldBe a[Left[_, _]]
  }

  test("readFieldIdsFromSchema does not swallow fatal errors") {
    val conf = new Configuration()
    conf.set("fs.fatal-footer.impl", classOf[FatalFooterFileSystem].getName)

    intercept[OutOfMemoryError] {
      MilvusParquetFooterReader.readFieldIdsFromSchema(
        "fatal-footer://bucket/file.parquet",
        conf
      )
    }
  }

  test("readFieldIdsFromSchema returns Left when a column has no field id") {
    val tmp = Files.createTempFile("milvus-fid-test-bad-", ".parquet")
    Files.delete(tmp)
    // No `.id(...)` on the second column — mimics a malformed parquet where
    // PARQUET:field_id never made it into the SchemaElement.
    val schema: MessageType = Types
      .buildMessage()
      .required(PrimitiveTypeName.INT64)
      .id(200)
      .named("good")
      .required(PrimitiveTypeName.INT64)
      .named("no_id")
      .named("milvus_group")

    val conf = new Configuration()
    GroupWriteSupport.setSchema(schema, conf)
    val writer = ExampleParquetWriter
      .builder(new HPath(tmp.toUri))
      .withType(schema)
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .build()
    try {
      val factory = new SimpleGroupFactory(schema)
      writer.write(factory.newGroup().append("good", 1L).append("no_id", 2L))
    } finally {
      writer.close()
    }

    try {
      val result = MilvusParquetFooterReader.readFieldIdsFromSchema(
        tmp.toUri.toString,
        new Configuration()
      )
      result shouldBe a[Left[_, _]]
      result.left.toOption.get.getMessage should include("no_id")
      result.left.toOption.get.getMessage should include("PARQUET:field_id")
    } finally {
      Files.deleteIfExists(tmp)
    }
  }

  test("readFieldIdsAndRowCount sums row groups across the footer") {
    val tmp = Files.createTempFile("milvus-rowcount-test-", ".parquet")
    Files.delete(tmp)
    val schema: MessageType = Types
      .buildMessage()
      .required(PrimitiveTypeName.INT64)
      .id(100)
      .named("pk")
      .required(PrimitiveTypeName.INT64)
      .id(0)
      .named("row_id")
      .named("milvus_group")

    val conf = new Configuration()
    GroupWriteSupport.setSchema(schema, conf)
    // parquet-mr only evaluates the row-group size once
    // recordCount >= DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK (100), so with 3
    // rows a tiny rowGroupSize would never flush and the file would hold one
    // block. Write enough rows for the check to run and flush multiple groups,
    // so the production footerInfo must genuinely sum across them.
    val totalRows = 150
    val writer = ExampleParquetWriter
      .builder(new HPath(tmp.toUri))
      .withType(schema)
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .withRowGroupSize(1)
      .build()
    try {
      val factory = new SimpleGroupFactory(schema)
      (1 to totalRows).foreach { i =>
        writer.write(
          factory.newGroup().append("pk", i.toLong).append("row_id", i.toLong)
        )
      }
    } finally {
      writer.close()
    }

    try {
      // The production path (used by the backup planner) returns field IDs and
      // the summed row count in a single open.
      val info = MilvusParquetFooterReader
        .readFieldIdsAndRowCount(tmp.toUri.toString, new Configuration())
        .getOrElse(fail("expected Right"))
      info.fieldIds shouldBe Seq(100L, 0L)
      info.rowCount shouldBe totalRows.toLong

      // Prove the file actually has multiple row groups (a first-block-only
      // sum would not reach totalRows).
      val parquet = org.apache.parquet.hadoop.ParquetFileReader.open(
        org.apache.parquet.hadoop.util.HadoopInputFile
          .fromPath(new HPath(tmp.toUri), new Configuration())
      )
      try {
        parquet.getFooter.getBlocks.size() should be >= 2
      } finally {
        parquet.close()
      }
    } finally {
      Files.deleteIfExists(tmp)
    }
  }
}

class FatalFooterFileSystem extends FileSystem {
  private var uri: URI = _

  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    uri = name
  }

  override def getUri: URI = uri

  override def open(path: HPath, bufferSize: Int): FSDataInputStream =
    throw new OutOfMemoryError("fatal")

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
    new FileStatus(1L, false, 1, 1L, 0L, path)
}
