package com.zilliz.spark.connector.read

import java.nio.file.Files

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
}
