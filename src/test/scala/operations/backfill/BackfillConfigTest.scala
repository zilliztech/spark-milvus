package com.zilliz.spark.connector.operations.backfill

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for BackfillConfig validation and options generation
 */
class BackfillConfigTest extends AnyFunSuite with Matchers {

  // ============ Validation Tests ============

  test("Valid config passes validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      milvusToken = "root:Milvus",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100
    )

    config.validate() shouldBe Right(())
  }

  test("Empty milvusUri fails validation") {
    val config = BackfillConfig(
      milvusUri = "",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100
    )

    config.validate() shouldBe Left("milvusUri cannot be empty")
  }

  test("Empty collectionName fails validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100
    )

    config.validate() shouldBe Left("collectionName cannot be empty")
  }

  test("Empty s3Endpoint fails validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100
    )

    config.validate() shouldBe Left("s3Endpoint cannot be empty")
  }

  test("Empty s3BucketName fails validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100
    )

    config.validate() shouldBe Left("s3BucketName cannot be empty")
  }

  test("Empty s3AccessKey fails validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100
    )

    config.validate() shouldBe Left("s3AccessKey cannot be empty")
  }

  test("Empty s3SecretKey fails validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "",
      pkFieldToRead = 100
    )

    config.validate() shouldBe Left("s3SecretKey cannot be empty")
  }

  test("Zero batchSize fails validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100,
      batchSize = 0
    )

    config.validate() shouldBe Left("batchSize must be positive")
  }

  test("Negative batchSize fails validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100,
      batchSize = -1
    )

    config.validate() shouldBe Left("batchSize must be positive")
  }

  // ============ Default Values Tests ============

  test("Default values are set correctly") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100
    )

    config.milvusToken shouldBe ""
    config.databaseName shouldBe "default"
    config.partitionName shouldBe None
    config.s3UseSSL shouldBe false
    config.s3RootPath shouldBe "files"
    config.s3Region shouldBe "us-east-1"
    config.batchSize shouldBe 1024
    config.customOutputPath shouldBe None
  }

  // ============ getMilvusReadOptions Tests ============

  test("getMilvusReadOptions returns correct basic options") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      milvusToken = "root:Milvus",
      databaseName = "my_database",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "access123",
      s3SecretKey = "secret456",
      s3RootPath = "data/milvus",
      s3UseSSL = true,
      pkFieldToRead = 100
    )

    val options = config.getMilvusReadOptions

    options("milvus.uri") shouldBe "http://localhost:19530"
    options("milvus.token") shouldBe "root:Milvus"
    options("milvus.database.name") shouldBe "my_database"
    options("milvus.collection.name") shouldBe "test_collection"
    options("milvus.extra.columns") shouldBe "segment_id,row_offset"
    options("milvus.field.ids") shouldBe "100"
    options("fs.address") shouldBe "localhost:9000"
    options("fs.bucket_name") shouldBe "test-bucket"
    options("fs.root_path") shouldBe "data/milvus"
    options("fs.access_key_id") shouldBe "access123"
    options("fs.access_key_value") shouldBe "secret456"
    options("fs.use_ssl") shouldBe "true"
  }

  test("getMilvusReadOptions includes partitionName when set") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      partitionName = Some("partition_1"),
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100
    )

    val options = config.getMilvusReadOptions

    options should contain key "milvus.partition.name"
    options("milvus.partition.name") shouldBe "partition_1"
  }

  test("getMilvusReadOptions does not include partitionName when not set") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      partitionName = None,
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100
    )

    val options = config.getMilvusReadOptions

    options should not contain key ("milvus.partition.name")
  }

  // ============ getS3WriteOptions Tests ============

  test("getS3WriteOptions returns correct options with default output path") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "access123",
      s3SecretKey = "secret456",
      s3RootPath = "files",
      s3Region = "us-west-2",
      s3UseSSL = true,
      pkFieldToRead = 100,
      batchSize = 2048
    )

    val options = config.getS3WriteOptions(
      collectionId = 123L,
      partitionId = 456L,
      segmentId = 789L
    )

    options("fs.storage_type") shouldBe "remote"
    options("fs.address") shouldBe "localhost:9000"
    options("fs.bucket_name") shouldBe "test-bucket"
    options("fs.root_path") shouldBe "files"
    options("fs.access_key_id") shouldBe "access123"
    options("fs.access_key_value") shouldBe "secret456"
    options("fs.use_ssl") shouldBe "true"
    options("fs.region") shouldBe "us-west-2"
    options("milvus.collection.name") shouldBe "segment_789_backfill"
    options("milvus.writer.customPath") shouldBe "test-bucket/files/insert_log/123/456/789/new_field"
    options("milvus.insertMaxBatchSize") shouldBe "2048"
  }

  test("getS3WriteOptions uses customOutputPath when set") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      pkFieldToRead = 100,
      customOutputPath = Some("custom/path/to/output")
    )

    val options = config.getS3WriteOptions(
      collectionId = 123L,
      partitionId = 456L,
      segmentId = 789L
    )

    options("milvus.writer.customPath") shouldBe "custom/path/to/output"
  }

  // ============ forTest Factory Method Tests ============

  test("forTest creates config with default test values") {
    val config = BackfillConfig.forTest(
      collectionName = "test_collection"
    )

    config.milvusUri shouldBe "http://localhost:19530"
    config.milvusToken shouldBe "root:Milvus"
    config.collectionName shouldBe "test_collection"
    config.pkFieldToRead shouldBe 100
    config.s3Endpoint shouldBe "localhost:9000"
    config.s3BucketName shouldBe "a-bucket"
    config.s3AccessKey shouldBe "minioadmin"
    config.s3SecretKey shouldBe "minioadmin"

    // Should pass validation
    config.validate() shouldBe Right(())
  }

  test("forTest allows overriding default values") {
    val config = BackfillConfig.forTest(
      collectionName = "custom_collection",
      pkFieldToRead = 200,
      milvusUri = "http://custom:19530",
      milvusToken = "custom:token",
      s3Endpoint = "custom:9000",
      s3BucketName = "custom-bucket"
    )

    config.milvusUri shouldBe "http://custom:19530"
    config.milvusToken shouldBe "custom:token"
    config.collectionName shouldBe "custom_collection"
    config.pkFieldToRead shouldBe 200
    config.s3Endpoint shouldBe "custom:9000"
    config.s3BucketName shouldBe "custom-bucket"
  }
}
