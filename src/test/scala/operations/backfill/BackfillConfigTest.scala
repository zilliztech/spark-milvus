package com.zilliz.spark.connector.operations.backfill

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Unit tests for BackfillConfig validation and options generation
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
      s3SecretKey = "minioadmin"
    )

    config.validate() shouldBe Right(())
  }

  test(
    "Empty milvusUri/collectionName is allowed by validate() in snapshot mode"
  ) {
    // milvusUri and collectionName are only required in client mode (no snapshot).
    // validate() must accept empty values; only validateForClientMode rejects them.
    val config = BackfillConfig(
      milvusUri = "",
      collectionName = "",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin"
    )

    config.validate() shouldBe Right(())
  }

  test("validate accepts empty AK/SK when s3UseIam=true") {
    val config = BackfillConfig(
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    config.validate() shouldBe Right(())
  }

  test("validateForClientMode fails on empty milvusUri") {
    val config = BackfillConfig(
      milvusUri = "",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin"
    )

    config.validateForClientMode().isLeft shouldBe true
  }

  test("validateForClientMode fails on empty collectionName") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin"
    )

    config.validateForClientMode().isLeft shouldBe true
  }

  test("Empty s3Endpoint fails validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin"
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
      s3SecretKey = "minioadmin"
    )

    config.validate() shouldBe Left("s3BucketName cannot be empty")
  }

  test("Unsupported s3CloudProvider fails validation") {
    val config = BackfillConfig(
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
      s3CloudProvider = "oss"
    )

    config.validate() match {
      case Left(error) => error should include("s3CloudProvider must be one of")
      case Right(_)    => fail("expected invalid s3CloudProvider to fail")
    }
  }

  test("Empty s3AccessKey/s3SecretKey is allowed (IAM/IRSA mode)") {
    // Under IAM/IRSA the SDK falls back to the default AWS credentials chain,
    // so empty static credentials must NOT fail validation.
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )

    config.validate() shouldBe Right(())
  }

  test("Empty s3AccessKey/s3SecretKey without useIam is rejected") {
    // Hard invariant: must use IAM or supply both AK and SK. Half-set or
    // fully-empty static credentials without useIam are never valid.
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "",
      s3SecretKey = ""
    )

    config.validate().isLeft shouldBe true
  }

  test("Half-set s3AccessKey without s3SecretKey is rejected") {
    val config = BackfillConfig(
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "ak",
      s3SecretKey = ""
    )
    config.validate().isLeft shouldBe true
  }

  test(
    "Source bucket half-set credentials without sourceS3UseIam are rejected"
  ) {
    val config = BackfillConfig(
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "main-ak",
      s3SecretKey = "main-sk",
      sourceS3AccessKey = Some("src-ak"),
      sourceS3SecretKey = Some("")
    )
    config.validate().isLeft shouldBe true
  }

  test("Source bucket override with sourceS3UseIam=true is accepted") {
    val config = BackfillConfig(
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "main-ak",
      s3SecretKey = "main-sk",
      sourceS3AccessKey = Some(""),
      sourceS3SecretKey = Some(""),
      sourceS3UseIam = Some(true)
    )
    config.validate() shouldBe Right(())
  }

  test("AssumeRole settings require IAM mode and a role ARN") {
    BackfillConfig(
      s3Endpoint = "s3.amazonaws.com",
      s3BucketName = "bucket",
      s3AccessKey = "ak",
      s3SecretKey = "sk",
      s3RoleArn = Some("arn:aws:iam::123456789012:role/data-role")
    ).validate() shouldBe Left("s3RoleArn requires s3UseIam=true")

    BackfillConfig(
      s3Endpoint = "s3.amazonaws.com",
      s3BucketName = "bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true,
      s3RoleSessionName = Some("spark-job")
    ).validate() shouldBe Left(
      "s3RoleSessionName and s3ExternalId require s3RoleArn"
    )
  }

  test("withHadoopStorageAssumeRole derives the AWS native main-storage role") {
    val hadoopConf = new Configuration(false)
    hadoopConf.set(
      BackfillConfig.HadoopS3CredentialsProvider,
      BackfillConfig.HadoopS3AssumedRoleProvider
    )
    hadoopConf.set(
      BackfillConfig.HadoopS3AssumedRoleArn,
      "arn:aws:iam::123456789012:role/data-role"
    )
    hadoopConf.set(
      BackfillConfig.HadoopS3AssumedRoleExternalId,
      "external-id"
    )
    val config = BackfillConfig(
      s3Endpoint = "s3.amazonaws.com",
      s3BucketName = "bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )

    val resolved = config.withHadoopStorageAssumeRole(
      hadoopConf,
      "spark app/with invalid characters and a very long identifier 1234567890"
    )

    resolved.s3RoleArn shouldBe Some(
      "arn:aws:iam::123456789012:role/data-role"
    )
    resolved.s3RoleSessionName.get should fullyMatch regex
      "[A-Za-z0-9+=,.@-]{1,64}"
    resolved.s3ExternalId shouldBe Some("external-id")
    resolved.validate() shouldBe Right(())
  }

  test(
    "withHadoopStorageAssumeRole derives the Alibaba native main-storage role"
  ) {
    val hadoopConf = new Configuration(false)
    hadoopConf.set(
      BackfillConfig.HadoopOssAssumedRoleArn,
      "acs:ram::123456789012:role/spark-data-role"
    )
    hadoopConf.set(
      BackfillConfig.HadoopOssAssumedRoleSessionName,
      "spark-job"
    )
    hadoopConf.set(
      BackfillConfig.HadoopOssAssumedRoleExternalId,
      "external-id"
    )
    val config = BackfillConfig(
      s3Endpoint = "oss-cn-hangzhou-internal.aliyuncs.com",
      s3BucketName = "bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "aliyun",
      s3UseIam = true
    )

    val resolved = config.withHadoopStorageAssumeRole(
      hadoopConf,
      "unused-default-session"
    )

    resolved.s3RoleArn shouldBe Some(
      "acs:ram::123456789012:role/spark-data-role"
    )
    resolved.s3RoleSessionName shouldBe Some("spark-job")
    resolved.s3ExternalId shouldBe Some("external-id")
    resolved.validate() shouldBe Right(())
  }

  test("withHadoopStorageAssumeRole ignores missing Alibaba role config") {
    val config = BackfillConfig(
      s3Endpoint = "oss-cn-hangzhou-internal.aliyuncs.com",
      s3BucketName = "bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "aliyun",
      s3UseIam = true
    )
    val hadoopConf = new Configuration(false)

    config.withHadoopStorageAssumeRole(hadoopConf, "spark-job") shouldBe config
  }

  test(
    "withHadoopStorageAssumeRole rejects an incomplete Alibaba AssumeRole config"
  ) {
    val config = BackfillConfig(
      s3Endpoint = "oss-cn-hangzhou-internal.aliyuncs.com",
      s3BucketName = "bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "aliyun",
      s3UseIam = true
    )
    val hadoopConf = new Configuration(false)
    hadoopConf.set(
      BackfillConfig.HadoopOssCredentialsProvider,
      BackfillConfig.HadoopOssAssumedRoleProvider
    )

    val error = intercept[IllegalArgumentException] {
      config.withHadoopStorageAssumeRole(hadoopConf, "spark-job")
    }
    error.getMessage should include(BackfillConfig.HadoopOssAssumedRoleArn)
  }

  test("withHadoopStorageAssumeRole ignores non-AssumeRole AWS config") {
    val config = BackfillConfig(
      s3Endpoint = "s3.amazonaws.com",
      s3BucketName = "bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val hadoopConf = new Configuration(false)
    hadoopConf.set(
      BackfillConfig.HadoopS3AssumedRoleArn,
      "arn:aws:iam::123456789012:role/data-role"
    )

    config.withHadoopStorageAssumeRole(hadoopConf, "spark-job") shouldBe config
  }

  test(
    "withHadoopStorageAssumeRole rejects an incomplete AWS AssumeRole config"
  ) {
    val config = BackfillConfig(
      s3Endpoint = "s3.amazonaws.com",
      s3BucketName = "bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val hadoopConf = new Configuration(false)

    hadoopConf.set(
      BackfillConfig.HadoopS3CredentialsProvider,
      BackfillConfig.HadoopS3AssumedRoleProvider
    )

    val error = intercept[IllegalArgumentException] {
      config.withHadoopStorageAssumeRole(hadoopConf, "spark-job")
    }
    error.getMessage should include(BackfillConfig.HadoopS3AssumedRoleArn)
  }

  test("Zero batchSize fails validation") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin",
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
      s3SecretKey = "minioadmin"
    )

    config.milvusToken shouldBe ""
    config.databaseName shouldBe "default"
    config.partitionName shouldBe None
    config.s3UseSSL shouldBe false
    config.s3RootPath shouldBe "files"
    config.s3Region shouldBe "us-east-1"
    config.s3CloudProvider shouldBe "aws"
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
      s3UseSSL = true
    )

    val options = config.getMilvusReadOptions

    options("milvus.uri") shouldBe "http://localhost:19530"
    options("milvus.token") shouldBe "root:Milvus"
    options("milvus.database.name") shouldBe "my_database"
    options("milvus.collection.name") shouldBe "test_collection"
    options("milvus.extra.columns") shouldBe "$segment_id,$row_offset"
    options("fs.address") shouldBe "localhost:9000"
    options("fs.bucket_name") shouldBe "test-bucket"
    options("fs.root_path") shouldBe "data/milvus"
    options("fs.access_key_id") shouldBe "access123"
    options("fs.access_key_value") shouldBe "secret456"
    options("fs.use_ssl") shouldBe "true"
    options("fs.cloud_provider") shouldBe "aws"
  }

  test("getMilvusReadOptions includes partitionName when set") {
    val config = BackfillConfig(
      milvusUri = "http://localhost:19530",
      collectionName = "test_collection",
      partitionName = Some("partition_1"),
      s3Endpoint = "localhost:9000",
      s3BucketName = "test-bucket",
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin"
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
      s3SecretKey = "minioadmin"
    )

    val options = config.getMilvusReadOptions

    options should not contain key("milvus.partition.name")
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
      s3CloudProvider = "aliyun",
      s3UseSSL = true,
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
    options("fs.cloud_provider") shouldBe "aliyun"
    options("milvus.collection.name") shouldBe "segment_789_backfill"
    options(
      "milvus.writer.customPath"
    ) shouldBe "files/insert_log/123/456/789"
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
      milvusUri = "http://custom:19530",
      milvusToken = "custom:token",
      s3Endpoint = "custom:9000",
      s3BucketName = "custom-bucket"
    )

    config.milvusUri shouldBe "http://custom:19530"
    config.milvusToken shouldBe "custom:token"
    config.collectionName shouldBe "custom_collection"
    config.s3Endpoint shouldBe "custom:9000"
    config.s3BucketName shouldBe "custom-bucket"
  }
}
