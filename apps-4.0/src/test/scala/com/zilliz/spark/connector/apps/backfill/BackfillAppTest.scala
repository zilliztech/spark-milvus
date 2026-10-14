package com.zilliz.spark.connector.apps.backfill

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.snapshot.json.SnapshotJson

/** CLI parsing, configuration validation and backfill input orchestration. */
class BackfillAppTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("BackfillAppTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // ============ parseArgs ============

  test("parseArgs handles flag and key/value pairs") {
    val args = Array(
      "--parquet",
      "/tmp/data.parquet",
      "--snapshot",
      "/tmp/snap.json",
      "--s3-endpoint",
      "minio:9000",
      "--s3-bucket",
      "a-bucket",
      "--s3-cloud-provider",
      "aliyun",
      "--s3-access-key",
      "ak",
      "--s3-secret-key",
      "sk",
      "--s3-use-ssl",
      "--use-iam"
    )
    val parsed = BackfillApp.parseArgs(args)
    parsed("parquet") shouldBe "/tmp/data.parquet"
    parsed("snapshot") shouldBe "/tmp/snap.json"
    parsed("s3-endpoint") shouldBe "minio:9000"
    parsed("s3-bucket") shouldBe "a-bucket"
    parsed("s3-cloud-provider") shouldBe "aliyun"
    parsed("s3-access-key") shouldBe "ak"
    parsed("s3-secret-key") shouldBe "sk"
    parsed("s3-use-ssl") shouldBe "true"
    parsed("use-iam") shouldBe "true"
  }

  test("parseArgs handles source-* dual bucket flags") {
    val parsed = BackfillApp.parseArgs(
      Array(
        "--source-s3-endpoint",
        "src:9000",
        "--source-s3-access-key",
        "src-ak",
        "--source-s3-secret-key",
        "src-sk",
        "--source-s3-use-ssl",
        "--source-use-iam"
      )
    )
    parsed("source-s3-endpoint") shouldBe "src:9000"
    parsed("source-s3-access-key") shouldBe "src-ak"
    parsed("source-s3-secret-key") shouldBe "src-sk"
    parsed("source-s3-use-ssl") shouldBe "true"
    parsed("source-use-iam") shouldBe "true"
  }

  test("parseArgs accepts --input-path and --input-format") {
    val parsed = BackfillApp.parseArgs(
      Array(
        "--input-path",
        "s3://bucket/backfill.lance",
        "--input-format",
        "lance"
      )
    )
    parsed("input-path") shouldBe "s3://bucket/backfill.lance"
    parsed("input-format") shouldBe "lance"
  }

  test("resolveInputPath keeps --parquet as the legacy alias") {
    BackfillApp.resolveInputPath(
      Map("parquet" -> "/tmp/data.parquet")
    ) shouldBe "/tmp/data.parquet"
  }

  test("resolveInputPath accepts --input-path as the format-neutral alias") {
    BackfillApp.resolveInputPath(
      Map("input-path" -> "s3://bucket/backfill.lance")
    ) shouldBe "s3://bucket/backfill.lance"
  }

  test("resolveInputPath rejects both --parquet and --input-path") {
    val ex = intercept[IllegalArgumentException] {
      BackfillApp.resolveInputPath(
        Map("parquet" -> "/tmp/a.parquet", "input-path" -> "/tmp/b.lance")
      )
    }
    ex.getMessage should include("mutually exclusive")
  }

  test("resolveInputPath treats empty flag values as absent") {
    // Template wrappers keep optional keys with empty values; an empty
    // --parquet must not trip the mutual-exclusion check nor shadow a
    // populated --input-path.
    BackfillApp.resolveInputPath(
      Map("parquet" -> "", "input-path" -> "s3://bucket/backfill.lance")
    ) shouldBe "s3://bucket/backfill.lance"
    BackfillApp.resolveInputPath(
      Map("parquet" -> " ", "input-path" -> "/tmp/a.parquet")
    ) shouldBe "/tmp/a.parquet"
  }

  test("resolveInputPath treats a sole empty path as missing") {
    an[IllegalArgumentException] should be thrownBy {
      BackfillApp.resolveInputPath(Map("parquet" -> ""))
    }
    an[IllegalArgumentException] should be thrownBy {
      BackfillApp.resolveInputPath(Map("input-path" -> ""))
    }
  }

  test("resolveInputPath requires an input path") {
    an[IllegalArgumentException] should be thrownBy {
      BackfillApp.resolveInputPath(Map.empty)
    }
  }

  test("parseArgs throws on missing value for non-flag") {
    an[IllegalArgumentException] should be thrownBy {
      BackfillApp.parseArgs(Array("--parquet"))
    }
  }

  test("parseArgs accepts --join-key") {
    val parsed = BackfillApp.parseArgs(
      Array("--join-key", "external_row_id")
    )
    parsed("join-key") shouldBe "external_row_id"
  }

  test("parseArgs reports a missing --join-key value") {
    val error = intercept[IllegalArgumentException] {
      BackfillApp.parseArgs(Array("--join-key"))
    }
    error.getMessage should include("Missing value for --join-key")
  }

  test("buildConfig maps --join-key to a physical field") {
    val config = BackfillApp.buildConfig(
      Map(
        "s3-endpoint" -> "endpoint",
        "s3-bucket" -> "bucket",
        "join-key" -> "  external_row_id  "
      )
    )

    config.joinKey shouldBe BackfillJoinKey.PhysicalField("external_row_id")
    config.validate() shouldBe Right(())
  }

  test("buildConfig keeps primary-key join when --join-key is omitted") {
    val config = BackfillApp.buildConfig(
      Map("s3-endpoint" -> "endpoint", "s3-bucket" -> "bucket")
    )

    config.joinKey shouldBe BackfillJoinKey.PrimaryKey
  }

  test("buildConfig leaves a blank join key for config validation") {
    val config = BackfillApp.buildConfig(
      Map(
        "s3-endpoint" -> "endpoint",
        "s3-bucket" -> "bucket",
        "join-key" -> "   "
      )
    )

    config.validate().left.toOption.get should include("cannot be blank")
  }

  test("buildConfig maps --input-format and defaults it to parquet") {
    val explicit = BackfillApp.buildConfig(
      Map(
        "s3-endpoint" -> "endpoint",
        "s3-bucket" -> "bucket",
        "input-format" -> " lance "
      )
    )
    explicit.inputFormat shouldBe "lance"

    val defaulted = BackfillApp.buildConfig(
      Map(
        "s3-endpoint" -> "endpoint",
        "s3-bucket" -> "bucket",
        "input-format" -> ""
      )
    )
    defaulted.inputFormat shouldBe BackfillConfig.DefaultInputFormat
  }

  test("parseArgs throws when key/value flag is followed by another flag") {
    val ex = intercept[IllegalArgumentException] {
      BackfillApp.parseArgs(Array("--parquet", "--snapshot", "/tmp/snap.json"))
    }
    ex.getMessage should include("Missing value for --parquet")
  }

  test(
    "parseArgs throws when source key/value flag is followed by another flag"
  ) {
    val ex = intercept[IllegalArgumentException] {
      BackfillApp.parseArgs(Array("--source-s3-endpoint", "--source-use-iam"))
    }
    ex.getMessage should include("Missing value for --source-s3-endpoint")
  }

  test("parseArgs allows non-flag values that begin with double dash") {
    val parsed = BackfillApp.parseArgs(
      Array(
        "--s3-secret-key",
        "--literal-secret",
        "--parquet",
        "/tmp/data.parquet"
      )
    )
    parsed("s3-secret-key") shouldBe "--literal-secret"
    parsed("parquet") shouldBe "/tmp/data.parquet"
  }

  test("parseArgs throws on unexpected positional arg") {
    an[IllegalArgumentException] should be thrownBy {
      BackfillApp.parseArgs(Array("oops"))
    }
  }

  // ============ validate() invariants ============

  test("validate fails on empty s3Endpoint") {
    val cfg = BackfillConfig(
      s3Endpoint = "",
      s3BucketName = "b",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    cfg.validate() shouldBe Left("s3Endpoint cannot be empty")
  }

  test("validate fails on empty s3BucketName") {
    val cfg = BackfillConfig(
      s3Endpoint = "ep",
      s3BucketName = "",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    cfg.validate() shouldBe Left("s3BucketName cannot be empty")
  }

  test("validate fails on non-positive batchSize") {
    val cfg = BackfillConfig(
      s3Endpoint = "ep",
      s3BucketName = "b",
      s3AccessKey = "ak",
      s3SecretKey = "sk",
      batchSize = 0
    )
    cfg.validate() shouldBe Left("batchSize must be positive")
  }

  test("validate accepts empty AK/SK (IAM/IRSA)") {
    val cfg = BackfillConfig(
      s3Endpoint = "ep",
      s3BucketName = "b",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    cfg.validate() shouldBe Right(())
  }

  test("validateForClientMode requires milvusUri and collectionName") {
    val base = BackfillConfig(
      s3Endpoint = "ep",
      s3BucketName = "b",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    base.validateForClientMode().isLeft shouldBe true
    base
      .copy(milvusUri = "http://x:19530")
      .validateForClientMode()
      .isLeft shouldBe true
    base
      .copy(milvusUri = "http://x:19530", collectionName = "c")
      .validateForClientMode() shouldBe Right(())
  }

  // ============ snapshot metadata and source lifecycle ============

  test("snapshot metadata base paths strip the configured Milvus endpoint") {
    val metadata = SnapshotJson
      .parse(
        """{
          |  "snapshot_info": {
          |    "name": "s1",
          |    "collection_id": 10,
          |    "partition_ids": [20],
          |    "create_ts": 1
          |  },
          |  "collection": {"schema": {"name": "c", "fields": []}},
          |  "manifest_list": [],
          |  "storagev2_manifest_list": [{
          |    "segmentID": 30,
          |    "manifest": "{\"ver\":7,\"base_path\":\"s3://minio:9000/managed-bucket/files/insert_log/10/20/30\"}"
          |  }]
          |}""".stripMargin
      )
      .toOption
      .get

    val (collectionId, partitions, basePaths) =
      MilvusBackfill.extractMetadataFromSnapshot(
        metadata,
        bucket = "managed-bucket",
        endpoint = "minio:9000"
      )

    collectionId shouldBe 10L
    partitions shouldBe Map(30L -> 20L)
    basePaths shouldBe Map(30L -> "files/insert_log/10/20/30")
  }

  test("OSS source checkpoint exposes and releases its persisted RDD") {
    val session = spark
    import session.implicits._

    val source = MilvusBackfill.localCheckpointBackfillData(
      spark,
      Seq((1L, "one"), (2L, "two")).toDF("id", "value")
    )
    val checkpointRDD = source.checkpointRDD.get
    try {
      source.dataFrame.collect().map(_.getLong(0)).toSeq shouldBe Seq(1L, 2L)
      checkpointRDD.getStorageLevel should not be StorageLevel.NONE
      spark.sparkContext.getPersistentRDDs.keySet should contain(
        checkpointRDD.id
      )
    } finally {
      checkpointRDD.unpersist(blocking = true)
    }
    spark.sparkContext.getPersistentRDDs.keySet should not contain checkpointRDD.id
  }

  // ============ parseArgs whitelist ============

  test("parseArgs rejects unknown flags (typo guard)") {
    // Common typos like --s3access-key (missing dash) should be rejected at
    // parse time instead of being silently absorbed and later ignored.
    val ex = intercept[IllegalArgumentException] {
      BackfillApp.parseArgs(Array("--s3access-key", "ak"))
    }
    ex.getMessage should include("Unknown argument")
  }

  test("parseArgs accepts the full known flag set") {
    // Smoke test that every documented CLI flag is in the whitelist.
    val args = (BackfillApp.KvFlags.toSeq.flatMap(k =>
      Seq("--" + k, "value")
    ) ++ BackfillApp.BoolFlags.toSeq.map("--" + _)).toArray
    val parsed = BackfillApp.parseArgs(args)
    parsed.keySet shouldBe BackfillApp.KnownFlags
  }

  // ============ FFI options skip static credentials in IAM mode ============

  test("getMilvusReadOptions omits AK/SK when s3UseIam=true") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "irsa-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val opts = cfg.getMilvusReadOptions
    opts.get("fs.access_key_id") shouldBe None
    opts.get("fs.access_key_value") shouldBe None
    opts("fs.use_iam") shouldBe "true"
  }

  test("MilvusBackfill.run derives Hadoop AssumeRole for embedded callers") {
    val hc = spark.sparkContext.hadoopConfiguration
    hc.set(
      BackfillConfig.HadoopS3CredentialsProvider,
      BackfillConfig.HadoopS3AssumedRoleProvider
    )
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "embedded-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )

    try {
      MilvusBackfill.run(spark, "file:///unused.parquet", "", cfg) match {
        case Left(error) =>
          error.message should include(
            "Invalid Hadoop storage AssumeRole configuration"
          )
          error.message should include(BackfillConfig.HadoopS3AssumedRoleArn)
        case Right(_) => fail("expected invalid Hadoop AssumeRole config")
      }
    } finally {
      hc.unset(BackfillConfig.HadoopS3CredentialsProvider)
      hc.unset(BackfillConfig.HadoopS3AssumedRoleArn)
    }
  }

  test("MilvusBackfill.run rejects a physical join key without a snapshot") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "embedded-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true,
      joinKey = BackfillJoinKey.PhysicalField("external_id")
    )

    // An empty snapshot path selects the client-mode path; a physical key has
    // no schema to resolve against there, so run() must fail before it tries
    // to build a MilvusClient or read the input.
    MilvusBackfill.run(spark, "file:///unused.parquet", "", cfg) match {
      case Left(error) =>
        error.message should include("external_id")
        error.message should include("requires a snapshot schema")
      case Right(_) => fail("expected physical join key to require a snapshot")
    }
  }

  test(
    "MilvusBackfill.run rejects unimplemented inputFormat at config validation"
  ) {
    BackfillConfig.AllowedInputFormats
      .diff(BackfillConfig.ImplementedInputFormats)
      .foreach { format =>
        val cfg = BackfillConfig(
          s3Endpoint = "localhost:9000",
          s3BucketName = "b",
          s3AccessKey = "minioadmin",
          s3SecretKey = "minioadmin",
          inputFormat = format
        )
        // An empty snapshotPath would otherwise be read; failing at
        // config.validate() proves the error short-circuits before any
        // snapshot/S3 work.
        MilvusBackfill.run(spark, "file:///unused.input", "", cfg) match {
          case Left(error) =>
            error.message should include("Invalid configuration")
            error.message should include(format)
          case Right(_) =>
            fail(s"expected unimplemented inputFormat '$format' to fail")
        }
      }
  }

  test("getMilvusReadOptions includes AK/SK when s3UseIam=false") {
    val cfg = BackfillConfig(
      s3Endpoint = "minio:9000",
      s3BucketName = "b",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    val opts = cfg.getMilvusReadOptions
    opts("fs.access_key_id") shouldBe "ak"
    opts("fs.access_key_value") shouldBe "sk"
  }

  test("getS3WriteOptionsForBasePath omits AK/SK when s3UseIam=true") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.us-west-2.amazonaws.com",
      s3BucketName = "irsa-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    val opts = cfg.getS3WriteOptionsForBasePath("base/path")
    opts.get("fs.access_key_id") shouldBe None
    opts.get("fs.access_key_value") shouldBe None
    opts("fs.use_iam") shouldBe "true"
  }

  test("Backfill native options include the configured AssumeRole settings") {
    val cfg = BackfillConfig(
      s3Endpoint = "oss-cn-shanghai.aliyuncs.com",
      s3BucketName = "managed-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3CloudProvider = "aliyun",
      s3UseIam = true,
      s3RoleArn = Some("acs:ram::123456789012:role/data-role"),
      s3RoleSessionName = Some("spark-job"),
      s3ExternalId = Some("external-id")
    )

    Seq(
      cfg.getMilvusReadOptions,
      cfg.getS3WriteOptionsForBasePath("base/path")
    ).foreach { opts =>
      opts("fs.role_arn") shouldBe
        "acs:ram::123456789012:role/data-role"
      opts("fs.session_name") shouldBe "spark-job"
      opts("fs.external_id") shouldBe "external-id"
      opts("fs.cloud_provider") shouldBe "aliyun"
      opts.get("fs.access_key_id") shouldBe None
      opts.get("fs.access_key_value") shouldBe None
    }
  }

  test("MilvusDataSource fully qualified class name resolves") {
    // MilvusBackfill.readCollectionWithMetadata uses
    // spark.read.format("com.zilliz.spark.connector.sources.MilvusDataSource")
    // to avoid shortName collisions with other connectors. This test pins the
    // FQCN so a future rename or move is caught at unit-test time.
    val fqcn = "com.zilliz.spark.connector.sources.MilvusDataSource"
    noException should be thrownBy Class.forName(fqcn)
  }
}
