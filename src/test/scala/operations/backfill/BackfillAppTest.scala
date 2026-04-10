package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Unit tests for BackfillApp.parseArgs,
  * MilvusBackfill.configureHadoopS3ForPath, the MilvusDataSource FQCN used by
  * spark.read.format(...), and the new IAM/IRSA invariants of
  * BackfillConfig.validate().
  */
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

  test("parseArgs throws on missing value for non-flag") {
    an[IllegalArgumentException] should be thrownBy {
      BackfillApp.parseArgs(Array("--parquet"))
    }
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

  // ============ configureHadoopS3ForPath ============

  test("configureHadoopS3ForPath is a no-op for non-s3 paths") {
    val cfg = BackfillConfig(
      s3Endpoint = "ep",
      s3BucketName = "b",
      s3AccessKey = "ak",
      s3SecretKey = "sk"
    )
    val key = "fs.s3a.bucket.never.endpoint"
    spark.sparkContext.hadoopConfiguration.unset(key)
    MilvusBackfill.configureHadoopS3ForPath(
      spark,
      "file:///tmp/x",
      cfg,
      isSource = false
    )
    spark.sparkContext.hadoopConfiguration.get(key) shouldBe null
  }

  test("configureHadoopS3ForPath writes per-bucket static credentials") {
    val cfg = BackfillConfig(
      s3Endpoint = "minio:9000",
      s3BucketName = "main-bucket",
      s3AccessKey = "ak1",
      s3SecretKey = "sk1",
      s3UseSSL = false
    )
    MilvusBackfill.configureHadoopS3ForPath(
      spark,
      "s3a://main-bucket/path/to/file",
      cfg,
      isSource = false
    )
    val hc = spark.sparkContext.hadoopConfiguration
    hc.get("fs.s3a.bucket.main-bucket.endpoint") shouldBe "minio:9000"
    hc.get("fs.s3a.bucket.main-bucket.path.style.access") shouldBe "true"
    hc.get(
      "fs.s3a.bucket.main-bucket.connection.ssl.enabled"
    ) shouldBe "false"
    hc.get("fs.s3a.bucket.main-bucket.access.key") shouldBe "ak1"
    hc.get("fs.s3a.bucket.main-bucket.secret.key") shouldBe "sk1"
    hc.get(
      "fs.s3a.bucket.main-bucket.aws.credentials.provider"
    ) shouldBe "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  }

  test("configureHadoopS3ForPath uses IAM provider when useIam=true") {
    val cfg = BackfillConfig(
      s3Endpoint = "s3.amazonaws.com",
      s3BucketName = "irsa-bucket",
      s3AccessKey = "",
      s3SecretKey = "",
      s3UseIam = true
    )
    MilvusBackfill.configureHadoopS3ForPath(
      spark,
      "s3a://irsa-bucket/key",
      cfg,
      isSource = false
    )
    val hc = spark.sparkContext.hadoopConfiguration
    val provider =
      hc.get("fs.s3a.bucket.irsa-bucket.aws.credentials.provider")
    // The IRSA-friendly chain must include WebIdentityTokenCredentialsProvider
    // (used by both EKS service account tokens and GKE Workload Identity)
    // and IAMInstanceCredentialsProvider for the EC2/EKS node fallback.
    // The legacy v1 DefaultAWSCredentialsProviderChain must NOT be used
    // because it has been unreliable on EKS pods.
    provider should include(
      "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    )
    provider should include(
      "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
    )
    provider should not include "DefaultAWSCredentialsProviderChain"
    // No static keys should be written in IAM mode
    hc.get("fs.s3a.bucket.irsa-bucket.access.key") shouldBe null
    hc.get("fs.s3a.bucket.irsa-bucket.secret.key") shouldBe null
  }

  // ============ normalizeS3Scheme ============

  test("normalizeS3Scheme rewrites s3:// to s3a://") {
    MilvusBackfill.normalizeS3Scheme(
      "s3://bucket/key.parquet"
    ) shouldBe "s3a://bucket/key.parquet"
  }

  test("normalizeS3Scheme leaves s3a:// and other schemes alone") {
    MilvusBackfill.normalizeS3Scheme(
      "s3a://bucket/key"
    ) shouldBe "s3a://bucket/key"
    MilvusBackfill.normalizeS3Scheme(
      "file:///tmp/x"
    ) shouldBe "file:///tmp/x"
    MilvusBackfill.normalizeS3Scheme("/local/path") shouldBe "/local/path"
    MilvusBackfill.normalizeS3Scheme(null) shouldBe null
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
    val opts = cfg.getS3WriteOptionsForBasePath("base/path", 1L)
    opts.get("fs.access_key_id") shouldBe None
    opts.get("fs.access_key_value") shouldBe None
    opts("fs.use_iam") shouldBe "true"
  }

  test(
    "configureHadoopS3ForPath with isSource=true honors source overrides and falls back to main"
  ) {
    val cfg = BackfillConfig(
      s3Endpoint = "minio:9000",
      s3BucketName = "main-bucket",
      s3AccessKey = "main-ak",
      s3SecretKey = "main-sk",
      sourceS3Endpoint = Some("src:9000"),
      sourceS3AccessKey = Some("src-ak"),
      sourceS3SecretKey = Some("src-sk")
      // sourceS3UseSSL/UseIam left None — should fall back to main
    )
    MilvusBackfill.configureHadoopS3ForPath(
      spark,
      "s3a://source-bucket/data.parquet",
      cfg,
      isSource = true
    )
    val hc = spark.sparkContext.hadoopConfiguration
    hc.get("fs.s3a.bucket.source-bucket.endpoint") shouldBe "src:9000"
    hc.get("fs.s3a.bucket.source-bucket.access.key") shouldBe "src-ak"
    hc.get("fs.s3a.bucket.source-bucket.secret.key") shouldBe "src-sk"
  }

  // ============ FQCN smoke test ============

  test("MilvusDataSource fully qualified class name resolves") {
    // MilvusBackfill.readCollectionWithMetadata uses
    // spark.read.format("com.zilliz.spark.connector.sources.MilvusDataSource")
    // to avoid shortName collisions with other connectors. This test pins the
    // FQCN so a future rename or move is caught at unit-test time.
    val fqcn = "com.zilliz.spark.connector.sources.MilvusDataSource"
    noException should be thrownBy Class.forName(fqcn)
  }
}
