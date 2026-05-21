package com.zilliz.spark.connector.tools

import java.util.Base64

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.read.MilvusSnapshotReader
import com.zilliz.spark.connector.MilvusOption

class MilvusReadAppTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("MilvusReadAppTest")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("parseArgs parses client mode arguments") {
    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "client",
        "--milvus-uri",
        "http://127.0.0.1:19530",
        "--milvus-token",
        "root:Milvus",
        "--database",
        "default",
        "--collection",
        "book",
        "--s3-endpoint",
        "127.0.0.1:9000",
        "--s3-bucket",
        "a-bucket",
        "--s3-root-path",
        "files",
        "--s3-access-key",
        "ak",
        "--s3-secret-key",
        "sk",
        "--s3-region",
        "us-east-1",
        "--show",
        "10",
        "--count",
        "--print-schema",
        "--debug-read",
        "--client-snapshot-name",
        "spark-read-test",
        "--client-snapshot-description",
        "test snapshot",
        "--client-snapshot-compaction-protection-seconds",
        "60",
        "--snapshot-max-json-bytes",
        "1024",
        "--spark-log-level",
        "WARN"
      )
    )

    args.mode shouldBe "client"
    args.milvusUri shouldBe "http://127.0.0.1:19530"
    args.collection shouldBe "book"
    args.s3Bucket shouldBe "a-bucket"
    args.show shouldBe Some(10)
    args.count shouldBe true
    args.printSchema shouldBe true
    args.debugRead shouldBe true
    args.clientSnapshotName shouldBe Some("spark-read-test")
    args.clientSnapshotDescription shouldBe Some("test snapshot")
    args.clientSnapshotCompactionProtectionSeconds shouldBe Some(60L)
    args.snapshotMaxJsonBytes shouldBe Some(1024L)
    args.sparkLogLevel shouldBe Some("WARN")
  }

  test("parseArgs parses snapshot mode arguments") {
    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "snapshot",
        "--snapshot",
        "s3://a-bucket/files/snapshots/metadata.json",
        "--s3-endpoint",
        "127.0.0.1:9000",
        "--s3-bucket",
        "a-bucket",
        "--s3-root-path",
        "files",
        "--use-iam",
        "--field-ids",
        "100,101",
        "--extra-columns",
        "$segment_id,$row_offset",
        "--output-parquet",
        "/tmp/milvus-read-output"
      )
    )

    args.mode shouldBe "snapshot"
    args.snapshot shouldBe Some("s3://a-bucket/files/snapshots/metadata.json")
    args.useIam shouldBe true
    args.fieldIds shouldBe Some("100,101")
    args.extraColumns shouldBe Some("$segment_id,$row_offset")
    args.outputParquet shouldBe Some("/tmp/milvus-read-output")
  }

  test("parseArgs rejects unknown flags") {
    an[IllegalArgumentException] should be thrownBy {
      MilvusReadApp.parseArgs(Array("--mode", "client", "--bad-flag", "x"))
    }
  }

  test("parseArgs rejects missing value for key flag") {
    an[IllegalArgumentException] should be thrownBy {
      MilvusReadApp.parseArgs(Array("--mode"))
    }
  }

  test("parseArgs rejects unsupported mode") {
    an[IllegalArgumentException] should be thrownBy {
      MilvusReadApp.parseArgs(Array("--mode", "offline"))
    }
  }

  test("parseArgs rejects snapshot mode without snapshot path") {
    an[IllegalArgumentException] should be thrownBy {
      MilvusReadApp.parseArgs(
        Array("--mode", "snapshot", "--s3-bucket", "a-bucket")
      )
    }
  }

  test("parseArgs rejects client-only options in snapshot mode") {
    val err = intercept[IllegalArgumentException] {
      MilvusReadApp.parseArgs(
        Array(
          "--mode",
          "snapshot",
          "--snapshot",
          "src/test/data/sample_snapshot.json",
          "--collection",
          "book",
          "--client-snapshot-name",
          "spark-read-test",
          "--s3-bucket",
          "a-bucket"
        )
      )
    }

    err.getMessage should include("Snapshot mode does not accept")
  }

  test("parseArgs rejects snapshot option in client mode") {
    an[IllegalArgumentException] should be thrownBy {
      MilvusReadApp.parseArgs(
        Array(
          "--mode",
          "client",
          "--snapshot",
          "src/test/data/sample_snapshot.json"
        )
      )
    }
  }

  test("parseArgs rejects non-positive client snapshot protection seconds") {
    an[IllegalArgumentException] should be thrownBy {
      MilvusReadApp.parseArgs(
        Array(
          "--mode",
          "client",
          "--client-snapshot-compaction-protection-seconds",
          "0"
        )
      )
    }
  }

  test("buildClientOptions maps client and storage arguments") {
    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "client",
        "--milvus-uri",
        "http://127.0.0.1:19530",
        "--milvus-token",
        "root:Milvus",
        "--database",
        "default",
        "--collection",
        "book",
        "--partition-name",
        "p1",
        "--s3-endpoint",
        "127.0.0.1:9000",
        "--s3-bucket",
        "a-bucket",
        "--s3-root-path",
        "files",
        "--s3-access-key",
        "ak",
        "--s3-secret-key",
        "sk",
        "--s3-region",
        "us-east-1",
        "--s3-use-ssl",
        "--field-ids",
        "100,101",
        "--extra-columns",
        "$segment_id,$row_offset",
        "--debug-read",
        "--client-snapshot-name",
        "spark-read-test",
        "--client-snapshot-description",
        "test snapshot",
        "--client-snapshot-compaction-protection-seconds",
        "60",
        "--snapshot-max-json-bytes",
        "1024"
      )
    )

    val opts = MilvusReadApp.buildClientOptions(args)
    opts(MilvusOption.MilvusUri) shouldBe "http://127.0.0.1:19530"
    opts(MilvusOption.MilvusToken) shouldBe "root:Milvus"
    opts(MilvusOption.MilvusDatabaseName) shouldBe "default"
    opts(MilvusOption.MilvusCollectionName) shouldBe "book"
    opts(MilvusOption.MilvusPartitionName) shouldBe "p1"
    opts(MilvusOption.ReaderFieldIDs) shouldBe "100,101"
    opts(MilvusOption.MilvusExtraColumns) shouldBe "$segment_id,$row_offset"
    opts(MilvusOption.ReaderDebug) shouldBe "true"
    opts(MilvusOption.ClientSnapshotName) shouldBe "spark-read-test"
    opts(MilvusOption.ClientSnapshotDescription) shouldBe "test snapshot"
    opts(
      MilvusOption.ClientSnapshotCompactionProtectionSeconds
    ) shouldBe "60"
    opts(MilvusOption.SnapshotMaxJsonBytes) shouldBe "1024"
    opts(Properties.FsConfig.FsAddress) shouldBe "127.0.0.1:9000"
    opts(Properties.FsConfig.FsBucketName) shouldBe "a-bucket"
    opts(Properties.FsConfig.FsRootPath) shouldBe "files"
    opts(Properties.FsConfig.FsAccessKeyId) shouldBe "ak"
    opts(Properties.FsConfig.FsAccessKeyValue) shouldBe "sk"
    opts(Properties.FsConfig.FsRegion) shouldBe "us-east-1"
    opts(Properties.FsConfig.FsUseSSL) shouldBe "true"
  }

  test("buildClientOptions rejects missing required client arguments") {
    val args = MilvusReadApp.parseArgs(Array("--mode", "client"))
    an[IllegalArgumentException] should be thrownBy {
      MilvusReadApp.buildClientOptions(args)
    }
  }

  test("buildStorageOptions omits AK/SK in IAM mode") {
    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "snapshot",
        "--snapshot",
        "src/test/data/sample_snapshot.json",
        "--s3-bucket",
        "a-bucket",
        "--use-iam"
      )
    )

    val opts = MilvusReadApp.buildStorageOptions(args)
    opts(Properties.FsConfig.FsUseIam) shouldBe "true"
    opts.get(Properties.FsConfig.FsAccessKeyId) shouldBe None
    opts.get(Properties.FsConfig.FsAccessKeyValue) shouldBe None
  }

  test("buildSnapshotOptionsFromMetadata emits empty manifest option") {
    val source = scala.io.Source.fromFile("src/test/data/sample_snapshot.json")
    val json =
      try source.mkString
      finally source.close()
    val metadata = MilvusSnapshotReader
      .parseSnapshotMetadata(json)
      .toOption
      .get
      .copy(manifestList = Seq.empty, storageV2ManifestList = Some(Seq.empty))
    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "snapshot",
        "--snapshot",
        "src/test/data/sample_snapshot.json",
        "--s3-bucket",
        "a-bucket"
      )
    )

    val opts = MilvusReadApp.buildSnapshotOptionsFromMetadata(
      args,
      metadata,
      json,
      v2Segments = Seq.empty
    )

    opts(MilvusOption.SnapshotManifests) shouldBe "[]"
    opts should not contain key(MilvusOption.SnapshotV2Segments)
  }

  test(
    "buildSnapshotOptionsFromMetadata includes schema bytes and manifest options"
  ) {
    val source = scala.io.Source.fromFile("src/test/data/sample_snapshot.json")
    val json =
      try source.mkString
      finally source.close()
    val metadata = MilvusSnapshotReader
      .parseSnapshotMetadata(json)
      .toOption
      .get

    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "snapshot",
        "--snapshot",
        "src/test/data/sample_snapshot.json",
        "--s3-endpoint",
        "127.0.0.1:9000",
        "--s3-bucket",
        "a-bucket",
        "--s3-root-path",
        "files",
        "--s3-access-key",
        "ak",
        "--s3-secret-key",
        "sk",
        "--field-ids",
        "100",
        "--extra-columns",
        "$segment_id,$row_offset",
        "--snapshot-max-json-bytes",
        "1024"
      )
    )

    val opts = MilvusReadApp.buildSnapshotOptionsFromMetadata(
      args,
      metadata,
      json,
      v2Segments = Seq.empty
    )

    opts(MilvusOption.SnapshotMode) shouldBe "true"
    opts should not contain key(MilvusOption.MilvusUri)
    opts(
      MilvusOption.MilvusCollectionName
    ) shouldBe metadata.collection.schema.name
    opts(
      MilvusOption.SnapshotCollectionId
    ) shouldBe metadata.snapshotInfo.collectionId.toString
    opts(
      MilvusOption.SnapshotPartitionIds
    ) shouldBe metadata.snapshotInfo.partitionIds.mkString(",")
    opts(MilvusOption.SnapshotSchemaJson) shouldBe json
    opts(MilvusOption.ReaderFieldIDs) shouldBe "100"
    opts(MilvusOption.MilvusExtraColumns) shouldBe "$segment_id,$row_offset"
    opts(MilvusOption.SnapshotMaxJsonBytes) shouldBe "1024"
    opts should contain key MilvusOption.SnapshotSchemaBytes
    Base64.getDecoder
      .decode(opts(MilvusOption.SnapshotSchemaBytes))
      .length should be > 0
    opts should contain key MilvusOption.SnapshotManifests
    opts should not contain key(MilvusOption.SnapshotV2Segments)
  }

  test("normalizeSnapshotPath converts s3 scheme to s3a") {
    MilvusReadApp.normalizeSnapshotPath(
      "s3://bucket/path/to/file.json"
    ) shouldBe
      "s3a://bucket/path/to/file.json"
    MilvusReadApp.normalizeSnapshotPath(
      "s3a://bucket/path/to/file.json"
    ) shouldBe
      "s3a://bucket/path/to/file.json"
    MilvusReadApp.normalizeSnapshotPath(
      "src/test/data/sample_snapshot.json"
    ) shouldBe
      "src/test/data/sample_snapshot.json"
  }

  test("readLocalSnapshotJson reads local snapshot file") {
    val json =
      MilvusReadApp.readLocalSnapshotJson("src/test/data/sample_snapshot.json")
    json should include("snapshot-info")
    json should include("collection")
  }

  test("readLocalSnapshotJson applies explicit max JSON byte limit") {
    an[IllegalArgumentException] should be thrownBy {
      MilvusReadApp.readLocalSnapshotJson(
        "src/test/data/sample_snapshot.json",
        maxBytes = 1L
      )
    }
  }

  test("configureHadoopS3A configures static credentials") {
    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "snapshot",
        "--snapshot",
        "s3://a-bucket/files/snapshots/metadata.json",
        "--s3-endpoint",
        "127.0.0.1:9000",
        "--s3-bucket",
        "a-bucket",
        "--s3-access-key",
        "ak",
        "--s3-secret-key",
        "sk",
        "--s3-region",
        "us-east-1"
      )
    )
    val conf = new Configuration()

    MilvusReadApp.configureHadoopS3A(conf, args)

    conf.get("fs.s3a.bucket.a-bucket.endpoint") shouldBe "127.0.0.1:9000"
    conf.get("fs.s3a.bucket.a-bucket.access.key") shouldBe "ak"
    conf.get("fs.s3a.bucket.a-bucket.secret.key") shouldBe "sk"
    conf.get("fs.s3a.bucket.a-bucket.endpoint.region") shouldBe "us-east-1"
  }

  test("configureHadoopS3A configures IAM credentials chain") {
    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "snapshot",
        "--snapshot",
        "s3://a-bucket/files/snapshots/metadata.json",
        "--s3-bucket",
        "a-bucket",
        "--use-iam"
      )
    )
    val conf = new Configuration()

    MilvusReadApp.configureHadoopS3A(conf, args)

    conf.get("fs.s3a.bucket.a-bucket.aws.credentials.provider") should include(
      "WebIdentityTokenCredentialsProvider"
    )
  }

  test("runActions caches a DataFrame before multiple data actions") {
    val sparkSession = spark
    import sparkSession.implicits._
    val acc = sparkSession.sparkContext.longAccumulator("run-actions-cache")
    val df = sparkSession
      .range(0, 3)
      .map { value =>
        acc.add(1L)
        value
      }
      .toDF("id")
    val args = MilvusReadApp.parseArgs(
      Array("--mode", "client", "--show", "2", "--count")
    )

    MilvusReadApp.runActions(df, args)

    acc.value shouldBe 3L
  }

  test("applyTransformations applies select and where") {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "tag")
    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "client",
        "--select",
        "id",
        "--where",
        "id >= 2"
      )
    )

    val result = MilvusReadApp.applyTransformations(df, args)
    result.columns.toSeq shouldBe Seq("id")
    result.collect().map(_.getLong(0)).toSeq shouldBe Seq(2L, 3L)
  }

  test("applyTransformations filters before selecting columns") {
    val sparkSession = spark
    import sparkSession.implicits._
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "tag")
    val args = MilvusReadApp.parseArgs(
      Array(
        "--mode",
        "client",
        "--select",
        "id",
        "--where",
        "tag = 'b'"
      )
    )

    val result = MilvusReadApp.applyTransformations(df, args)
    result.columns.toSeq shouldBe Seq("id")
    result.collect().map(_.getLong(0)).toSeq shouldBe Seq(2L)
  }
}
