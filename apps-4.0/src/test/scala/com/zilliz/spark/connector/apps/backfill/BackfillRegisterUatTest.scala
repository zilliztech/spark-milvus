package com.zilliz.spark.connector.apps.backfill

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.procedure.Register
import io.milvus.grpc.schema.{DataType, FieldSchema}

/** The first write scenario that reaches Milvus end to end: a backfill adds a
  * column to a live collection's segments, `register` hands the new manifest
  * versions to Milvus through BatchUpdateManifest, and a query through the
  * service returns the column (capabilities W2's write half and A4's backfill
  * branch).
  *
  * Two scenarios, both cancelled without their environment:
  * {{{
  *   prepare:  MILVUS_UAT_URI, MILVUS_UAT_TOKEN, MILVUS_UAT_BACKFILL_COLLECTION
  *             adds a nullable Int64 field and takes a snapshot; prints the path
  *   run:      the above plus MILVUS_UAT_SNAPSHOT_PATH (the snapshot just taken),
  *             MILVUS_JNI_S3_BUCKET, MILVUS_JNI_S3_ROOT_PATH, AWS_*
  *             writes the source parquet, backfills, registers, queries
  * }}}
  */
class BackfillRegisterUatTest extends AnyFunSuite with Matchers {

  private def env(n: String): Option[String] =
    sys.env.get(n).map(_.trim).filter(_.nonEmpty)

  private val newField = "score"

  private def clientOf(uri: String) =
    MilvusClient(
      MilvusOption(
        Map(MilvusOption.MilvusUri -> uri) ++
          env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
      ).connectionParams
    )

  test("prepare: add a nullable field to the collection and take a snapshot") {
    val uri = env("MILVUS_UAT_URI").getOrElse(cancel("set MILVUS_UAT_URI"))
    val collection = env("MILVUS_UAT_BACKFILL_COLLECTION")
      .getOrElse(cancel("set MILVUS_UAT_BACKFILL_COLLECTION"))
    val client = clientOf(uri)
    try {
      client
        .addCollectionField(
          "default",
          collection,
          FieldSchema(
            name = newField,
            dataType = DataType.Int64,
            nullable = true
          )
        )
        .get
      val schema = client.getCollectionSchema("default", collection).get
      info(
        s"fields now: ${schema.fields.map(f => s"${f.name}:${f.fieldID}").mkString(", ")}"
      )
      schema.fields.map(_.name) should contain(newField)
      val snapshot = client
        .createSnapshotForRead(
          "default",
          collection,
          s"spark_uat_bf_${System.currentTimeMillis()}",
          "before backfill",
          3600L
        )
        .get
      info(s"export MILVUS_UAT_SNAPSHOT_PATH=${snapshot.s3Location}")
    } finally client.close()
  }

  test(
    "run: backfill a column, register the manifests, query the column back"
  ) {
    val uri = env("MILVUS_UAT_URI").getOrElse(cancel("set MILVUS_UAT_URI"))
    val collection = env("MILVUS_UAT_BACKFILL_COLLECTION")
      .getOrElse(cancel("set MILVUS_UAT_BACKFILL_COLLECTION"))
    val snapshotPath = env("MILVUS_UAT_SNAPSHOT_PATH")
      .getOrElse(cancel("set MILVUS_UAT_SNAPSHOT_PATH"))
    val bucket =
      env("MILVUS_JNI_S3_BUCKET").getOrElse(cancel("set MILVUS_JNI_S3_BUCKET"))
    val rootPath = env("MILVUS_JNI_S3_ROOT_PATH")
      .getOrElse(cancel("set MILVUS_JNI_S3_ROOT_PATH, the instance's root"))
    val region = env("MILVUS_JNI_S3_REGION").getOrElse("us-west-2")
    val stagingRoot =
      env("MILVUS_UAT_WRITE_PREFIX").getOrElse("spark-uat-write")

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("backfill-register-uat")
      .config("spark.ui.enabled", "false")
      .config(
        "spark.sql.extensions",
        "com.zilliz.spark.connector.extensions.MilvusSparkSessionExtensions"
      )
      .getOrCreate()
    try {
      // The source: one row per key of the collection (ids 0..2999 on UAT),
      // as a local parquet file; the segments are read and written through
      // the native store, so Spark itself never touches the bucket.
      val sourcePath = java.nio.file.Files
        .createTempDirectory("backfill-src")
        .resolve("source.parquet")
        .toString
      spark
        .range(3000)
        .select(col("id").as("pk"), (col("id") * 2).as(newField))
        .write
        .parquet(sourcePath)
      info(s"source parquet at $sourcePath")

      val config = BackfillConfig(
        milvusUri = uri,
        milvusToken = env("MILVUS_UAT_TOKEN").getOrElse(""),
        collectionName = collection,
        s3Endpoint = s"s3.$region.amazonaws.com",
        s3BucketName = bucket,
        s3AccessKey = "",
        s3SecretKey = "",
        s3UseSSL = true,
        s3RootPath = rootPath,
        s3Region = region,
        s3UseIam = true,
        stagingRoot = Some(stagingRoot),
        jobId = Some(s"backfill-${System.currentTimeMillis()}")
      )
      val result = MilvusBackfill
        .run(spark, sourcePath, snapshotPath, config)
        .fold(e => fail(s"backfill failed: $e"), identity)
      info(
        s"backfill: ${result.segmentsProcessed} segment(s), ${result.totalRowsWritten} rows, staging ${result.stagingPrefix}"
      )
      result.stagingPrefix should not be empty
      result.segmentResults.values.foreach(r =>
        r.committedVersion should be > 0L
      )

      val options = Map(
        MilvusOption.MilvusUri -> uri,
        MilvusOption.MilvusCollectionName -> collection,
        StorageProperties.BucketName -> bucket,
        StorageProperties.Address -> s"s3.$region.amazonaws.com",
        StorageProperties.Region -> region,
        StorageProperties.CloudProvider -> "aws",
        StorageProperties.UseSSL -> "true",
        StorageProperties.UseIam -> "true",
        StorageProperties.RootPath -> rootPath
      ) ++ env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
      // The SQL front (work item #17) first, then the Scala entry point,
      // which finds the job already registered.
      def sqlValue(v: String) = "'" + v.replace("'", "''") + "'"
      val optionArgs = (options - MilvusOption.MilvusCollectionName)
        .map { case (k, v) => s"`$k` => ${sqlValue(v)}" }
        .mkString(",\n  ")
      val call =
        s"""CALL milvus.system.register(${sqlValue(collection)},
           |  staging => ${sqlValue(result.stagingPrefix)},
           |  $optionArgs)""".stripMargin
      info(call.replaceAll("(`milvus.token` => )'[^']*'", "$1'***'"))
      val registered = spark.sql(call).collect()
      info(
        s"CALL returned ${registered.length} row(s): ${registered.mkString(", ")}"
      )
      registered.length should be > 0
      registered.foreach { r =>
        r.getString(3) shouldBe "registered"
        r.getLong(2) should be > 0L
      }
      val again = spark.sql(call).collect()
      again.foreach(r => r.getString(3) shouldBe "already_registered")
      Register
        .run(options, result.stagingPrefix)
        .alreadyRegistered shouldBe true

      val client = clientOf(uri)
      try {
        // A query goes through the loaded collection, and loading needs an
        // index on the vector field; both are idempotent enough for a test.
        client.createIndex("default", collection, "v") match {
          case scala.util.Success(_) => info("index created on v")
          case scala.util.Failure(e) if e.getMessage.contains("exist") =>
            info("index on v already exists")
          case scala.util.Failure(e) => throw e
        }
        client.loadCollection("default", collection).get
        val deadline = System.currentTimeMillis() + 300000L
        var state = client.getLoadState("default", collection).get
        while (
          state != io.milvus.grpc.common.LoadState.LoadStateLoaded &&
          System.currentTimeMillis() < deadline
        ) {
          Thread.sleep(3000)
          state = client.getLoadState("default", collection).get
        }
        info(s"load state: $state")
        state shouldBe io.milvus.grpc.common.LoadState.LoadStateLoaded

        val fields = client
          .query(
            "default",
            collection,
            "id in [10, 11, 12]",
            Seq("id", newField)
          )
          .get
        info(
          s"query: ${fields.map(f => s"${f.fieldName}=${f.field}").mkString(" | ")}"
        )
        val scores = fields
          .find(_.fieldName == newField)
          .getOrElse(fail(s"$newField not returned"))
          .getScalars
          .getLongData
          .data
        scores.sorted shouldBe Seq(20L, 22L, 24L)
      } finally client.close()
    } finally spark.stop()
  }
}
