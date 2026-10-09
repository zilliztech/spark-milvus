package com.zilliz.spark.connector.write

import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson
}
import com.zilliz.milvus.storage.write.commit.{Committer, JobManifest}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.options.{
  HadoopStorageKeys,
  MilvusOption,
  StorageOptions
}
import com.zilliz.spark.connector.uat.UatWriteScope
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** The connector alone, against the UAT bucket, with no Milvus service: a
  * DataFrame is written as V3 segments through
  * `df.write.format("milvus").mode("append")`, the job manifest the commit
  * wrote names the segments, and the same rows are read back through
  * `format("milvus")` from those segment manifests (capabilities W1's write
  * half, W3 and R3). The write is pure-connector: the collection schema comes
  * from `milvus.snapshot.schema.bytes`, nothing is registered.
  *
  * Cancels unless the bucket is named:
  * {{{
  *   MILVUS_JNI_S3_BUCKET=bucket
  *   MILVUS_JNI_S3_REGION=us-west-2            # optional
  *   MILVUS_UAT_WRITE_PREFIX=spark-uat-write   # optional, the fs.root_path used
  *   MILVUS_UAT_KEEP_WRITE=true                # optional: skip the abort, keep the files
  *   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
  * }}}
  */
class ConnectorWriteReadUatTest extends AnyFunSuite with Matchers {

  private def env(n: String): Option[String] =
    sys.env.get(n).map(_.trim).filter(_.nonEmpty)

  private val rows = 3000
  private val dim = 4

  private def storageOptions(): Map[String, String] = {
    val bucket = env("MILVUS_JNI_S3_BUCKET").getOrElse(
      cancel("set MILVUS_JNI_S3_BUCKET and AWS_* to reach the UAT bucket")
    )
    val region = env("MILVUS_JNI_S3_REGION").getOrElse("us-west-2")
    Map(
      StorageProperties.BucketName -> bucket,
      StorageProperties.Address -> s"s3.$region.amazonaws.com",
      StorageProperties.Region -> region,
      StorageProperties.CloudProvider -> "aws",
      StorageProperties.UseSSL -> "true",
      StorageProperties.UseIam -> "true",
      StorageProperties.RootPath -> env("MILVUS_UAT_WRITE_PREFIX")
        .getOrElse("spark-uat-write")
    )
  }

  /** The collection schema both the write and the read take, as the protobuf a
    * snapshot would carry: field ids and the vector dimension come from it.
    */
  private val schemaBytes: String = Base64.getEncoder.encodeToString(
    CollectionSchema(
      name = "connector_rt",
      fields = Seq(
        FieldSchema(
          fieldID = 100,
          name = "id",
          dataType = DataType.Int64,
          isPrimaryKey = true
        ),
        FieldSchema(
          fieldID = 101,
          name = "name",
          dataType = DataType.VarChar,
          typeParams = Seq(KeyValuePair("max_length", "64"))
        ),
        FieldSchema(
          fieldID = 102,
          name = "v",
          dataType = DataType.FloatVector,
          typeParams = Seq(KeyValuePair("dim", dim.toString))
        )
      )
    ).toByteArray
  )

  test(
    "a DataFrame written by the connector reads back through the connector"
  ) {
    val configuredStorage = storageOptions()
    val scope = new UatWriteScope(
      configuredStorage(StorageProperties.RootPath),
      "connector-write-read"
    )
    val storage = configuredStorage + (StorageProperties.RootPath -> scope.root)
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("connector-write-read-uat")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      val store = HadoopStorageKeys.storeFrom(storage)
      scope.run(store, keep = env("MILVUS_UAT_KEEP_WRITE").isDefined) {
        val df = spark
          .range(rows)
          .select(
            col("id"),
            concat(lit("row-"), col("id")).as("name"),
            array(
              (0 until dim).map(d => (col("id") * 10 + d).cast("float")): _*
            )
              .as("v")
          )
          .repartition(2)

        // --- the table: snapshot mode with only a schema, no segments ---
        val tableOptions = storage ++ Map(
          MilvusOption.SnapshotMode -> "true",
          MilvusOption.SnapshotSchemaBytes -> schemaBytes,
          MilvusOption.SnapshotCollectionId -> "1",
          MilvusOption.SnapshotPartitionIds -> "0",
          MilvusOption.MilvusCollectionName -> "connector_rt",
          MilvusOption.MilvusInsertMaxBatchSize -> "1000"
        )

        // --- what the write protocol refuses before any task starts ---
        val overwrite = intercept[AnalysisException](
          df.write
            .format("milvus")
            .mode("overwrite")
            .options(tableOptions)
            .save()
        )
        info(
          s"overwrite refused by Spark: ${overwrite.getMessage.linesIterator.next()}"
        )
        val incomplete = intercept[Exception](
          df.drop("v")
            .write
            .format("milvus")
            .mode("append")
            .options(tableOptions)
            .save()
        )
        incomplete.getMessage should include("missing from the DataFrame")

        // --- write: two tasks, two V3 segments under {root}/staging/{job}/ ---
        df.write.format("milvus").mode("append").options(tableOptions).save()

        // --- commit: the job is the one staging prefix with a marker ---
        val root = storage(StorageProperties.RootPath)
        val (layout, manifest) = {
          val committed = store
            .list(s"$root/staging", recursive = false)
            .filter(_.isDirectory)
            .map(d =>
              StagingLayout(root, d.path.stripSuffix("/").split("/").last)
            )
            .filter(l => store.exists(l.marker))
          withClue(
            s"one committed job expected under the owned directory $root/staging: "
          )(committed.size shouldBe 1)
          val layout = committed.head
          val manifest = JobManifest
            .fromJson(
              new String(store.readAll(layout.manifest), StandardCharsets.UTF_8)
            )
            .fold(e => throw e, identity)
          (layout, manifest)
        }
        info(
          s"job manifest: ${manifest.jobId}, ${manifest.segments.size} segments, ${manifest.rowCount} rows"
        )
        manifest.jobId shouldBe layout.jobId
        manifest.segments.size shouldBe 2
        manifest.rowCount shouldBe rows.toLong
        manifest.segments.foreach { seg =>
          seg.manifestVersion shouldBe 1L
          seg.basePath should startWith(layout.prefix + "/")
        }
        val basePaths = manifest.segments.map(_.basePath)
        info(s"wrote ${basePaths.size} segment(s): ${basePaths.mkString(", ")}")

        // --- read: the manifests the write produced, no service in between ---
        val manifests = SegmentListJson.encodeManifestItems(
          basePaths.zipWithIndex.map { case (path, i) =>
            ManifestItemJson(i + 1L, s"""{"ver":-1,"base_path":"$path"}""")
          }
        )
        val readOptions =
          tableOptions + (MilvusOption.SnapshotManifests -> manifests)
        val back = spark.read.format("milvus").options(readOptions).load()
        info(s"schema: ${back.schema.treeString}")
        val got = back.collect().map(r => r.getLong(0) -> r).toMap
        got.size shouldBe rows
        (0L until rows.toLong).foreach { i =>
          val r = got(i)
          r.getString(1) shouldBe s"row-$i"
          r.getSeq[Float](2) shouldBe (0 until dim).map(d =>
            (i * 10 + d).toFloat
          )
        }
        val columnar = spark.read
          .format("milvus")
          .options(readOptions + (MilvusOption.ReadColumnar -> "true"))
          .load()
          .count()
        columnar shouldBe rows.toLong

        // --- abort: the same committer deletes the job's files. The zero-byte
        // directory markers milvus-storage created stay: the loon C API has no
        // directory delete and refuses them as "not a file" ---
        if (env("MILVUS_UAT_KEEP_WRITE").isDefined) {
          info(
            s"MILVUS_UAT_KEEP_WRITE set: leaving ${layout.prefix} for inspection"
          )
        } else {
          val deleted = new Committer(store, layout).abort()
          info(s"abort deleted $deleted files under ${layout.prefix}")
          deleted should be >= 6 // two parquet, two manifests, manifest.json, _committed
          store
            .list(layout.prefix, recursive = true)
            .filterNot(_.isDirectory) shouldBe empty
          store.exists(layout.marker) shouldBe false
        }
      }
    } finally spark.stop()
  }
}
