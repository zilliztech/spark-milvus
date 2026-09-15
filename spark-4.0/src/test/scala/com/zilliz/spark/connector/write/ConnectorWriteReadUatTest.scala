package com.zilliz.spark.connector.write

import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
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
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** The connector alone, against the UAT bucket, with no Milvus service: a
  * DataFrame is written as V3 segments through the connector's writer, and the
  * same rows are read back through `format("milvus")` from the segment
  * manifests the write produced (capabilities W1's write half and R3).
  *
  * Cancels unless the bucket is named:
  * {{{
  *   MILVUS_JNI_S3_BUCKET=bucket
  *   MILVUS_JNI_S3_REGION=us-west-2            # optional
  *   MILVUS_UAT_WRITE_PREFIX=spark-uat-write   # optional, the fs.root_path used
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

  /** The collection schema the read needs, as the protobuf the snapshot would
    * carry. Field ids match `milvus.writer.fieldIds` below.
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
    val storage = storageOptions()
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("connector-write-read-uat")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      val df = spark
        .range(rows)
        .select(
          col("id"),
          concat(lit("row-"), col("id")).as("name"),
          array((0 until dim).map(d => (col("id") * 10 + d).cast("float")): _*)
            .as("v")
        )
        .repartition(2)

      // --- write: two tasks, two V3 segments under {root}/staging/{job}/ ---
      val writeOptions = storage ++ Map(
        MilvusOption.MilvusCollectionName -> "connector_rt",
        MilvusOption.WriterFieldIds -> "id:100,name:101,v:102",
        MilvusOption.vectorDimKey("v") -> dim.toString,
        MilvusOption.MilvusInsertMaxBatchSize -> "1000"
      )
      val basePaths = MilvusV3Writer.writeDataFrame(df, writeOptions).get
      info(s"wrote ${basePaths.size} segment(s): ${basePaths.mkString(", ")}")
      basePaths.size shouldBe 2

      // --- commit: the job manifest and the marker sit next to the segments ---
      val jobPrefix = "(.*/staging/[^/]+)/".r
        .findFirstMatchIn(basePaths.head)
        .map(_.group(1))
        .getOrElse(fail(s"no staging prefix in ${basePaths.head}"))
      val store = HadoopStorageKeys.storeFrom(storage)
      val manifest =
        try {
          store.exists(s"$jobPrefix/_committed") shouldBe true
          JobManifest
            .fromJson(
              new String(
                store.readAll(s"$jobPrefix/manifest.json"),
                StandardCharsets.UTF_8
              )
            )
            .fold(e => throw e, identity)
        } finally store.close()
      info(
        s"job manifest: ${manifest.jobId}, ${manifest.segments.size} segments, ${manifest.rowCount} rows"
      )
      manifest.segments.map(_.basePath).sorted shouldBe basePaths.sorted
      manifest.rowCount shouldBe rows.toLong
      manifest.segments.foreach(_.manifestVersion shouldBe 1L)

      // --- read: the manifests the write produced, no service in between ---
      val manifests = SegmentListJson.encodeManifestItems(
        basePaths.zipWithIndex.map { case (path, i) =>
          ManifestItemJson(i + 1L, s"""{"ver":-1,"base_path":"$path"}""")
        }
      )
      val readOptions = storage ++ Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotManifests -> manifests,
        MilvusOption.SnapshotSchemaBytes -> schemaBytes,
        MilvusOption.SnapshotCollectionId -> "1",
        MilvusOption.SnapshotPartitionIds -> "0",
        MilvusOption.MilvusCollectionName -> "connector_rt"
      )
      val back = spark.read.format("milvus").options(readOptions).load()
      info(s"schema: ${back.schema.treeString}")
      val got = back.collect().map(r => r.getLong(0) -> r).toMap
      got.size shouldBe rows
      (0L until rows.toLong).foreach { i =>
        val r = got(i)
        r.getString(1) shouldBe s"row-$i"
        r.getSeq[Float](2) shouldBe (0 until dim).map(d => (i * 10 + d).toFloat)
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
      val cleanup = HadoopStorageKeys.storeFrom(storage)
      try {
        val layout =
          StagingLayout(storage(StorageProperties.RootPath), manifest.jobId)
        layout.prefix shouldBe jobPrefix
        val deleted = new Committer(cleanup, layout).abort()
        info(s"abort deleted $deleted files under $jobPrefix")
        deleted should be >= 6 // two parquet, two manifests, manifest.json, _committed
        cleanup
          .list(jobPrefix, recursive = true)
          .filterNot(_.isDirectory) shouldBe empty
        cleanup.exists(s"$jobPrefix/_committed") shouldBe false
      } finally cleanup.close()
    } finally spark.stop()
  }
}
