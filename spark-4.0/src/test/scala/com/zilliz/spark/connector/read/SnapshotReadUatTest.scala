package com.zilliz.spark.connector.read

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.SnapshotCatalog
import com.zilliz.spark.connector.options.{
  MilvusOption,
  StorageOptions,
  V2SegmentResolvers
}

/** The whole read chain on a real snapshot in the UAT bucket: the snapshot JSON
  * is read through `SnapshotCatalog` on the driver, planned into partitions,
  * and every partition is read by the native reader on a local Spark executor.
  * No Milvus service is involved (capability R3).
  *
  * Cancels unless the environment names the snapshot:
  * {{{
  *   MILVUS_JNI_S3_BUCKET=bucket
  *   MILVUS_JNI_S3_REGION=us-west-2            # optional
  *   MILVUS_JNI_S3_ENDPOINT=s3.us-west-2.amazonaws.com   # optional
  *   MILVUS_UAT_SNAPSHOT_PATH=files/snapshots/<coll>/metadata/<id>.json
  *   MILVUS_UAT_EXPECTED_ROWS=12345           # optional
  *   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
  * }}}
  */
class SnapshotReadUatTest extends AnyFunSuite with Matchers {

  private def env(n: String): Option[String] =
    sys.env.get(n).map(_.trim).filter(_.nonEmpty)

  private def storageOptions(): Map[String, String] = {
    val bucket = env("MILVUS_JNI_S3_BUCKET").getOrElse(
      cancel(
        "set MILVUS_JNI_S3_BUCKET, MILVUS_UAT_SNAPSHOT_PATH and AWS_* to reach the UAT bucket"
      )
    )
    val region = env("MILVUS_JNI_S3_REGION").getOrElse("us-west-2")
    val endpoint =
      env("MILVUS_JNI_S3_ENDPOINT").getOrElse(s"s3.$region.amazonaws.com")
    Map(
      StorageProperties.BucketName -> bucket,
      StorageProperties.Address -> endpoint,
      StorageProperties.Region -> region,
      StorageProperties.CloudProvider -> "aws",
      StorageProperties.UseSSL -> "true",
      StorageProperties.UseIam -> "true"
    )
  }

  private def snapshotPath(): String =
    env("MILVUS_UAT_SNAPSHOT_PATH").getOrElse(
      cancel(
        "set MILVUS_UAT_SNAPSHOT_PATH to a snapshot JSON in the UAT bucket"
      )
    )

  /** Not part of the read. The other cases need a collection with flushed
    * segments and a snapshot of it; this makes both through `client.api`
    * (capabilities C2 and A1) when `MILVUS_UAT_URI` is set, and prints the
    * snapshot location for `MILVUS_UAT_SNAPSHOT_PATH`.
    */
  test(
    "prepare: a collection with flushed rows and a snapshot of it (C2, A1)"
  ) {
    import io.milvus.grpc.schema._
    val uri = env("MILVUS_UAT_URI").getOrElse(
      cancel(
        "set MILVUS_UAT_URI (and MILVUS_UAT_TOKEN) to prepare a collection"
      )
    )
    val collection = env("MILVUS_UAT_COLLECTION").getOrElse(
      "spark_uat_" + System.currentTimeMillis()
    )
    val rows = env("MILVUS_UAT_ROWS").map(_.toInt).getOrElse(3000)
    val dim = 4
    val client = com.zilliz.milvus.client.api.MilvusClient(
      MilvusOption(
        Map(MilvusOption.MilvusUri -> uri) ++
          env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
      ).connectionParams
    )
    try {
      if (client.getCollectionInfo("", collection).isFailure) {
        val schema = client.createCollectionSchema(
          name = collection,
          fields = Seq(
            client.createCollectionField(
              "id",
              isPrimary = true,
              dataType = DataType.Int64
            ),
            client.createCollectionField(
              "name",
              dataType = DataType.VarChar,
              typeParams = Map("max_length" -> "64")
            ),
            client.createCollectionField(
              "v",
              dataType = DataType.FloatVector,
              typeParams = Map("dim" -> dim.toString)
            )
          )
        )
        client
          .createCollection(collectionName = collection, schema = schema)
          .get
        this.info(s"created collection $collection")
        val batch = 1000
        (0 until rows by batch).foreach { start =>
          val ids = (start until math.min(start + batch, rows)).map(_.toLong)
          val fields = Seq(
            FieldData(
              `type` = DataType.Int64,
              fieldName = "id",
              field = FieldData.Field.Scalars(
                ScalarField(data =
                  ScalarField.Data.LongData(LongArray(data = ids))
                )
              )
            ),
            FieldData(
              `type` = DataType.VarChar,
              fieldName = "name",
              field = FieldData.Field.Scalars(
                ScalarField(data =
                  ScalarField.Data.StringData(
                    StringArray(data = ids.map(i => s"row-$i"))
                  )
                )
              )
            ),
            FieldData(
              `type` = DataType.FloatVector,
              fieldName = "v",
              field = FieldData.Field.Vectors(
                VectorField(
                  dim = dim,
                  data = VectorField.Data.FloatVector(
                    FloatArray(data =
                      ids.flatMap(i =>
                        (0 until dim).map(d => (i * 10 + d).toFloat)
                      )
                    )
                  )
                )
              )
            )
          )
          client
            .insert(
              collectionName = collection,
              fieldsData = fields,
              numRows = ids.size
            )
            .get
        }
        client.flush(collectionNames = Seq(collection)).get
        this.info(s"inserted $rows rows and flushed")
      }
      // Flush is asynchronous on the service and GetPersistentSegmentInfo is a
      // denied API on Zilliz Cloud, so the wait is a fixed pause; whether the
      // snapshot then holds every row is what the read cases check.
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      val name = "spark_uat_" + System.currentTimeMillis()
      val snapshot = client
        .createSnapshotForRead(
          "",
          collection,
          name,
          "spark-milvus UAT read",
          86400L
        )
        .get
      this.info(s"created snapshot ${snapshot.name} at ${snapshot.s3Location}")
      this.info(
        s"export MILVUS_UAT_COLLECTION=$collection MILVUS_UAT_SNAPSHOT_PATH=${snapshot.s3Location}"
      )
    } finally client.close()
  }

  /** Deletes rows of the prepared collection and takes a new snapshot, so the
    * read cases can check that deleted rows stay out (capability R8). Needs
    * `MILVUS_UAT_URI`, `MILVUS_UAT_COLLECTION` and `MILVUS_UAT_DELETE_IDS`
    * (comma-separated primary keys).
    */
  test("prepare: delete rows and take a new snapshot (R8)") {
    val uri = env("MILVUS_UAT_URI").getOrElse(
      cancel("set MILVUS_UAT_URI (and MILVUS_UAT_TOKEN) to delete rows")
    )
    val collection = env("MILVUS_UAT_COLLECTION").getOrElse(
      cancel("set MILVUS_UAT_COLLECTION to the prepared collection")
    )
    val ids = env("MILVUS_UAT_DELETE_IDS")
      .map(_.split(",").map(_.trim.toInt).toSeq)
      .getOrElse(cancel("set MILVUS_UAT_DELETE_IDS to the ids to delete"))
    val client = com.zilliz.milvus.client.api.MilvusClient(
      MilvusOption(
        Map(MilvusOption.MilvusUri -> uri) ++
          env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
      ).connectionParams
    )
    try {
      client
        .delete[Int](
          collectionName = collection,
          pkName = Some("id"),
          pks = ids
        )
        .get
      client.flush(collectionNames = Seq(collection)).get
      this.info(s"deleted ${ids.size} rows of $collection and flushed")
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      val name = "spark_uat_del_" + System.currentTimeMillis()
      val snapshot = client
        .createSnapshotForRead(
          "",
          collection,
          name,
          "spark-milvus UAT delete read",
          86400L
        )
        .get
      this.info(s"created snapshot ${snapshot.name} at ${snapshot.s3Location}")
      this.info(
        s"export MILVUS_UAT_SNAPSHOT_PATH=${snapshot.s3Location} MILVUS_UAT_DELETED_IDS=${ids.mkString(",")}"
      )
    } finally client.close()
  }

  test("SnapshotCatalog reads the snapshot JSON through the native store") {
    val options = storageOptions()
    val path = snapshotPath()
    val bucket = options(StorageProperties.BucketName)
    val store = StorageOptions.storeFor(
      StorageOptions.buildHadoopConfForOptions(options, ""),
      bucket,
      options
    )
    val snapshot =
      new SnapshotCatalog(store, bucket, V2SegmentResolvers.footer(true))
        .read(path)
    info(
      s"snapshot ${snapshot.name}: collection ${snapshot.collectionId}, " +
        s"${snapshot.partitionIds.size} partitions, ${snapshot.segments.size} segments " +
        s"(${snapshot.v3Segments.size} V3, ${snapshot.v2Segments.size} V2, " +
        s"${snapshot.deleteOnlySegments.size} delete-only), " +
        s"${snapshot.schema.fields.size} fields"
    )
    snapshot.dataSegments should not be empty
    snapshot.primaryKeyField should not be empty
    snapshot.segments.foreach(seg =>
      info(
        s"segment ${seg.id} v${seg.storageVersion} rows=${seg.rows} deletes=${seg.deletes}"
      )
    )
  }

  private def withSpark(f: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("snapshot-read-uat")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try f(spark)
    finally spark.stop()
  }

  private def read(spark: SparkSession, extra: (String, String)*) = {
    var reader = spark.read
      .format("milvus")
      .option(MilvusOption.SnapshotPath, snapshotPath())
    storageOptions().foreach { case (k, v) => reader = reader.option(k, v) }
    extra.foreach { case (k, v) => reader = reader.option(k, v) }
    reader.load()
  }

  test("format(\"milvus\") with milvus.snapshot.path reads every row") {
    storageOptions(); snapshotPath()
    withSpark { spark =>
      val df = read(spark)
      info(s"schema: ${df.schema.treeString}")
      val rows = df.count()
      info(s"row reader delivered $rows rows")
      rows should be > 0L
      env("MILVUS_UAT_EXPECTED_ROWS").foreach(e => rows shouldBe e.toLong)
      val sample = df.limit(3).collect()
      sample.foreach(r => info(r.toString.take(200)))
      // Rows deleted before the snapshot was taken must not come back.
      env("MILVUS_UAT_DELETED_IDS").foreach { ids =>
        val deleted = ids.split(",").map(_.trim.toLong).toSeq
        val present =
          df.filter(org.apache.spark.sql.functions.col("id").isin(deleted: _*))
            .count()
        info(s"${deleted.size} deleted ids, $present of them present")
        present shouldBe 0L
      }
    }
  }

  test("the columnar reader delivers the same row count") {
    storageOptions(); snapshotPath()
    withSpark { spark =>
      val rowCount = read(spark).count()
      val columnar = read(spark, MilvusOption.ReadColumnar -> "true").count()
      info(s"row path $rowCount rows, columnar path $columnar rows")
      columnar shouldBe rowCount
    }
  }
}
