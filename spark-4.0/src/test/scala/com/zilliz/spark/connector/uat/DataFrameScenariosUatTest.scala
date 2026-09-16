package com.zilliz.spark.connector.uat

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  QueryStageExec
}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson
}
import com.zilliz.milvus.storage.write.commit.{Committer, JobManifest}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.options.{HadoopStorageKeys, MilvusOption}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** Scenarios written the way a user job is written: DataFrame operations
  * chained over the UAT collections, each run on both outlets (columnar, the
  * default, and the row path) with the results compared to values derived from
  * how the collections were built. Work item #18.
  *
  * Collections, all built by `SnapshotReadUatTest`'s prepare cases:
  *   - spark_uat_v3: ids 0 until 3000, name = "row-<id>", v = [id*10 + d]; one
  *     snapshot before the delete of ids 0..9, one after (folded into the
  *     segment manifest); backfill later added score = id * 2.
  *   - spark_uat_parts: the same rows in three partitions: _default 0..999, p1
  *     1000..1999 and 3000..3499 (two segments), p2 2000..2999.
  *   - spark_uat_types: 100 rows, 13 columns, every value derived from id.
  *
  * Environment:
  * {{{
  *   MILVUS_JNI_S3_BUCKET / MILVUS_JNI_S3_REGION / AWS_*   the bucket
  *   MILVUS_JNI_S3_ROOT_PATH                     instance root, client mode
  *   MILVUS_UAT_V3_SNAPSHOT                      spark_uat_v3 before deletes
  *   MILVUS_UAT_V3_DELETED_SNAPSHOT              after deletes (ids 0..9)
  *   MILVUS_UAT_TYPES_SNAPSHOT_PATH              spark_uat_types
  *   MILVUS_UAT_URI / MILVUS_UAT_TOKEN           client mode, S5 and S11
  *   MILVUS_UAT_PARTS_COLLECTION                 S5
  *   MILVUS_UAT_BACKFILL_COLLECTION              S11 (takes its own snapshot)
  *   MILVUS_UAT_WRITE_PREFIX                     S10 staging, default
  *                                               spark-uat-write
  * }}}
  * Scenarios whose variables are missing cancel; nothing else is touched.
  */
class DataFrameScenariosUatTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private def env(n: String): Option[String] =
    sys.env.get(n).map(_.trim).filter(_.nonEmpty)

  private def need(n: String): String =
    env(n).getOrElse(cancel(s"set $n"))

  private val dim = 4
  private val rows = 3000L
  private val deletedIds: Set[Long] = (0L until 10L).toSet

  private def storageOptions(): Map[String, String] = {
    val bucket = need("MILVUS_JNI_S3_BUCKET")
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
    ) ++ env("MILVUS_JNI_S3_ROOT_PATH").map(StorageProperties.RootPath -> _)
  }

  private var sparkSession: SparkSession = null

  private def spark: SparkSession = {
    if (sparkSession == null) {
      sparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("dataframe-scenarios-uat")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    }
    sparkSession
  }

  override protected def afterAll(): Unit = {
    if (sparkSession != null) sparkSession.stop()
  }

  /** A snapshot read by path, on the given outlet. */
  private def snapshot(
      path: String,
      columnar: Boolean,
      extra: (String, String)*
  ): DataFrame = {
    var reader = spark.read
      .format("milvus")
      .option(MilvusOption.SnapshotPath, path)
      .option(MilvusOption.ReadColumnar, columnar.toString)
    storageOptions().foreach { case (k, v) => reader = reader.option(k, v) }
    extra.foreach { case (k, v) => reader = reader.option(k, v) }
    reader.load()
  }

  /** Runs `body` once per outlet, so every scenario is checked on both. */
  private def bothOutlets(body: Boolean => Unit): Unit =
    Seq(true, false).foreach { columnar =>
      withClue(s"columnar=$columnar: ")(body(columnar))
    }

  private def v3(columnar: Boolean, extra: (String, String)*): DataFrame =
    snapshot(need("MILVUS_UAT_V3_SNAPSHOT"), columnar, extra: _*)

  private def v3Deleted(columnar: Boolean): DataFrame =
    snapshot(need("MILVUS_UAT_V3_DELETED_SNAPSHOT"), columnar)

  private def types(columnar: Boolean, extra: (String, String)*): DataFrame =
    snapshot(need("MILVUS_UAT_TYPES_SNAPSHOT_PATH"), columnar, extra: _*)

  private def expectedVector(id: Long): Seq[Float] =
    (0 until dim).map(d => (id * 10 + d).toFloat)

  // ---------------------------------------------------------------- S1

  test(
    "S1 projection: select, selectExpr, rename, drop, vector only, pk only"
  ) {
    bothOutlets { columnar =>
      val df = v3(columnar)
      df.columns.toSeq shouldBe Seq("id", "name", "v")

      val renamed = df
        .select(col("id").as("pk"), col("name"))
        .selectExpr("pk", "upper(name) as NAME")
        .collect()
      renamed.length shouldBe rows
      renamed.foreach { r =>
        r.getString(1) shouldBe s"ROW-${r.getLong(0)}"
      }

      val dropped = df.drop("v").collect()
      dropped.head.schema.fieldNames.toSeq shouldBe Seq("id", "name")
      dropped.length shouldBe rows

      val vectorsOnly = df.select("v").collect()
      vectorsOnly.length shouldBe rows
      vectorsOnly.foreach(r => r.getSeq[Float](0).length shouldBe dim)

      df.select("id").agg(sum("id")).collect().head.getLong(0) shouldBe
        (0L until rows).sum
    }
  }

  // ---------------------------------------------------------------- S2

  test("S2 filters: comparison, isin, isNull, startsWith, and/or/not, limit") {
    bothOutlets { columnar =>
      val df = v3(columnar)
      df.filter(col("id") >= 100 && col("id") < 200).count() shouldBe 100L
      df.filter(col("id").isin(1L, 2L, 3L, 5000L)).count() shouldBe 3L
      df.filter(col("name").isNull).count() shouldBe 0L
      df.filter(col("name").isNotNull).count() shouldBe rows
      df.filter(col("name").startsWith("row-29")).count() shouldBe 111L
      df.filter(col("name").endsWith("0")).count() shouldBe 300L
      df.filter(!(col("id") < 10)).count() shouldBe rows - 10
      df.filter(col("id") === 7 || col("id") === 8).count() shouldBe 2L
      df.filter(col("id") > 2990 && col("name") =!= "row-2995").count() shouldBe
        8L

      val limited = df.filter(col("id") >= 1000).limit(50).collect()
      limited.length shouldBe 50
      limited.foreach(r => r.getLong(0) should be >= 1000L)

      val ordered = df.filter(col("id") < 100).orderBy(col("id").desc).limit(3)
      ordered.collect().map(_.getLong(0)).toSeq shouldBe Seq(99L, 98L, 97L)
    }
  }

  // ---------------------------------------------------------------- S3

  test("S3 aggregation: agg, groupBy on metadata columns, distinct, orderBy") {
    bothOutlets { columnar =>
      val df = v3(columnar)
      val stats = df
        .agg(
          count("*").as("n"),
          sum("id").as("s"),
          min("id").as("lo"),
          max("id").as("hi"),
          avg("id").as("mean")
        )
        .collect()
        .head
      stats.getLong(0) shouldBe rows
      stats.getLong(1) shouldBe (0L until rows).sum
      stats.getLong(2) shouldBe 0L
      stats.getLong(3) shouldBe rows - 1
      stats.getDouble(4) shouldBe ((rows - 1).toDouble / 2) +- 1e-9

      val withMeta = v3(
        columnar,
        MilvusOption.MilvusExtraColumns -> "_segment_id,_row_offset,_timestamp"
      )
      val bySegment = withMeta
        .groupBy(col("_segment_id"))
        .agg(
          count("*").as("n"),
          max("_row_offset").as("hi"),
          min("_timestamp").as("ts")
        )
        .collect()
      bySegment.map(_.getLong(1)).sum shouldBe rows
      // Row offsets are positions in the segment; with no deletes they run
      // from 0 to n - 1 in every segment. Timestamps are Milvus's, positive.
      bySegment.foreach { r =>
        r.getLong(2) shouldBe r.getLong(1) - 1
        r.getLong(3) should be > 0L
      }

      df.select((col("id") % 10).as("d")).distinct().count() shouldBe 10L
      val top = df.orderBy(col("id").desc).limit(2).collect()
      top.map(_.getLong(0)).toSeq shouldBe Seq(2999L, 2998L)
    }
  }

  // ---------------------------------------------------------------- S4

  test(
    "S4 two snapshots of one collection: join, except finds the deleted ids"
  ) {
    bothOutlets { columnar =>
      val before = v3(columnar)
      val after = v3Deleted(columnar)
      before.count() shouldBe rows
      after.count() shouldBe rows - deletedIds.size

      val joined = before
        .select(col("id"), col("name").as("before_name"))
        .join(after.select(col("id"), col("name").as("after_name")), "id")
      joined.count() shouldBe rows - deletedIds.size
      joined.filter(col("before_name") =!= col("after_name")).count() shouldBe
        0L

      val gone = before.select("id").except(after.select("id")).collect()
      gone.map(_.getLong(0)).toSet shouldBe deletedIds

      after.filter(col("id").isin(deletedIds.toSeq: _*)).count() shouldBe 0L
    }
  }

  // ---------------------------------------------------------------- S5

  test("S5 client mode: partition and segment selection, union, except") {
    val uri = need("MILVUS_UAT_URI")
    val collection = need("MILVUS_UAT_PARTS_COLLECTION")
    need("MILVUS_JNI_S3_ROOT_PATH")
    val base = storageOptions() ++ Map(
      MilvusOption.MilvusUri -> uri,
      MilvusOption.MilvusCollectionName -> collection
    ) ++ env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)

    // Selectors take numeric ids; names are the client's to resolve.
    val client = com.zilliz.milvus.client.api.MilvusClient(
      MilvusOption(
        Map(MilvusOption.MilvusUri -> uri) ++
          env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
      ).connectionParams
    )
    val partitionIds: Map[String, Long] =
      try
        Seq("_default", "p1", "p2")
          .map(n => n -> client.getPartitionID("", collection, n).get)
          .toMap
      finally client.close()

    bothOutlets { columnar =>
      def read(extra: (String, String)*): DataFrame =
        spark.read
          .format("milvus")
          .options(
            base ++ extra + (MilvusOption.ReadColumnar -> columnar.toString)
          )
          .load()
      def partition(name: String): DataFrame =
        read(MilvusOption.MilvusPartitions -> partitionIds(name).toString)

      val all = read()
      all.count() shouldBe 3500L

      val default = partition("_default")
      val p1 = partition("p1")
      val p2 = partition("p2")
      default.count() shouldBe 1000L
      p1.count() shouldBe 1500L
      p2.count() shouldBe 1000L
      p1.select("id").collect().map(_.getLong(0)).toSet shouldBe
        ((1000L until 2000L) ++ (3000L until 3500L)).toSet

      val together = default.union(p1).union(p2)
      together.count() shouldBe 3500L
      all.select("id").except(together.select("id")).count() shouldBe 0L
      together.select("id").except(all.select("id")).count() shouldBe 0L

      // p1 spans two segments; row offsets restart at 0 in each.
      val p1Meta = read(
        MilvusOption.MilvusPartitions -> partitionIds("p1").toString,
        MilvusOption.MilvusExtraColumns -> "_segment_id,_row_offset"
      )
      val segments = p1Meta
        .groupBy(col("_segment_id"))
        .agg(count("*").as("n"), min("_row_offset"), max("_row_offset"))
        .collect()
      segments.length shouldBe 2
      segments.foreach { r =>
        r.getLong(2) shouldBe 0L
        r.getLong(3) shouldBe r.getLong(1) - 1
      }
      val one = segments.head.getLong(0)
      read(MilvusOption.MilvusSegments -> one.toString).count() shouldBe
        segments.head.getLong(1)
      // Both selectors together read their intersection.
      read(
        MilvusOption.MilvusPartitions -> partitionIds("p1").toString,
        MilvusOption.MilvusSegments -> one.toString
      ).count() shouldBe segments.head.getLong(1)
      // A segment outside the selected partition is refused, not emptied:
      // every requested id has to exist in what the scan reads.
      val outside = intercept[IllegalArgumentException](
        read(
          MilvusOption.MilvusPartitions -> partitionIds("p2").toString,
          MilvusOption.MilvusSegments -> one.toString
        ).count()
      )
      outside.getMessage should include("not found in partition")
    }
  }

  // ---------------------------------------------------------------- S6

  test(
    "S6 one type-specific operation per column of the all-types collection"
  ) {
    bothOutlets { columnar =>
      val df = types(columnar)
      df.count() shouldBe 100L

      val exploded = df.select(col("id"), explode(col("arr")).as("e"))
      exploded.count() shouldBe 300L
      exploded
        .groupBy("id")
        .agg(sum("e").as("s"))
        .collect()
        .foreach(r => r.getLong(1) shouldBe 3 * r.getLong(0) + 3)

      df.select(col("id"), get_json_object(col("j"), "$.k").as("k"))
        .collect()
        .foreach(r => r.getString(1) shouldBe r.getLong(0).toString)
      df.select(get_json_object(col("j"), "$.tag").as("tag"))
        .filter(col("tag") === "t7")
        .count() shouldBe 1L

      df.select(functions.size(col("v")))
        .distinct()
        .collect()
        .map(_.getInt(0))
        .toSeq shouldBe Seq(dim)
      df.select(col("id"), element_at(col("v"), 1).as("v0"))
        .collect()
        .foreach(r => r.getFloat(1) shouldBe (r.getLong(0) * 10).toFloat)

      // bv holds id in two little-endian bytes; hex() reads them as stored.
      df.select(col("id"), hex(col("bv")).as("h"))
        .collect()
        .foreach { r =>
          val i = r.getLong(0)
          r.getString(1) shouldBe f"${i & 0xff}%02X${(i >> 8) & 0xff}%02X"
        }

      df.select(coalesce(col("opt"), lit(-1)).as("o"), col("id"))
        .collect()
        .foreach { r =>
          if (r.getLong(1) % 3 == 0) r.getInt(0) shouldBe -1
          else r.getInt(0).toLong shouldBe r.getLong(1)
        }
      df.filter(col("opt").isNull).count() shouldBe 34L

      df.filter(col("b")).count() shouldBe 50L
      df.agg(sum("i16"), sum("i32"), sum("f"), sum("d")).collect().head match {
        case r =>
          r.getLong(0) shouldBe (0L until 100L).map(_ * 100).sum
          r.getLong(1) shouldBe (0L until 100L).map(_ * 1000).sum
          r.getDouble(2) shouldBe (0 until 100).map(_ * 0.5).sum +- 1e-6
          r.getDouble(3) shouldBe (0 until 100).map(_ * 0.25).sum +- 1e-9
      }
      df.filter(col("s") === "row-42")
        .select("i8")
        .collect()
        .head
        .getByte(0) shouldBe
        42.toByte
    }
  }

  // ---------------------------------------------------------------- S7

  test(
    "S7 vector.raw bytes decode to the same vectors the default read gives"
  ) {
    bothOutlets { columnar =>
      val decoded = types(columnar)
        .select("id", "v", "bv")
        .collect()
        .map(r => r.getLong(0) -> r)
        .toMap
      val raw = types(columnar, MilvusOption.ReadVectorRaw -> "true")
        .select("id", "v", "bv")
        .collect()
        .map(r => r.getLong(0) -> r)
        .toMap
      raw.size shouldBe 100
      raw.foreach { case (id, r) =>
        val bytes = r.getAs[Array[Byte]](1)
        bytes.length shouldBe dim * 4
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        val floats = (0 until dim).map(_ => buffer.getFloat())
        floats shouldBe decoded(id).getSeq[Float](1)
        floats shouldBe expectedVector(id)
        r.getAs[Array[Byte]](2).toSeq shouldBe decoded(id)
          .getAs[Array[Byte]](2)
          .toSeq
      }
    }
  }

  // ---------------------------------------------------------------- S8

  test("S8 SQL over a temp view gives what the DataFrame calls give") {
    bothOutlets { columnar =>
      val view = s"uat_v3_${if (columnar) "col" else "row"}"
      v3(columnar).createOrReplaceTempView(view)
      spark
        .sql(s"select count(*) from $view where id >= 100 and id < 200")
        .collect()
        .head
        .getLong(0) shouldBe 100L
      spark
        .sql(s"select name from $view where id = 5")
        .collect()
        .head
        .getString(0) shouldBe "row-5"
      val grouped = spark
        .sql(
          s"select id % 3 as g, count(*) as n, max(id) as hi from $view group by id % 3 order by g"
        )
        .collect()
      grouped.map(_.getLong(1)).toSeq shouldBe Seq(1000L, 1000L, 1000L)
      grouped.map(_.getLong(2)).toSeq shouldBe Seq(2997L, 2998L, 2999L)
      spark
        .sql(s"select size(v) as d from $view where id = 0")
        .collect()
        .head
        .getInt(0) shouldBe dim
      spark.catalog.dropTempView(view)
    }
  }

  // ---------------------------------------------------------------- S9

  test("S9 one DataFrame, several actions, cache") {
    bothOutlets { columnar =>
      val df = v3(columnar)
      df.count() shouldBe rows
      df.collect().length.toLong shouldBe rows
      df.filter(col("id") < 10).count() shouldBe 10L
      df.cache()
      try {
        df.count() shouldBe rows
        df.filter(col("id") >= 2990).count() shouldBe 10L
        df.select("name").filter(col("name") === "row-1").count() shouldBe 1L
      } finally df.unpersist()
      df.count() shouldBe rows
    }
  }

  // ---------------------------------------------------------------- S10

  /** The schema the write table takes: the same three fields the collection
    * has, with the field ids Milvus gave them.
    */
  private val writeSchemaBytes: String = Base64.getEncoder.encodeToString(
    CollectionSchema(
      name = "uat_scenarios_rt",
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

  test("S10 read, transform, write back to staging, read the segments, abort") {
    val storage = storageOptions() + (StorageProperties.RootPath -> env(
      "MILVUS_UAT_WRITE_PREFIX"
    ).getOrElse("spark-uat-write"))
    val tableOptions = storage ++ Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotSchemaBytes -> writeSchemaBytes,
      MilvusOption.SnapshotCollectionId -> "1",
      MilvusOption.SnapshotPartitionIds -> "0",
      MilvusOption.MilvusCollectionName -> "uat_scenarios_rt",
      MilvusOption.MilvusInsertMaxBatchSize -> "1000"
    )
    val root = storage(StorageProperties.RootPath)

    // Other suites (the backfill) commit jobs under the same prefix, so the
    // scenario looks for the job its own write added.
    def committedJobs(): Set[StagingLayout] = {
      val store = HadoopStorageKeys.storeFrom(storage)
      try
        store
          .list(s"$root/staging", recursive = false)
          .filter(_.isDirectory)
          .map(d =>
            StagingLayout(root, d.path.stripSuffix("/").split("/").last)
          )
          .filter(l => store.exists(l.marker))
          .toSet
      finally store.close()
    }

    bothOutlets { columnar =>
      val written = 500L
      val df = v3(columnar)
        .filter(col("id") < written)
        .withColumn("name", concat(col("name"), lit("-x")))
        .repartition(2)
      val before = committedJobs()
      df.write.format("milvus").mode("append").options(tableOptions).save()

      val store = HadoopStorageKeys.storeFrom(storage)
      try {
        val committed = (committedJobs() -- before).toSeq
        withClue(s"one new committed job expected under $root/staging: ")(
          committed.size shouldBe 1
        )
        val layout = committed.head
        val manifest = JobManifest
          .fromJson(
            new String(store.readAll(layout.manifest), StandardCharsets.UTF_8)
          )
          .fold(e => throw e, identity)
        manifest.rowCount shouldBe written
        manifest.segments.size shouldBe 2

        val manifests = SegmentListJson.encodeManifestItems(
          manifest.segments.map(_.basePath).zipWithIndex.map { case (path, i) =>
            ManifestItemJson(i + 1L, s"""{"ver":-1,"base_path":"$path"}""")
          }
        )
        val back = spark.read
          .format("milvus")
          .options(
            tableOptions + (MilvusOption.SnapshotManifests -> manifests) +
              (MilvusOption.ReadColumnar -> columnar.toString)
          )
          .load()
          .collect()
          .map(r => r.getLong(0) -> r)
          .toMap
        back.size.toLong shouldBe written
        (0L until written).foreach { i =>
          back(i).getString(1) shouldBe s"row-$i-x"
          back(i).getSeq[Float](2) shouldBe expectedVector(i)
        }

        val deleted = new Committer(store, layout).abort()
        info(s"columnar=$columnar: abort deleted $deleted files")
        store
          .list(layout.prefix, recursive = true)
          .filterNot(_.isDirectory) shouldBe empty
      } finally store.close()
    }
  }

  // ---------------------------------------------------------------- S11

  /** A snapshot pins each segment's manifest version, and the backfill
    * registered a newer version than any snapshot taken before it, so the
    * scenario takes a fresh snapshot through the client, reads it by path, and
    * drops it afterwards.
    */
  test("S11 the column backfill wrote reads back and filters") {
    val uri = need("MILVUS_UAT_URI")
    val collection = need("MILVUS_UAT_BACKFILL_COLLECTION")
    val client = com.zilliz.milvus.client.api.MilvusClient(
      MilvusOption(
        Map(MilvusOption.MilvusUri -> uri) ++
          env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
      ).connectionParams
    )
    val name = "spark_uat_scenarios_" + System.currentTimeMillis()
    try {
      val taken = client
        .createSnapshotForRead(
          "",
          collection,
          name,
          "spark-milvus UAT scenarios S11",
          3600L
        )
        .get
      info(s"snapshot $name at ${taken.s3Location}")
      bothOutlets { columnar =>
        val df = snapshot(taken.s3Location, columnar)
        df.columns.toSeq shouldBe Seq("id", "name", "v", "score")
        val alive = rows - deletedIds.size
        df.count() shouldBe alive
        df.filter(col("score") > 5000).count() shouldBe 499L
        df.filter(col("score") =!= col("id") * 2).count() shouldBe 0L
        df.filter(col("score").isNull).count() shouldBe 0L
        df.agg(sum("score")).collect().head.getLong(0) shouldBe
          (10L until rows).map(_ * 2).sum
        df.select("id", "score")
          .orderBy(col("score").desc)
          .limit(1)
          .collect()
          .head
          .getLong(0) shouldBe rows - 1
      }
    } finally {
      client.dropSnapshot("", collection, name).failed.foreach { e =>
        info(s"dropping snapshot $name failed: ${e.getMessage}")
      }
      client.close()
    }
  }

  // ---------------------------------------------------------------- S12

  /** The scan node's metrics after an action. Adaptive execution hides each
    * finished stage's subtree behind a QueryStageExec, so the walk opens both.
    */
  private def scanMetrics(df: DataFrame): Map[String, Long] = {
    def scansIn(plan: SparkPlan): Seq[BatchScanExec] = plan match {
      case adaptive: AdaptiveSparkPlanExec => scansIn(adaptive.executedPlan)
      case stage: QueryStageExec           => scansIn(stage.plan)
      case scan: BatchScanExec             => Seq(scan)
      case other                           => other.children.flatMap(scansIn)
    }
    val scans = scansIn(df.queryExecution.executedPlan)
    scans should have size 1
    scans.head.metrics.collect {
      case (name, metric) if name.startsWith("milvus.") => name -> metric.value
    }
  }

  test(
    "S12 metrics: the row path materializes every delivered row, columnar none"
  ) {
    bothOutlets { columnar =>
      val counted = v3Deleted(columnar).groupBy().count()
      counted.collect().head.getLong(0) shouldBe rows - deletedIds.size
      val metrics = scanMetrics(counted)
      info(s"columnar=$columnar ${metrics.toSeq.sorted.mkString(", ")}")
      metrics("milvus.jni.calls") should be > 0L
      metrics("milvus.arrow.batches") should be > 0L
      metrics("milvus.arrow.bytes") should be > 0L
      metrics("milvus.rows.materialized") shouldBe
        (if (columnar) 0L else rows - deletedIds.size)

      val filtered =
        v3Deleted(columnar).filter(col("id") < 100).groupBy().count()
      filtered.collect().head.getLong(0) shouldBe 90L
      // The core evaluator excludes rows while they are still in Arrow. The
      // row outlet materializes only the 90 rows that survive both predicate
      // and deletes; the columnar outlet materializes no InternalRow objects.
      scanMetrics(filtered)("milvus.rows.materialized") shouldBe
        (if (columnar) 0L else 90L)
    }
  }
}
