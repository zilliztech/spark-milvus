package com.zilliz.spark.connector.uat

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  LongType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.read.MilvusSearch

/** Vector search against the UAT collections: the query-set entry, both
  * delivery paths, both modes, and the numbers it reports.
  *
  * The collections are the ones `SnapshotReadUatTest` prepares. `spark_uat_v3`
  * holds ids 0 until 3000 with `v = [id*10 + d]`, so the nearest neighbour of
  * `v(target)` is `target` itself and the next two are `target ± 1`, both at
  * squared L2 400. Ids 0..9 are deleted in the deleted snapshot.
  *
  * Environment:
  * {{{
  *   MILVUS_JNI_S3_BUCKET / MILVUS_JNI_S3_REGION / AWS_*   the bucket
  *   MILVUS_JNI_S3_ROOT_PATH                     instance root
  *   MILVUS_UAT_V3_SNAPSHOT                      spark_uat_v3 before deletes
  *   MILVUS_UAT_V3_DELETED_SNAPSHOT              after deletes (ids 0..9)
  *   MILVUS_UAT_INDEXED_SNAPSHOT                 a snapshot whose vector field
  *                                               carries a persisted index
  * }}}
  * A case whose variables are missing cancels. Knowhere's native package is
  * Linux only, so this suite runs where that package exists.
  */
class VectorSearchUatTest
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

  private var sparkSession: SparkSession = null

  private def spark: SparkSession = {
    if (sparkSession == null) {
      sparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("vector-search-uat")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    }
    sparkSession
  }

  override protected def afterAll(): Unit =
    if (sparkSession != null) sparkSession.stop()

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

  private def options(
      snapshot: String,
      extra: (String, String)*
  ): Map[String, String] =
    storageOptions() ++ Map(MilvusOption.SnapshotPath -> snapshot) ++ extra

  private def vectorOf(id: Long): Array[Float] =
    Array.tabulate(dim)(d => (id * 10 + d).toFloat)

  /** A query set as the entry takes it: `query_id` and `vector`. */
  private def queries(ids: Seq[Long]): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("query_id", LongType, nullable = false),
        StructField("vector", ArrayType(FloatType, false), nullable = false)
      )
    )
    spark.createDataFrame(
      ids.map(id => Row(id, vectorOf(id))).asJava,
      schema
    )
  }

  private def hits(frame: DataFrame): Seq[Row] =
    frame.orderBy("query_id", "rank").collect().toSeq

  private def idsOf(rows: Seq[Row], query: Long): Seq[Long] =
    rows.filter(_.getAs[Long]("query_id") == query).map(_.getAs[Long]("id"))

  /** The `milvus.search.*` accumulator values one search produced, as the stage
    * page shows them.
    */
  private def metricsOf(body: => Unit): Map[String, Long] = {
    val counted = mutable.Map.empty[String, Long]
    val listener = new SparkListener {
      override def onTaskEnd(end: SparkListenerTaskEnd): Unit =
        end.taskInfo.accumulables
          .filter(_.name.exists(_.startsWith("milvus.search.")))
          .foreach { value =>
            val name = value.name.get
            val update = value.update.map(_.toString.toLong).getOrElse(0L)
            counted(name) = counted.getOrElse(name, 0L) + update
          }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      body
      val deadline = System.nanoTime() + 30L * 1000L * 1000L * 1000L
      while (
        System.nanoTime() < deadline &&
        !counted.contains(SearchMetrics.Candidates)
      ) Thread.sleep(50L)
    } finally spark.sparkContext.removeSparkListener(listener)
    counted.toMap
  }

  // -------------------------------------------------------------- prepare

  /** Builds the collection the index cases need: rows like `spark_uat_v3`'s, an
    * HNSW index on the vector field, and a snapshot taken after the index is
    * built. Prints the snapshot location for `MILVUS_UAT_INDEXED_SNAPSHOT`.
    * Needs `MILVUS_UAT_URI` and `MILVUS_UAT_TOKEN`.
    */
  test("prepare: a collection whose vector field carries an HNSW index") {
    import io.milvus.grpc.schema._
    val uri = need("MILVUS_UAT_URI")
    val collection =
      env("MILVUS_UAT_VECTOR_COLLECTION").getOrElse("spark_uat_vector")
    val count = env("MILVUS_UAT_VECTOR_ROWS").map(_.toInt).getOrElse(3000)
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
        info(s"created collection $collection")
        val batch = 1000
        (0 until count by batch).foreach { start =>
          val ids = (start until math.min(start + batch, count)).map(_.toLong)
          client
            .insert(
              collectionName = collection,
              fieldsData = Seq(
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
                        FloatArray(data = ids.flatMap(i => vectorOf(i).toSeq))
                      )
                    )
                  )
                )
              ),
              numRows = ids.size
            )
            .get
        }
        client.flush(collectionNames = Seq(collection)).get
        info(s"inserted $count rows and flushed")
      }
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      val parameters = Map(
        "index_type" -> env("MILVUS_UAT_INDEX_TYPE").getOrElse("HNSW"),
        "metric_type" -> "L2",
        "M" -> "16",
        "efConstruction" -> "200"
      )
      client.createIndex("", collection, "v", parameters) match {
        case scala.util.Success(_) => info(s"index requested: $parameters")
        case scala.util.Failure(failure) =>
          info(s"createIndex refused: ${failure.getMessage}")
      }
      // The build is asynchronous; the snapshot must be taken after it, or it
      // records no index files for the segment.
      val deadline = System.currentTimeMillis() + 300000L
      var built = false
      while (!built && System.currentTimeMillis() < deadline) {
        val described = client.describeIndexes("", collection, "v")
        described.foreach { indexes =>
          indexes.foreach(index =>
            info(
              s"index ${index.indexName}: state=${index.state}, indexed=${index.indexedRows}/${index.totalRows}, params=${index.params}"
            )
          )
          built = indexes.nonEmpty && indexes.forall(index =>
            index.indexedRows >= count.toLong && index.state.isFinished
          )
        }
        if (!built) Thread.sleep(5000L)
      }
      built shouldBe true
      val name = "spark_uat_vector_" + System.currentTimeMillis()
      val snapshot = client
        .createSnapshotForRead(
          "",
          collection,
          name,
          "spark-milvus UAT vector search",
          86400L
        )
        .get
      info(s"created snapshot ${snapshot.name} at ${snapshot.s3Location}")
      info(
        s"export MILVUS_UAT_VECTOR_COLLECTION=$collection MILVUS_UAT_INDEXED_SNAPSHOT=${snapshot.s3Location}"
      )
    } finally client.close()
  }

  // ---------------------------------------------------------------- exact

  test("exact search answers every query with its own nearest rows") {
    val targets = Seq(11L, 100L, 2999L)
    val found = hits(
      MilvusSearch.search(
        spark,
        options(need("MILVUS_UAT_V3_SNAPSHOT")),
        queries(targets),
        "v",
        3,
        "L2",
        mode = "exact",
        outputColumns = Seq("id", "name")
      )
    )

    found.map(_.getAs[Long]("query_id")).distinct shouldBe targets
    targets.foreach { target =>
      val rows = found.filter(_.getAs[Long]("query_id") == target)
      rows.map(_.getAs[Int]("rank")) shouldBe Seq(1, 2, 3)
      rows.head.getAs[Long]("id") shouldBe target
      rows.head.getAs[Double]("_score") shouldBe 0.0
      rows.head.getAs[String]("name") shouldBe s"row-$target"
      // The second and third are target ± 1, both at squared L2 400; which of
      // them ranks second is decided by segment id and row offset.
      rows.tail
        .map(_.getAs[Long]("id"))
        .toSet
        .subsetOf(
          Set(target - 1, target + 1)
        ) shouldBe true
      rows.tail.foreach(_.getAs[Double]("_score") shouldBe 400.0)
    }
  }

  test("a query set of a thousand queries keeps every query's own answer") {
    val targets = (0L until 1000L).map(_ * 3 + 1)
    val found = hits(
      MilvusSearch.search(
        spark,
        options(need("MILVUS_UAT_V3_SNAPSHOT")),
        queries(targets),
        "v",
        1,
        "L2",
        mode = "exact",
        outputColumns = Seq("id")
      )
    )

    found.size shouldBe targets.size
    found.foreach { row =>
      row.getAs[Long]("id") shouldBe row.getAs[Long]("query_id")
      row.getAs[Int]("rank") shouldBe 1
    }
  }

  test("the two delivery paths and any number of query groups agree") {
    val targets = Seq(7L, 250L, 1500L)
    val snapshot = need("MILVUS_UAT_V3_SNAPSHOT")
    def search(extra: (String, String)*): Seq[Row] = hits(
      MilvusSearch.search(
        spark,
        options(snapshot, extra: _*),
        queries(targets),
        "v",
        2,
        "L2",
        mode = "exact",
        outputColumns = Seq("id")
      )
    )

    var broadcast: Seq[Row] = Seq.empty
    val single = metricsOf { broadcast = search() }
    // One query is 16 bytes of vector, so 8 bytes sends the set with the
    // shuffle instead, and a group limit of one query's worth cuts it into
    // three groups.
    var delivered: Seq[Row] = Seq.empty
    metricsOf {
      delivered = search(MilvusOption.SearchQueriesMaxBytes -> "8")
    }
    var grouped: Seq[Row] = Seq.empty
    val many = metricsOf {
      grouped = search(MilvusOption.SearchGroupMaxBytes -> "72")
    }
    var both: Seq[Row] = Seq.empty
    metricsOf {
      both = search(
        MilvusOption.SearchQueriesMaxBytes -> "8",
        MilvusOption.SearchGroupMaxBytes -> "72"
      )
    }

    broadcast.map(_.getAs[Long]("id")) should have size 6
    delivered.map(_.getAs[Long]("id")) shouldBe broadcast.map(
      _.getAs[Long]("id")
    )
    grouped.map(_.getAs[Long]("id")) shouldBe broadcast.map(_.getAs[Long]("id"))
    both.map(_.getAs[Long]("id")) shouldBe broadcast.map(_.getAs[Long]("id"))

    // Three query groups run on the segments a task already read, so the
    // segments are opened as many times as with one group, and Knowhere is
    // called once per group instead.
    many(SearchMetrics.Segments) shouldBe single(SearchMetrics.Segments)
    many(SearchMetrics.KnowhereCalls) should be >
      single(SearchMetrics.KnowhereCalls)
  }

  test("deleted rows and filtered rows never come back") {
    val deleted = hits(
      MilvusSearch.search(
        spark,
        options(need("MILVUS_UAT_V3_DELETED_SNAPSHOT")),
        queries(Seq(0L)),
        "v",
        5,
        "L2",
        mode = "exact",
        outputColumns = Seq("id")
      )
    )

    idsOf(deleted, 0L).toSet.intersect(deletedIds) shouldBe empty
    idsOf(deleted, 0L).head shouldBe 10L

    val filtered = hits(
      MilvusSearch.search(
        spark,
        options(need("MILVUS_UAT_V3_SNAPSHOT")),
        queries(Seq(100L)),
        "v",
        3,
        "L2",
        mode = "exact",
        filter = Some("id >= 200"),
        outputColumns = Seq("id")
      )
    )

    idsOf(filtered, 100L).min should be >= 200L
    idsOf(filtered, 100L).head shouldBe 200L
  }

  test("without output columns the result is the hits themselves") {
    val found = MilvusSearch.search(
      spark,
      options(need("MILVUS_UAT_V3_SNAPSHOT")),
      queries(Seq(42L)),
      "v",
      2,
      "L2",
      mode = "exact"
    )

    found.schema.fieldNames.toSeq shouldBe Seq(
      "query_id",
      "rank",
      "_score",
      "_segment_id",
      "_row_offset"
    )
    found.count() shouldBe 2L
  }

  // ---------------------------------------------------------------- index

  test("index search finds what the exact scan finds") {
    val snapshot =
      env("MILVUS_UAT_INDEXED_SNAPSHOT").getOrElse(
        cancel("set MILVUS_UAT_INDEXED_SNAPSHOT")
      )
    val targets = Seq(11L, 640L, 2500L)
    def search(mode: String) = hits(
      MilvusSearch.search(
        spark,
        options(snapshot),
        queries(targets),
        "v",
        5,
        "L2",
        mode = mode,
        outputColumns = Seq("id")
      )
    )

    val exact = search("exact")
    val indexed = search("index")

    indexed.map(_.getAs[Long]("query_id")) shouldBe exact.map(
      _.getAs[Long]("query_id")
    )
    targets.foreach { target =>
      idsOf(indexed, target).head shouldBe target
      // Recall at 5 against the exact answer of the same query.
      val overlap =
        idsOf(indexed, target).toSet.intersect(idsOf(exact, target).toSet)
      withClue(s"query $target recall: ")(overlap.size should be >= 4)
    }
  }

  // ---------------------------------------------------------------- metrics

  test("a search reports its work in the milvus.search.* accumulators") {
    val snapshot = need("MILVUS_UAT_V3_SNAPSHOT")
    val counted = mutable.Map.empty[String, Long]
    val listener = new SparkListener {
      override def onTaskEnd(end: SparkListenerTaskEnd): Unit =
        end.taskInfo.accumulables
          .filter(_.name.exists(_.startsWith("milvus.search.")))
          .foreach { value =>
            val update = value.update.map(_.toString.toLong).getOrElse(0L)
            counted(value.name.get) = counted.getOrElse(value.name.get, 0L) +
              update
          }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      MilvusSearch
        .search(
          spark,
          options(snapshot),
          queries(Seq(11L, 12L)),
          "v",
          3,
          "L2",
          mode = "exact",
          outputColumns = Seq("id")
        )
        .collect()
      // Accumulator updates reach the listener after the tasks end.
      val deadline = System.nanoTime() + 30L * 1000L * 1000L * 1000L
      while (
        System.nanoTime() < deadline &&
        !counted.contains(SearchMetrics.TakeRows)
      ) Thread.sleep(50L)
    } finally spark.sparkContext.removeSparkListener(listener)

    counted.getOrElse(SearchMetrics.Segments, 0L) should be > 0L
    counted.getOrElse(SearchMetrics.KnowhereCalls, 0L) should be > 0L
    counted.getOrElse(SearchMetrics.KnowhereNanos, 0L) should be > 0L
    counted.getOrElse(SearchMetrics.ReadBytes, 0L) should be > 0L
    counted.getOrElse(SearchMetrics.Candidates, 0L) should be >= 6L
    counted.getOrElse(SearchMetrics.TakeRows, 0L) should be > 0L
    counted.getOrElse(SearchMetrics.TakeNanos, 0L) should be > 0L
  }

  // ---------------------------------------------------------------- failures

  test("a query set the collection cannot answer fails before any task runs") {
    val snapshot = need("MILVUS_UAT_V3_SNAPSHOT")

    the[IllegalArgumentException] thrownBy MilvusSearch.search(
      spark,
      options(snapshot),
      queries(Seq(1L, 1L)),
      "v",
      2,
      "L2",
      mode = "exact"
    )

    the[IllegalArgumentException] thrownBy MilvusSearch.search(
      spark,
      options(snapshot),
      queries(Seq(1L)),
      "missing_field",
      2,
      "L2",
      mode = "exact"
    )
  }
}
