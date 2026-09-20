package com.zilliz.spark.connector.uat

import scala.jdk.CollectionConverters._

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
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.procedure.{
  BuildIndexProcedure,
  ProcedureArgs,
  WriteSnapshotProcedure
}
import com.zilliz.spark.connector.read.MilvusSearch

/** Building indexes over a real collection, and searching what was built.
  *
  * The chain under test is the one a user runs: a Milvus collection with real
  * data on real object storage, a snapshot Milvus itself wrote, `build_index`
  * over that snapshot, `write_snapshot` over the job it produced, and a search
  * through the snapshot that came out.
  *
  * What the results are judged against is computed here, not read back from
  * either side: every row's vector is a function of its id, so the exact top-k
  * of a query is brute force over the ids, and both the connector's exact scan
  * and its index search are compared against that. Deleted ids must be absent
  * from every answer.
  *
  * Environment:
  * {{{
  *   MILVUS_UAT_URI / MILVUS_UAT_TOKEN      prepare only, through a port-forward
  *   MILVUS_JNI_S3_BUCKET / _REGION / AWS_* the bucket
  *   MILVUS_JNI_S3_ROOT_PATH                instance root
  *   MILVUS_UAT_BIG_SNAPSHOT                the snapshot prepare printed
  *   MILVUS_UAT_BIG_OUTPUT                  prefix the build writes under
  * }}}
  * A case whose variables are missing cancels. Knowhere's native package is
  * Linux only, so the build and search cases run where that package exists.
  */
class IndexBuildUatTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private def env(n: String): Option[String] =
    sys.env.get(n).map(_.trim).filter(_.nonEmpty)

  private def need(n: String): String =
    env(n).getOrElse(cancel(s"set $n"))

  private val collection =
    env("MILVUS_UAT_BIG_COLLECTION").getOrElse("spark_uat_index_big")
  private val dim = env("MILVUS_UAT_BIG_DIM").map(_.toInt).getOrElse(128)
  private val rows = env("MILVUS_UAT_BIG_ROWS").map(_.toLong).getOrElse(100000L)
  private val deleted: Set[Long] = (0L until 100L).toSet
  private val topK = 10

  private var sparkSession: SparkSession = null

  private def spark: SparkSession = {
    if (sparkSession == null) {
      sparkSession = SparkSession
        .builder()
        .master("local[4]")
        .appName("index-build-uat")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    }
    sparkSession
  }

  override protected def afterAll(): Unit =
    if (sparkSession != null) sparkSession.stop()

  // ------------------------------------------------------------ the data

  /** A row's vector, from its id alone. `java.util.Random` is specified down to
    * the bit, so the values inserted into Milvus and the values this test
    * computes a baseline from are the same numbers.
    */
  private def vectorOf(id: Long): Array[Float] = {
    val random = new java.util.Random(id * 0x9e3779b97f4a7c15L)
    Array.fill(dim)(random.nextFloat())
  }

  /** A query near a row but not on it, so the answer is a neighbourhood rather
    * than one exact match.
    */
  private def queryVector(id: Long, jitter: Float): Array[Float] = {
    val random = new java.util.Random(id ^ 0x5deece66dL)
    vectorOf(id).map(value => value + (random.nextFloat() - 0.5f) * jitter)
  }

  private def squaredL2(left: Array[Float], right: Array[Float]): Double = {
    var sum = 0.0d
    var i = 0
    while (i < left.length) {
      val d = left(i).toDouble - right(i).toDouble
      sum += d * d
      i += 1
    }
    sum
  }

  private val queryIds: Seq[Long] =
    Seq(137L, 2048L, 5000L, 12345L, 33333L, 50000L, 67890L, 84321L, 99999L,
      41414L)

  private def queryOf(id: Long): Array[Float] =
    queryVector(id, if (id % 2 == 0) 0.4f else 0.0f)

  /** The answer, computed here: the `topK` nearest ids by squared L2, deleted
    * ids excluded, ties broken by the smaller id — the order a search returns.
    */
  private def baseline(
      query: Array[Float],
      filter: Long => Boolean = _ => true
  ): Seq[(Long, Double)] = {
    val best = scala.collection.mutable.PriorityQueue
      .empty[(Long, Double)](Ordering.by { case (id, score) => (score, id) })
    var id = 0L
    while (id < rows) {
      if (!deleted(id) && filter(id)) {
        val score = squaredL2(query, vectorOf(id))
        if (best.size < topK) best.enqueue(id -> score)
        else {
          val (worstId, worstScore) = best.head
          if (score < worstScore || (score == worstScore && id < worstId)) {
            best.dequeue()
            best.enqueue(id -> score)
          }
        }
      }
      id += 1L
    }
    best.dequeueAll.reverse.toSeq
  }

  // --------------------------------------------------------------- setup

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

  private def queries(ids: Seq[Long]): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("query_id", LongType, nullable = false),
        StructField("vector", ArrayType(FloatType, false), nullable = false)
      )
    )
    spark.createDataFrame(
      ids.map(id => Row(id, queryOf(id))).asJava,
      schema
    )
  }

  private def hits(result: DataFrame): Map[Long, Seq[(Long, Double)]] = result
    .orderBy("query_id", "rank")
    .collect()
    .toSeq
    .groupBy(_.getAs[Long]("query_id"))
    .map { case (queryId, rows) =>
      queryId -> rows
        .map(row => row.getAs[Long]("id") -> row.getAs[Double]("_score"))
        .toSeq
    }

  private def recall(
      found: Seq[(Long, Double)],
      truth: Seq[(Long, Double)]
  ): Double = {
    val expected = truth.map(_._1).toSet
    found.count(hit => expected(hit._1)).toDouble / truth.size.toDouble
  }

  // ------------------------------------------------------------- prepare

  /** Flushes, waiting out the service's flush rate limit rather than failing on
    * it: the limit is one call every ten seconds.
    */
  private def flushed(
      client: com.zilliz.milvus.client.api.MilvusClient
  ): Unit = {
    var attempt = 0
    var done = false
    while (!done) {
      client.flush(collectionNames = Seq(collection)) match {
        case scala.util.Success(_) => done = true
        case scala.util.Failure(failure)
            if attempt < 12 &&
              failure.getMessage.toLowerCase.contains("rate limit") =>
          attempt += 1
          info(s"flush rate limited, retrying in 15s (attempt $attempt)")
          Thread.sleep(15000L)
        case scala.util.Failure(failure) => throw failure
      }
    }
  }

  /** Creates the collection this suite searches: `rows` rows of `dim`
    * dimensions written in several flushes so the snapshot holds several
    * segments, then deletes the first hundred ids. Prints the snapshot to
    * export as `MILVUS_UAT_BIG_SNAPSHOT`.
    *
    * The snapshot must be taken after Milvus has folded the delete into the
    * segment manifests (its L0 compaction), because a delete-only segment is
    * storage version 0 and the connector's snapshot writer only describes
    * version 3 segments.
    */
  test("prepare: a collection of 100k rows with deletes") {
    import io.milvus.grpc.schema._
    val uri = need("MILVUS_UAT_URI")
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
            client.createCollectionField("label", dataType = DataType.Int64),
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

        val batch = 5000
        val perFlush = 25000
        var written = 0L
        while (written < rows) {
          val ids = (written until math.min(written + batch, rows)).toSeq
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
                  `type` = DataType.Int64,
                  fieldName = "label",
                  field = FieldData.Field.Scalars(
                    ScalarField(data =
                      ScalarField.Data.LongData(
                        LongArray(data = ids.map(_ % 10L))
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
                        FloatArray(data = ids.flatMap(vectorOf(_).toSeq))
                      )
                    )
                  )
                )
              ),
              numRows = ids.size
            )
            .get
          written += ids.size.toLong
          if (written % perFlush == 0) {
            flushed(client)
            info(s"inserted and flushed $written of $rows rows")
          }
        }
        info(s"inserted $rows rows")
      }

      // Deleting ids that are already gone is a delete record Milvus drops, so
      // this runs whether or not the collection was created by this call.
      Thread.sleep(20000L)
      client
        .delete(
          collectionName = collection,
          pkName = Some("id"),
          pks = deleted.toSeq.sorted.map(_.toInt)
        )
        .get
      flushed(client)
      info(s"deleted ids 0 until ${deleted.size}")

      // Milvus needs a moment to seal the segments and to fold the delete into
      // the segment manifests; the snapshot has to come after both.
      Thread.sleep(
        env("MILVUS_UAT_BIG_WAIT_MS").map(_.toLong).getOrElse(120000L)
      )
      val name = "spark_uat_index_big_" + System.currentTimeMillis()
      val snapshot = client
        .createSnapshotForRead(
          "",
          collection,
          name,
          "spark-milvus UAT index build",
          86400L
        )
        .get
      info(s"created snapshot ${snapshot.name} at ${snapshot.s3Location}")
      info(s"export MILVUS_UAT_BIG_SNAPSHOT=${snapshot.s3Location}")
    } finally client.close()
  }

  // ---------------------------------------------------------- end to end

  private case class Family(
      indexType: String,
      buildParameters: String,
      searchParameters: Map[String, String],
      minimumRecall: Double
  )

  private val families = Seq(
    Family("HNSW", "M=16,efConstruction=200", Map("ef" -> "128"), 0.95),
    Family("IVF_FLAT", "nlist=128", Map("nprobe" -> "32"), 0.90),
    Family("FLAT", "", Map.empty, 1.0)
  )

  test("the connector builds an index over a real snapshot and searches it") {
    val snapshot = need("MILVUS_UAT_BIG_SNAPSHOT")
    val output = env("MILVUS_UAT_BIG_OUTPUT")
      .getOrElse("spark-uat-index/" + System.currentTimeMillis())
    val source = storageOptions() ++ Map(MilvusOption.SnapshotPath -> snapshot)
    info(s"writing under $output")

    // What the answers are judged against, computed from the ids alone.
    val truth = queryIds.map(id => id -> baseline(queryOf(id))).toMap
    truth.foreach { case (id, best) =>
      best should have size topK
      best.map(_._1).foreach(hit => deleted should not contain hit)
    }

    val exact = hits(
      MilvusSearch.search(
        spark,
        source,
        queries(queryIds),
        "v",
        topK,
        "L2",
        mode = "exact",
        outputColumns = Seq("id")
      )
    )
    exact.keySet shouldBe queryIds.toSet
    queryIds.foreach { id =>
      exact(id).map(_._1) shouldBe truth(id).map(_._1)
      exact(id).map(_._2).zip(truth(id).map(_._2)).foreach {
        case (found, expected) => found shouldBe (expected +- 1e-3)
      }
    }
    info(s"exact scan matches brute force on all ${queryIds.size} queries")

    val summary = families.map { family =>
      val startBuild = System.nanoTime()
      val built = BuildIndexProcedure.run(
        ProcedureArgs(
          values = Map(
            "collection" -> collection,
            "field" -> "v",
            "output" -> s"$output/${family.indexType.toLowerCase}",
            "index_type" -> family.indexType,
            "metric" -> "L2",
            "build_id" -> System.currentTimeMillis()
          ) ++ Option(family.buildParameters)
            .filter(_.nonEmpty)
            .map("params" -> _),
          options = source
        )
      )
      val buildMillis = (System.nanoTime() - startBuild) / 1000000L
      built should not be empty
      val indexedRows = built.map(_.getLong(2)).sum
      val indexBytes = built.map(_.getLong(4)).sum
      val jobId = built.head.getString(6)

      val written = WriteSnapshotProcedure.run(
        ProcedureArgs(
          values = Map(
            "collection" -> collection,
            "job" -> jobId,
            "input" -> s"$output/${family.indexType.toLowerCase}"
          ),
          options = source
        )
      )
      written should have size 1
      val builtSnapshot = written.head.getString(0)

      val startSearch = System.nanoTime()
      val found = hits(
        MilvusSearch.search(
          spark,
          storageOptions() ++ Map(MilvusOption.SnapshotPath -> builtSnapshot),
          queries(queryIds),
          "v",
          topK,
          "L2",
          mode = "index",
          searchParameters = family.searchParameters,
          outputColumns = Seq("id")
        )
      )
      val searchMillis = (System.nanoTime() - startSearch) / 1000000L

      found.keySet shouldBe queryIds.toSet
      queryIds.foreach { id =>
        found(id) should have size topK
        found(id).map(_._1).foreach(hit => deleted should not contain hit)
      }
      val recalls = queryIds.map(id => recall(found(id), truth(id)))
      val average = recalls.sum / recalls.size

      if (family.minimumRecall >= 1.0) {
        // An index that stores the vectors as they are answers exactly what
        // the exact scan does, ids and scores alike.
        queryIds.foreach { id =>
          found(id).map(_._1) shouldBe truth(id).map(_._1)
        }
      }
      withClue(s"${family.indexType} recall $average: ") {
        average should be >= family.minimumRecall
      }

      info(
        f"${family.indexType}%-9s segments=${built.size}%-3d rows=$indexedRows%-7d " +
          f"bytes=$indexBytes%-10d build=${buildMillis}ms search=${searchMillis}ms recall=$average%.4f"
      )
      (
        family.indexType,
        built.size,
        indexedRows,
        indexBytes,
        average,
        builtSnapshot
      )
    }

    info(
      "index families verified against brute force: " +
        summary.map(row => s"${row._1} recall=${row._5}").mkString(", ")
    )

    // A filter runs before the index is probed, so the answer has to be the
    // nearest rows among the rows that pass, not the nearest rows filtered
    // afterwards.
    val filtered = queryIds.take(4)
    val filteredTruth =
      filtered.map(id => id -> baseline(queryOf(id), _ % 10L == 3L)).toMap
    val hnswSnapshot = summary.head._6
    val filteredHits = hits(
      MilvusSearch.search(
        spark,
        storageOptions() ++ Map(MilvusOption.SnapshotPath -> hnswSnapshot),
        queries(filtered),
        "v",
        topK,
        "L2",
        mode = "index",
        searchParameters = Map("ef" -> "256"),
        filter = Some("label == 3"),
        outputColumns = Seq("id")
      )
    )
    filtered.foreach { id =>
      filteredHits(id).map(_._1).foreach(hit => (hit % 10L) shouldBe 3L)
      withClue(s"filtered query $id: ") {
        recall(filteredHits(id), filteredTruth(id)) should be >= 0.95
      }
    }
    info(
      s"filtered search matches filtered brute force on ${filtered.size} queries"
    )
  }

}
