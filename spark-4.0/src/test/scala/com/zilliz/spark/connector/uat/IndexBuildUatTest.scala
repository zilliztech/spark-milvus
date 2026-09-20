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

  private val collection = env("MILVUS_UAT_BIG_COLLECTION").getOrElse(
    if (env("MILVUS_UAT_BIG_DISTRIBUTION").contains("uniform"))
      "spark_uat_index_big"
    else "spark_uat_index_cl"
  )
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

  /** How the vectors are distributed. `clustered` is what an embedding model
    * produces — points gathered around centroids, so a neighbourhood means
    * something. `uniform` is the worst case for any graph or list index: in 128
    * dimensions every point is about as far from a query as every other, and
    * recall falls however the index is tuned.
    */
  private val distribution =
    env("MILVUS_UAT_BIG_DISTRIBUTION").getOrElse("clustered")

  private val clusters = 200

  private def centroid(cluster: Int): Array[Float] = {
    val random = new java.util.Random(0x51ed270bL * (cluster + 1))
    Array.fill(dim)(random.nextFloat() * 10f)
  }

  /** A row's vector, from its id alone. `java.util.Random` is specified down to
    * the bit, so the values inserted into Milvus and the values this test
    * computes a baseline from are the same numbers.
    */
  private def vectorOf(id: Long): Array[Float] = {
    val random = new java.util.Random(id * 0x9e3779b97f4a7c15L)
    if (distribution == "uniform") Array.fill(dim)(random.nextFloat())
    else {
      val around = centroid((id % clusters.toLong).toInt)
      Array.tabulate(dim)(d =>
        around(d) + (random.nextGaussian().toFloat * 0.35f)
      )
    }
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

  /** Retries a call the service did not answer in time: a resumed instance is
    * slow for its first requests and the client's deadline is ten seconds.
    */
  private def retried[A](what: String)(call: => scala.util.Try[A]): A = {
    var attempt = 0
    var result: Option[A] = None
    while (result.isEmpty) {
      val outcome =
        try call
        catch { case failure: Throwable => scala.util.Failure(failure) }
      outcome match {
        case scala.util.Success(value) => result = Some(value)
        case scala.util.Failure(failure)
            if attempt < 10 &&
              failure.getMessage.contains("DEADLINE_EXCEEDED") =>
          attempt += 1
          info(s"$what timed out, retrying (attempt $attempt)")
          Thread.sleep(5000L)
        case scala.util.Failure(failure) => throw failure
      }
    }
    result.get
  }

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

        // One insert has to answer inside the client's ten second write
        // deadline, and a just-resumed instance is slower than a warm one.
        val batch = env("MILVUS_UAT_BIG_BATCH").map(_.toInt).getOrElse(5000)
        val perFlush = 25000
        var written = 0L
        while (written < rows) {
          val ids = (written until math.min(written + batch, rows)).toSeq
          retried("insert") {
            client.insert(
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
          }
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
      widthName: String,
      widths: Seq[Int],
      minimumRecall: Double
  )

  /** Each family is built once and then searched at several widths: `ef` for
    * HNSW, `nprobe` for IVF. The width is the knob a user turns when recall is
    * short, so the curve is part of what this reports.
    */
  private val families = Seq(
    Family(
      "HNSW",
      "M=16,efConstruction=200",
      "ef",
      Seq(64, 128, 256, 512),
      0.95
    ),
    Family("IVF_FLAT", "nlist=128", "nprobe", Seq(8, 32, 128), 0.95),
    Family("FLAT", "", "", Seq.empty, 1.0)
  )

  /** Writes the snapshot for a build job that already ran, which is how the
    * delivery half is exercised without building again — and how a snapshot can
    * be placed where a particular reader needs it. Milvus's external restore,
    * for one, requires every path in the snapshot to sit under the root its
    * metadata URI derives, so the output prefix is the experiment.
    *
    * Needs `MILVUS_UAT_BIG_JOB` (the job id `build_index` returned),
    * `MILVUS_UAT_BIG_INPUT` (the prefix it wrote under) and
    * `MILVUS_UAT_BIG_OUTPUT`.
    */
  test("write_snapshot places a finished build's snapshot under a prefix") {
    val snapshot = need("MILVUS_UAT_BIG_SNAPSHOT")
    val job = need("MILVUS_UAT_BIG_JOB")
    val input = need("MILVUS_UAT_BIG_INPUT")
    val output = need("MILVUS_UAT_BIG_OUTPUT")
    val rows = WriteSnapshotProcedure.run(
      ProcedureArgs(
        values = Map(
          "collection" -> collection,
          "job" -> job,
          "input" -> input,
          "output" -> output
        ),
        options = storageOptions() ++ Map(MilvusOption.SnapshotPath -> snapshot)
      )
    )
    rows should have size 1
    info(
      s"snapshot=${rows.head.getString(0)} segments=${rows.head.getInt(3)} " +
        s"indexes=${rows.head.getInt(4)} bytes=${rows.head.getLong(5)}"
    )
  }

  test("the connector builds an index over a real snapshot and searches it") {
    val snapshot = need("MILVUS_UAT_BIG_SNAPSHOT")
    val output = env("MILVUS_UAT_BIG_OUTPUT")
      .getOrElse("spark-uat-index/" + System.currentTimeMillis())
    val source = storageOptions() ++ Map(MilvusOption.SnapshotPath -> snapshot)
    info(s"$distribution data, writing under $output")

    // What the answers are judged against, computed from the ids alone.
    val truth = queryIds.map(id => id -> baseline(queryOf(id))).toMap
    truth.foreach { case (_, best) =>
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

    val measured = families.map { family =>
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
      val indexOptions =
        storageOptions() ++ Map(MilvusOption.SnapshotPath -> builtSnapshot)

      val widths = if (family.widths.isEmpty) Seq(0) else family.widths
      val curve = widths.map { width =>
        val parameters =
          if (family.widths.isEmpty) Map.empty[String, String]
          else Map(family.widthName -> width.toString)
        val startSearch = System.nanoTime()
        val found = hits(
          MilvusSearch.search(
            spark,
            indexOptions,
            queries(queryIds),
            "v",
            topK,
            "L2",
            mode = "index",
            searchParameters = parameters,
            outputColumns = Seq("id")
          )
        )
        val searchMillis = (System.nanoTime() - startSearch) / 1000000L
        found.keySet shouldBe queryIds.toSet
        queryIds.foreach { id =>
          found(id) should have size topK
          found(id).map(_._1).foreach(hit => deleted should not contain hit)
        }
        val average =
          queryIds.map(id => recall(found(id), truth(id))).sum / queryIds.size
        info(
          f"${family.indexType}%-9s ${family.widthName}%-6s $width%-4d " +
            f"recall=$average%.4f search=${searchMillis}ms"
        )
        (width, average, found)
      }

      info(
        f"${family.indexType}%-9s segments=${built.size}%-2d rows=$indexedRows%-7d " +
          f"bytes=$indexBytes%-10d build=${buildMillis}ms"
      )
      (
        family,
        built.size,
        indexedRows,
        indexBytes,
        buildMillis,
        curve,
        builtSnapshot
      )
    }

    // Judged after every family has run, so one short recall does not hide the
    // rest of the evidence.
    measured.foreach { case (family, _, _, _, _, curve, _) =>
      val best = curve.map(_._2).max
      withClue(
        s"${family.indexType} best recall $best over widths " +
          curve.map(row => s"${row._1}:${row._2}").mkString(", ") + ": "
      ) {
        best should be >= family.minimumRecall
      }
      if (family.minimumRecall >= 1.0) {
        val found = curve.head._3
        queryIds.foreach { id =>
          found(id).map(_._1) shouldBe truth(id).map(_._1)
        }
      }
    }

    // A filter runs before the index is probed, so the answer has to be the
    // nearest rows among the rows that pass, not the nearest rows filtered
    // afterwards.
    val filtered = queryIds.take(4)
    val filteredTruth =
      filtered.map(id => id -> baseline(queryOf(id), _ % 10L == 3L)).toMap
    val hnswSnapshot = measured.head._7
    val filteredHits = hits(
      MilvusSearch.search(
        spark,
        storageOptions() ++ Map(MilvusOption.SnapshotPath -> hnswSnapshot),
        queries(filtered),
        "v",
        topK,
        "L2",
        mode = "index",
        searchParameters = Map("ef" -> "512"),
        filter = Some("label == 3"),
        outputColumns = Seq("id")
      )
    )
    filtered.foreach { id =>
      filteredHits(id).map(_._1).foreach(hit => (hit % 10L) shouldBe 3L)
      withClue(s"filtered query $id: ") {
        recall(filteredHits(id), filteredTruth(id)) should be >= 0.9
      }
    }
    info(
      s"filtered search matches filtered brute force on ${filtered.size} queries"
    )
  }
}
