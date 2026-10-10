package com.zilliz.spark.connector.uat

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{call_function, col}
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
import com.zilliz.spark.connector.extensions.{
  MilvusNearestByJoinExec,
  MilvusSparkPlugin,
  MilvusSparkSessionExtensions
}
import com.zilliz.spark.connector.implicits._
import com.zilliz.spark.connector.options.MilvusOption

/** NEAREST BY on the UAT collections, judged against the answer worked out from
  * the ids: row `id` holds `v = [id*10 + d]`, so the squared L2 between rows
  * `a` and `b` is `400 (a - b)^2` and a query's nearest rows are known without
  * reading the collection (docs/design/architecture/dataframe-api.html section
  * 10, step 1). Compiled into every line: Spark's own NEAREST BY on 4.2, the
  * connector's `nearestByJoin` before it.
  *
  * `spark_uat_v3` and its deleted snapshot are the ones `SnapshotReadUatTest`
  * prepares: ids 0 until 3000, `name = row-<id>`, ids 0..9 deleted in the
  * deleted snapshot. The indexed collection is prepared here.
  *
  * Environment:
  * {{{
  *   MILVUS_JNI_S3_BUCKET / MILVUS_JNI_S3_REGION / AWS_*   the bucket
  *   MILVUS_JNI_S3_ROOT_PATH                     instance root
  *   MILVUS_UAT_V3_SNAPSHOT                      spark_uat_v3 before deletes
  *   MILVUS_UAT_V3_DELETED_SNAPSHOT              after deletes (ids 0..9)
  *   MILVUS_UAT_INDEXED_SNAPSHOT                 a snapshot whose vector field
  *                                               carries a persisted index
  *   MILVUS_UAT_URI / MILVUS_UAT_TOKEN           prepare only
  * }}}
  * A case whose variables are missing cancels. Knowhere's native package is
  * Linux only, so this suite runs where that package exists.
  */
class NearestByUatTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with AdaptiveSparkPlanHelper {

  private def env(n: String): Option[String] =
    sys.env.get(n).map(_.trim).filter(_.nonEmpty)

  private def need(n: String): String =
    env(n).getOrElse(cancel(s"set $n"))

  private val dim = 4
  private val rows = 3000L

  private var sparkSession: SparkSession = null

  private def spark: SparkSession = {
    if (sparkSession == null) {
      sparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("nearest-by-uat")
        .config(
          "spark.sql.extensions",
          classOf[MilvusSparkSessionExtensions].getName
        )
        .config("spark.plugins", classOf[MilvusSparkPlugin].getName)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    }
    sparkSession
  }

  override protected def afterAll(): Unit =
    if (sparkSession != null) sparkSession.stop()

  private def options(snapshot: String): Map[String, String] = {
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
      StorageProperties.UseIam -> "true",
      MilvusOption.SnapshotPath -> snapshot
    ) ++ env("MILVUS_JNI_S3_ROOT_PATH").map(StorageProperties.RootPath -> _)
  }

  private def vectorOf(id: Long): Array[Float] =
    Array.tabulate(dim)(d => (id * 10 + d).toFloat)

  private val schema = StructType(
    Seq(
      StructField("query_id", LongType, nullable = false),
      StructField("vector", ArrayType(FloatType, false), nullable = false)
    )
  )

  /** Queries on the rows themselves: query `id` is row `id`'s vector. */
  private def queries(ids: Seq[Long]): DataFrame =
    spark.createDataFrame(ids.map(id => Row(id, vectorOf(id))).asJava, schema)

  /** The `k` live ids nearest row `target`. The answer must not depend on how
    * ties are broken, so the k-th must be nearer than the next.
    */
  private def expected(
      target: Long,
      k: Int,
      live: Long => Boolean
  ): Set[Long] = {
    val nearest = (0L until rows)
      .filter(live)
      .sortBy(id => (math.abs(id - target), id))
      .take(k + 1)
    withClue(s"the $k nearest rows of $target are not unique: ") {
      math.abs(nearest(k - 1) - target) should be <
        math.abs(nearest(k) - target)
    }
    nearest.take(k).toSet
  }

  /** Each query's hits from NEAREST BY over a snapshot, as id and name, and the
    * check that the connector took the join over.
    */
  private def nearest(
      snapshot: String,
      ids: Seq[Long],
      k: Int,
      mode: String,
      filter: Option[Column] = None
  ): Map[Long, Seq[(Long, String)]] = {
    val query = queries(ids)
    val read = spark.read.format("milvus").options(options(snapshot)).load()
    val base = filter.fold(read)(condition => read.where(condition))
    val joined = query.nearestByJoin(
      base,
      call_function("vector_l2_distance", query("vector"), base("v")),
      k,
      mode,
      "distance"
    )
    val taken = collectFirst(joined.queryExecution.executedPlan) {
      case exec: MilvusNearestByJoinExec => exec
    }
    withClue("taken over as a Milvus table input: ")(taken should not be empty)
    joined
      .select(col("query_id"), col("id"), col("name"))
      .collect()
      .groupBy(_.getLong(0))
      .map { case (query, hits) =>
        query -> hits.map(hit => (hit.getLong(1), hit.getString(2))).toSeq
      }
  }

  private val targets = (0L until 300L).map(_ * 10 + 3)

  // -------------------------------------------------------------- prepare

  /** Builds the collection the APPROX case needs: rows like `spark_uat_v3`'s,
    * an HNSW index on the vector field, and a snapshot taken after the index is
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

  // ---------------------------------------------------------------- cases

  test("EXACT NEAREST BY finds each query's nearest rows and their columns") {
    val found = nearest(need("MILVUS_UAT_V3_SNAPSHOT"), targets, 5, "exact")

    found.keySet shouldBe targets.toSet
    targets.foreach { target =>
      withClue(s"query $target: ") {
        found(target).map(_._1).toSet shouldBe expected(target, 5, _ => true)
        found(target).foreach { case (id, name) => name shouldBe s"row-$id" }
      }
    }
  }

  test("deleted rows and filtered rows never come back") {
    val deleted = (0L until 10L).toSet
    val near = 0L until 20L
    val found =
      nearest(need("MILVUS_UAT_V3_DELETED_SNAPSHOT"), near, 5, "exact")

    near.foreach { target =>
      withClue(s"query $target: ") {
        found(target).map(_._1).toSet shouldBe
          expected(target, 5, id => !deleted(id))
      }
    }

    val filtered = nearest(
      need("MILVUS_UAT_V3_SNAPSHOT"),
      Seq(100L),
      3,
      "exact",
      Some(col("id") >= 200L)
    )
    filtered(100L).map(_._1).toSet shouldBe Set(200L, 201L, 202L)
  }

  test("APPROX NEAREST BY searches the index and finds the nearest rows") {
    val snapshot = need("MILVUS_UAT_INDEXED_SNAPSHOT")
    val near = Seq(11L, 640L, 2500L)
    val approx = nearest(snapshot, near, 5, "approx")
    val exact = nearest(snapshot, near, 5, "exact")

    near.foreach { target =>
      val truth = expected(target, 5, _ => true)
      exact(target).map(_._1).toSet shouldBe truth
      val found = approx(target).map(_._1).toSet
      found should contain(target)
      // Recall at 5 against the exact answer of the same query.
      withClue(s"query $target recall: ") {
        found.intersect(truth).size should be >= 4
      }
    }
  }
}
