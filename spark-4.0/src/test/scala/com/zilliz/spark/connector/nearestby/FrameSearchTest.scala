package com.zilliz.spark.connector.nearestby

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  BoundReference,
  GenericInternalRow
}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, StringType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.milvus.storage.index.SearchPlan
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.spark.connector.extensions.ConnectorVectorRanking
import com.zilliz.spark.connector.metrics.NearestByMetrics
import com.zilliz.spark.connector.options.SearchLimits
import com.zilliz.spark.connector.read.{FrameSearch, NearestBySearch}

/** A DataFrame input's search with limits small enough to cut its queries into
  * several groups and to keep them off the driver: every base partition meets
  * every group, the groups arrive by broadcast or by shuffle, and the query
  * rows meet their hits by number either way
  * (docs/design/architecture/dataframe-api.html section 4). The answer is a
  * brute-force search's.
  */
class FrameSearchTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var available = true
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    try NativeVectorLibrary.load()
    catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError |
          _: RuntimeException =>
        available = false
    }
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("frame-search")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  import FrameSearchTest._

  private val vectorType = ArrayType(FloatType, containsNull = true)

  /** Forty rows in four partitions, no two at the same distance from a query.
    */
  private val baseVectors =
    (0 until 40).map(id => id.toLong -> Array(id.toFloat, (id % 7).toFloat))

  private def base: RDD[InternalRow] =
    spark.sparkContext.parallelize(baseVectors, 4).map(row)

  private val queryVectors =
    (0 until 12).map(q => s"q$q" -> Array(q * 3.3f + 0.1f, q % 5 + 0.37f))

  private def queryRows: Seq[InternalRow] =
    queryVectors.map { case (qid, values) =>
      new GenericInternalRow(
        Array[Any](UTF8String.fromString(qid), vector(values: _*))
      )
    } :+ new GenericInternalRow(
      Array[Any](UTF8String.fromString("none"), null)
    )

  private def bruteForce(k: Int, outer: Boolean): Seq[(String, Long)] =
    (queryVectors.flatMap { case (qid, query) =>
      baseVectors
        .map { case (id, row) =>
          id -> query.zip(row).map { case (a, b) => (a - b) * (a - b) }.sum
        }
        .sortBy(_._2)
        .take(k)
        .map(hit => qid -> hit._1)
    } ++ (if (outer) Seq("none" -> -1L) else Seq.empty)).sorted

  test(
    "every base partition meets every query group, by broadcast or by shuffle"
  ) {
    assume(available, "the native libraries are not on this machine")
    val k = 3
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val types = Seq(LongType, vectorType)
    val perQuery = SearchPlan.bytesPerQuery(layout, k) +
      k * FrameSearch.rowWidth(types, 2)
    // Three queries a group, and nothing broadcast from a frame.
    val limits = SearchLimits(1L, 3L * perQuery, None)
    FrameSearch
      .cut(12, layout, k, FrameSearch.rowWidth(types, 2), limits.groupMaxBytes)
      .map(_.queries) shouldBe Seq(3, 3, 3, 3)
    for {
      held <- Seq(true, false)
      outer <- Seq(false, true)
    } withClue(s"held=$held outer=$outer: ") {
      val queries =
        if (held) NearestBySearch.Held(queryRows.toArray)
        else
          NearestBySearch.Computed(
            spark.sparkContext.parallelize(queryRows, 3)
          )
      val rows = NearestBySearch
        .run(
          spark,
          queries,
          Seq(StringType, vectorType),
          BoundReference(1, vectorType, nullable = true),
          NearestBySearch.Frame(
            FrameSearch.Base(
              base,
              types,
              BoundReference(1, vectorType, nullable = true)
            ),
            limits
          ),
          outer = outer,
          approx = false,
          k = k,
          metric = MetricType.L2,
          outputTypes = Seq(StringType, vectorType, LongType, vectorType),
          function = ConnectorVectorRanking(
            MetricType.L2,
            "vector_l2_distance",
            queryFirst = true
          ),
          bySpark = _ => fail("no query row is Spark's"),
          metrics = NearestByMetrics.create(spark.sparkContext)
        )
        .map(row =>
          row.getUTF8String(0).toString ->
            (if (row.isNullAt(2)) -1L else row.getLong(2))
        )
        .collect()
        .toSeq
        .sorted
      rows shouldBe bruteForce(k, outer)
    }
  }
}

object FrameSearchTest {

  def vector(values: Float*): GenericArrayData =
    new GenericArrayData(values.map(Float.box).toArray[Any])

  def row(entry: (Long, Array[Float])): InternalRow =
    new GenericInternalRow(Array[Any](entry._1, vector(entry._2: _*)))
}
