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
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.milvus.storage.vector.SearchPlan
import com.zilliz.spark.connector.extensions.ConnectorVectorRanking
import com.zilliz.spark.connector.metrics.{NearestByMetrics, SearchMetrics}
import com.zilliz.spark.connector.options.SearchLimits
import com.zilliz.spark.connector.search.{
  FrameSearch,
  NearestByInput,
  NearestByQueryRows,
  NearestBySearch,
  SearchExecutionPlan
}

/** A DataFrame input's search with limits small enough to cut its queries into
  * several groups and to keep them off the driver: every base partition meets
  * every group, the groups arrive by broadcast or by shuffle, and the query
  * rows meet their hits by number either way
  * (docs/design/projects/spark-milvus/architecture/dataframe-api.html section
  * 4). The answer is a brute-force search's.
  */
class FrameSearchTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with Eventually {

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
      SearchPlan.QueryIdBytes + SearchPlan.TopKQueueBytes +
      k * FrameSearch.rowWidth(types, 2)
    // Three queries a group, and nothing broadcast from a frame.
    val group = 3L * perQuery
    val limits = SearchLimits(1L, Some(group), None)
    SearchExecutionPlan
      .frameGroups(
        12,
        layout,
        k,
        FrameSearch.rowWidth(types, 2),
        group,
        Long.MaxValue
      )
      .map(_.queries) shouldBe Seq(3, 3, 3, 3)
    for {
      held <- Seq(true, false)
      outer <- Seq(false, true)
    } withClue(s"held=$held outer=$outer: ") {
      val queries =
        if (held) NearestByQueryRows.Held(queryRows.toArray)
        else
          NearestByQueryRows.Computed(
            spark.sparkContext
              .parallelize(queryRows, 3)
              .union(spark.sparkContext.parallelize(Seq.empty[InternalRow], 2))
          )
      val metrics = NearestByMetrics.create(spark.sparkContext)
      val context = spark.sparkContext
      val preparation = s"prepare-frame-queries-$held-$outer"
      val collection = s"collect-frame-queries-$held-$outer"
      context.setJobGroup(preparation, "Prepare query groups")
      val result =
        try {
          NearestBySearch.run(
            spark,
            queries,
            Seq(StringType, vectorType),
            BoundReference(1, vectorType, nullable = true),
            NearestByInput.Frame(
              FrameSearch.Base(
                base,
                types.zipWithIndex.map { case (dataType, i) =>
                  BoundReference(i, dataType, nullable = true)
                },
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
            metrics = metrics
          )
        } finally context.clearJobGroup()
      context.setJobGroup(collection, "Collect joined rows")
      val rows =
        try {
          result
            .map(row =>
              row.getUTF8String(0).toString ->
                (if (row.isNullAt(2)) -1L else row.getLong(2))
            )
            .collect()
            .toSeq
            .sorted
        } finally context.clearJobGroup()
      // Seeing the later collection job means the status listener has also
      // consumed every preparation job's start event.
      eventually(timeout(Span(10, Seconds))) {
        context.statusTracker.getJobIdsForGroup(collection) should not be empty
      }
      // Classification and dense group positions need one job each; stable
      // association ids must not count the cached query rows a third time.
      context.statusTracker.getJobIdsForGroup(preparation).length shouldBe
        (if (held) 0 else 2)
      rows shouldBe bruteForce(k, outer)
      metrics(SearchMetrics.Candidates).value shouldBe
        queryVectors.size.toLong * 4L * k
    }
  }
  test(
    "wide returned rows bound scan blocks by actual bytes without changing top-k"
  ) {
    assume(available, "the native libraries are not on this machine")
    val input = spark.sparkContext.parallelize(0 until 1280, 1).map { i =>
      new GenericInternalRow(
        Array[Any](
          i.toLong,
          if (i == 1 || i == 1278) null else vector(i.toFloat, 0f),
          UTF8String.fromString("x" * 65536)
        )
      ): InternalRow
    }
    val metrics = NearestByMetrics.create(spark.sparkContext)
    val result = NearestBySearch
      .run(
        spark,
        NearestByQueryRows.Held(
          Seq(0f, 514f, 1279f).map { value =>
            new GenericInternalRow(Array[Any](vector(value, 0f))): InternalRow
          }.toArray
        ),
        Seq(vectorType),
        BoundReference(0, vectorType, nullable = true),
        NearestByInput.Frame(
          FrameSearch.Base(
            input,
            Seq(
              BoundReference(0, LongType, nullable = false),
              BoundReference(2, StringType, nullable = false)
            ),
            BoundReference(1, vectorType, nullable = true)
          )
        ),
        outer = false,
        approx = false,
        k = 1,
        metric = MetricType.L2,
        outputTypes = Seq(vectorType, LongType, StringType),
        function = ConnectorVectorRanking(
          MetricType.L2,
          "vector_l2_distance",
          queryFirst = true
        ),
        bySpark = _ => fail("no query row is Spark's"),
        metrics = metrics
      )
      .map(row =>
        (
          row.getArray(0).getFloat(0),
          row.getLong(1),
          row.getUTF8String(2).numBytes()
        )
      )
      .collect()
    // Keep an early candidate, clear a NULL slot when the buffer is reused,
    // and retain the last row from a shorter final batch containing a NULL.
    result.toSeq.sorted shouldBe Seq(0, 514, 1279).map(id =>
      (id.toFloat, id.toLong, 65536)
    )
    metrics(SearchMetrics.KnowhereCalls).value should be >= 3L
  }

}

object FrameSearchTest {

  def vector(values: Float*): GenericArrayData =
    new GenericArrayData(values.map(Float.box).toArray[Any])

  def row(entry: (Long, Array[Float])): InternalRow =
    new GenericInternalRow(Array[Any](entry._1, vector(entry._2: _*)))
}
