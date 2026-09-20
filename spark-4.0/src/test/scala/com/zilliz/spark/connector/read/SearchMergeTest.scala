package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.spark.connector.metrics.SearchMetrics

/** The merge stage inside Spark: candidates from several tasks become one top-k
  * per query, with the ranks the result contract promises.
  */
class SearchMergeTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("search-merge")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def candidates(rows: Seq[(Long, Long, Long, Double)]) =
    spark.createDataFrame(
      rows.map { case (query, segment, offset, score) =>
        Row(query, segment, offset, score)
      }.asJava,
      SegmentSetSearch.CandidateSchema
    )

  test("each query keeps its own best k, ranked from one") {
    val merged = MilvusSearch.merged(
      candidates(
        Seq(
          (1L, 10L, 0L, 5.0),
          (1L, 11L, 3L, 1.0),
          (1L, 11L, 4L, 3.0),
          (2L, 10L, 7L, 9.0),
          (2L, 11L, 8L, 2.0)
        )
      ),
      2,
      "L2"
    )

    merged.schema.fieldNames shouldBe Array(
      "query_id",
      "rank",
      "_score",
      "_segment_id",
      "_row_offset"
    )
    val rows = merged
      .orderBy("query_id", "rank")
      .collect()
      .map(row =>
        (
          row.getAs[Long]("query_id"),
          row.getAs[Int]("rank"),
          row.getAs[Double]("_score"),
          row.getAs[Long]("_segment_id"),
          row.getAs[Long]("_row_offset")
        )
      )
      .toSeq

    rows shouldBe Seq(
      (1L, 1, 1.0, 11L, 3L),
      (1L, 2, 3.0, 11L, 4L),
      (2L, 1, 2.0, 11L, 8L),
      (2L, 2, 9.0, 10L, 7L)
    )
  }

  test("a larger score wins under COSINE") {
    val rows = MilvusSearch
      .merged(
        candidates(
          Seq((1L, 10L, 0L, 0.2), (1L, 10L, 1L, 0.9), (1L, 11L, 2L, 0.5))
        ),
        2,
        "COSINE"
      )
      .orderBy("rank")
      .collect()
      .map(row => (row.getAs[Int]("rank"), row.getAs[Double]("_score")))
      .toSeq

    rows shouldBe Seq((1, 0.9), (2, 0.5))
  }

  test("a query with fewer candidates than k keeps them all") {
    val rows = MilvusSearch
      .merged(candidates(Seq((7L, 10L, 0L, 1.5))), 10, "L2")
      .collect()

    rows.map(_.getAs[Long]("query_id")).toSeq shouldBe Seq(7L)
    rows.map(_.getAs[Int]("rank")).toSeq shouldBe Seq(1)
  }

  test("a local master's tasks all share one JVM's memory") {
    // local[2] in beforeAll: two tasks at once, so an executor budget is
    // halved before a task plans against it.
    MilvusSearch.taskSlotsPerExecutor(spark) shouldBe 2
  }

  test("a progress count reads at a glance") {
    SearchProgress.grouped(0L) shouldBe "0"
    SearchProgress.grouped(999L) shouldBe "999"
    SearchProgress.grouped(10000L) shouldBe "10,000"
    SearchProgress.grouped(12060426240L) shouldBe "12,060,426,240"
    SearchProgress.grouped(53687091200L) shouldBe "53,687,091,200"
  }

  test("a search registers the accumulators the design names") {
    val metrics = SearchMetrics.create(spark.sparkContext)

    metrics.all.map(_._1) shouldBe Seq(
      "milvus.search.segment.searches",
      "milvus.search.read.bytes",
      "milvus.search.read.nanos",
      "milvus.search.index.bytes",
      "milvus.search.index.load.nanos",
      "milvus.search.bitmap.nanos",
      "milvus.search.knowhere.calls",
      "milvus.search.knowhere.nanos",
      "milvus.search.compared.pairs",
      "milvus.search.candidates",
      "milvus.search.take.rows",
      "milvus.search.take.nanos"
    )
    metrics.all.foreach { case (name, accumulator) =>
      accumulator.name shouldBe Some(name)
      accumulator.value shouldBe 0L
    }

    spark.sparkContext
      .parallelize(Seq(3L, 4L), 2)
      .foreach(metrics.candidates.add)

    metrics.candidates.value shouldBe 7L
    metrics.summary should include("milvus.search.candidates=7")
  }

  test("a second search counts on its own accumulators") {
    val first = SearchMetrics.create(spark.sparkContext)
    val second = SearchMetrics.create(spark.sparkContext)

    first.segmentSearches.add(2L)

    second.segmentSearches.value shouldBe 0L
  }

  test("no candidates give no rows") {
    MilvusSearch
      .merged(candidates(Seq.empty), 3, "L2")
      .collect() shouldBe empty
  }
}
