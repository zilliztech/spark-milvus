package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

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

  test("no candidates give no rows") {
    MilvusSearch
      .merged(candidates(Seq.empty), 3, "L2")
      .collect() shouldBe empty
  }
}
