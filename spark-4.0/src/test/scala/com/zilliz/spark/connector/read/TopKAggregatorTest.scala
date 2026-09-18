package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The merge stage: one query's candidates become its global top-k. */
class TopKAggregatorTest extends AnyFunSuite with Matchers {

  private def merged(
      aggregator: TopKAggregator,
      candidates: Seq[SearchCandidate]*
  ): Seq[SearchHit] = {
    val buffers =
      candidates.map(part => part.foldLeft(aggregator.zero)(aggregator.reduce))
    val merged = buffers.reduce(aggregator.merge)
    aggregator.finish(merged).hits.toSeq
  }

  test("the best k survive, in rank order") {
    val hits = merged(
      new TopKAggregator(2, "L2"),
      Seq(SearchCandidate(1L, 7L, 5.0), SearchCandidate(1L, 8L, 1.0)),
      Seq(SearchCandidate(2L, 3L, 3.0), SearchCandidate(2L, 4L, 9.0))
    )

    hits.map(_.rank) shouldBe Seq(1, 2)
    hits.map(_.score) shouldBe Seq(1.0, 3.0)
    hits.map(_.segmentId) shouldBe Seq(1L, 2L)
    hits.map(_.rowOffset) shouldBe Seq(8L, 3L)
  }

  test("a larger score is the better one for IP and COSINE") {
    val hits = merged(
      new TopKAggregator(2, "COSINE"),
      Seq(
        SearchCandidate(1L, 1L, 0.1),
        SearchCandidate(1L, 2L, 0.9),
        SearchCandidate(1L, 3L, 0.5)
      )
    )

    hits.map(_.score) shouldBe Seq(0.9, 0.5)
  }

  test("equal scores rank by segment, then by row") {
    val hits = merged(
      new TopKAggregator(3, "L2"),
      Seq(SearchCandidate(9L, 2L, 1.0), SearchCandidate(2L, 5L, 1.0)),
      Seq(SearchCandidate(2L, 1L, 1.0))
    )

    hits.map(hit => (hit.segmentId, hit.rowOffset)) shouldBe Seq(
      (2L, 1L),
      (2L, 5L),
      (9L, 2L)
    )
  }

  test("a query with fewer candidates than k keeps them all") {
    val hits = merged(
      new TopKAggregator(10, "L2"),
      Seq(SearchCandidate(1L, 1L, 2.0))
    )

    hits.map(_.rank) shouldBe Seq(1)
  }

  test("merging in either order gives the same answer") {
    val aggregator = new TopKAggregator(2, "L2")
    val left = Seq(SearchCandidate(1L, 1L, 4.0), SearchCandidate(1L, 2L, 2.0))
    val right = Seq(SearchCandidate(2L, 1L, 3.0), SearchCandidate(2L, 2L, 1.0))

    merged(aggregator, left, right) shouldBe merged(aggregator, right, left)
  }

  test("an unsupported metric is refused where the search is built") {
    the[IllegalArgumentException] thrownBy new TopKAggregator(2, "EUCLID").zero
  }
}
