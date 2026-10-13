package com.zilliz.spark.connector.options

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SearchLimitsTest extends AnyFunSuite with Matchers {

  test("calls in flight default to 2, take the option, and never exceed 8") {
    SearchLimits.Default.callsInFlight shouldBe 2
    SearchLimits.from(Map.empty).callsInFlight shouldBe 2
    SearchLimits
      .from(Map("milvus.search.calls.in.flight" -> "1"))
      .callsInFlight shouldBe 1
    SearchLimits
      .from(Map("MILVUS.SEARCH.CALLS.IN.FLIGHT" -> "3"))
      .callsInFlight shouldBe 3
    SearchLimits
      .from(Map("milvus.search.calls.in.flight" -> "99"))
      .callsInFlight shouldBe 8
    an[IllegalArgumentException] should be thrownBy
      SearchLimits.from(Map("milvus.search.calls.in.flight" -> "0"))
  }

  test("the collect threads and query ranges default to the planner's choice") {
    val limits = SearchLimits.from(Map.empty)
    limits.collectThreads shouldBe 0
    limits.queryRanges shouldBe 0
  }

  test("the collect threads and query ranges are read as counts") {
    val limits = SearchLimits.from(
      Map(
        MilvusOption.SearchCollectThreads -> "4",
        MilvusOption.SearchQueryRanges -> "2"
      )
    )
    limits.collectThreads shouldBe 4
    limits.queryRanges shouldBe 2
    an[IllegalArgumentException] should be thrownBy SearchLimits.from(
      Map(MilvusOption.SearchCollectThreads -> "-1")
    )
    an[IllegalArgumentException] should be thrownBy SearchLimits.from(
      Map(MilvusOption.SearchQueryRanges -> "two")
    )
  }
}
