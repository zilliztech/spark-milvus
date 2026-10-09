package com.zilliz.spark.connector.options

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SearchLimitsTest extends AnyFunSuite with Matchers {

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
