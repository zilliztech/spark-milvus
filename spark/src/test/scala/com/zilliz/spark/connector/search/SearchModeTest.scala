package com.zilliz.spark.connector.search

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SearchModeTest extends AnyFunSuite with Matchers {

  test("a mode prints as its name") {
    Seq(SearchMode.PreferIndex, SearchMode.Exact).foreach { mode =>
      mode.toString shouldBe mode.name
    }
  }
}
