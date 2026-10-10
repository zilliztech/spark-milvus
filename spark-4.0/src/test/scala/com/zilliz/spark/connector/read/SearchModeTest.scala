package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SearchModeTest extends AnyFunSuite with Matchers {

  test("a mode prints as its name") {
    Seq(SearchMode.Index, SearchMode.Exact).foreach { mode =>
      mode.toString shouldBe mode.name
    }
  }
}
