package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SearchModeTest extends AnyFunSuite with Matchers {

  test("a mode is read by its name in any letter case") {
    SearchMode.fromName("index") shouldBe Some(SearchMode.Index)
    SearchMode.fromName("INDEX") shouldBe Some(SearchMode.Index)
    SearchMode.fromName("exact") shouldBe Some(SearchMode.Exact)
    SearchMode.fromName("Exact") shouldBe Some(SearchMode.Exact)
  }

  test("a mode the search does not run in is no mode") {
    Seq("ann", "", " index", null).foreach { name =>
      SearchMode.fromName(name) shouldBe None
    }
  }

  test("a mode reads back as the name a call gives") {
    Seq(SearchMode.Index, SearchMode.Exact).foreach { mode =>
      mode.toString shouldBe mode.name
      SearchMode.fromName(mode.name) shouldBe Some(mode)
    }
  }
}
