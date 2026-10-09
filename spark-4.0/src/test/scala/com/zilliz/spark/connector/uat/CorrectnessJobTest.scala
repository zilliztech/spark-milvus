package com.zilliz.spark.connector.uat

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.uat.CorrectnessJob.{Case, Result}

class CorrectnessJobTest extends AnyFunSuite with Matchers {
  private val selected = Seq(
    Case("R-01", "read", "first", () => ()),
    Case("R-02", "read", "second", () => ())
  )
  private val passed =
    selected.map(c => Result(c.id, c.group, c.title, "PASS", "", 0.0))

  test("only a nonempty, complete set of PASS results succeeds") {
    CorrectnessJob.exitCode(selected, passed) shouldBe 0
    CorrectnessJob.exitCode(Seq.empty, Seq.empty) shouldBe 1
    CorrectnessJob.exitCode(selected, passed.take(1)) shouldBe 1
    CorrectnessJob.exitCode(selected, Seq(passed.head, passed.head)) shouldBe 1
    Seq("FAIL", "SKIP", "UNKNOWN").foreach { status =>
      CorrectnessJob.exitCode(
        selected,
        passed.updated(1, passed(1).copy(status = status))
      ) shouldBe 1
    }
  }

  test("selection rejects unknown IDs even when valid IDs were also supplied") {
    Seq("R-01,not-a-case", "not-a-case", "", "R-01,", "R-01,R-01").foreach {
      ids =>
        intercept[IllegalArgumentException] {
          CorrectnessJob.selectCases(Map("cases" -> ids))
        }
    }
    intercept[IllegalArgumentException] {
      CorrectnessJob.selectCases(Map("group" -> "not-a-group"))
    }
  }

  test("explicit cases override the group and execute the complete selection") {
    CorrectnessJob
      .selectCases(Map("cases" -> "R-01,R-02", "group" -> "search"))
      .map(_.id) shouldBe Seq("R-01", "R-02")
    val read = CorrectnessJob.selectCases(Map("group" -> "read"))
    read should not be empty
    all(read.map(_.group)) shouldBe "read"
    val allCases = CorrectnessJob.selectCases(Map.empty)
    allCases.map(_.id).distinct.size shouldBe allCases.size
  }

  test("malformed arguments cannot silently select a different test set") {
    Seq(
      Array("--cases"),
      Array("--case", "R-01"),
      Array("--cases", "R-01", "--cases", "R-02")
    ).foreach { args =>
      intercept[IllegalArgumentException](CorrectnessJob.parseArguments(args))
    }
    CorrectnessJob.parseArguments(
      Array("--cases", "R-01", "--results", "local")
    ) shouldBe
      Map("cases" -> "R-01", "results" -> "local")
  }
}
