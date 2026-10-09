package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QuerySetSearchJobTest extends AnyFunSuite with Matchers {

  private def outputs(args: String*): Seq[String] =
    QuerySetSearchJob.outputColumns(QuerySetSearchJob.parse(args.toArray), "id")

  test("without --output-columns each hit carries the primary key") {
    outputs() shouldBe Seq("id")
    QuerySetSearchJob.outputColumns(
      QuerySetSearchJob.parse(Array.empty),
      "pk"
    ) shouldBe Seq("pk")
  }

  test("--output-columns lists the columns in order, and an empty value none") {
    outputs("--output-columns", "id, labels,emb") shouldBe
      Seq("id", "labels", "emb")
    outputs("--output-columns", "labels") shouldBe Seq("labels")
    outputs("--output-columns", "") shouldBe empty
  }

  test("no other column may take the name the primary key is written as") {
    intercept[IllegalArgumentException] {
      QuerySetSearchJob.outputColumns(
        QuerySetSearchJob.parse(Array("--output-columns", "pk,id")),
        "pk"
      )
    }
  }

  test("a filter is one argument even when it holds spaces and quotes") {
    QuerySetSearchJob
      .parse(Array("--filter", "labels == \"label_1p\""))
      .get("filter") shouldBe Some("labels == \"label_1p\"")
  }
}
