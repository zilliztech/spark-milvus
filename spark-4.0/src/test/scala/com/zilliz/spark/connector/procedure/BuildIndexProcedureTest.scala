package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What `CALL milvus.system.build_index(...)` takes and answers with
  * (docs/design/architecture/vector-search.html section 2.7).
  */
class BuildIndexProcedureTest extends AnyFunSuite with Matchers {

  test("the call names its table, field and where the objects go") {
    val required =
      BuildIndexProcedure.parameters.filter(_.required).map(_.name)
    required shouldBe Seq("field", "output")
    // `collection` stays first and `table` comes last, so a call that passes
    // the collection by position still binds.
    BuildIndexProcedure.parameters
      .filterNot(_.required)
      .map(_.name) shouldBe Seq(
      "collection",
      "index_type",
      "metric",
      "params",
      "build_id",
      "index_version",
      "store_path_version",
      "table"
    )
    BuildIndexProcedure.parameters.head.name shouldBe "collection"
    BuildIndexProcedure.parameters
      .find(_.name == "build_id")
      .map(_.dataType) shouldBe Some(LongType)
    BuildIndexProcedure.parameters
      .find(_.name == "field")
      .map(_.dataType) shouldBe Some(StringType)
  }

  test(
    "a call names the table or the collection, and options only with a collection"
  ) {
    def tableName(values: Map[String, Any], options: Map[String, String]) =
      ProcedureSupport.tableName(ProcedureArgs(values, options), "build_index")

    tableName(Map("collection" -> "c"), Map("milvus.uri" -> "u")) shouldBe None
    tableName(Map("table" -> "milvus.db.docs"), Map.empty) shouldBe
      Some("milvus.db.docs")
    the[IllegalArgumentException] thrownBy tableName(
      Map("collection" -> "c", "table" -> "t"),
      Map.empty
    ) should have message
      "procedure build_index: give 'collection' or 'table', not both"
    the[IllegalArgumentException] thrownBy tableName(
      Map.empty,
      Map.empty
    ) should have message "procedure build_index: give 'collection' or 'table'"
    the[IllegalArgumentException] thrownBy tableName(
      Map("table" -> "t"),
      Map("milvus.uri" -> "u", "fs.bucket_name" -> "b")
    ) should have message
      "procedure build_index: a table is reached through the options it was read with, " +
      "so a call with 'table' takes none; got fs.bucket_name, milvus.uri"
  }

  test("a row says what one segment's index cost") {
    BuildIndexProcedure.outputSchema.fieldNames.toSeq shouldBe Seq(
      "segment_id",
      "partition_id",
      "row_count",
      "objects",
      "bytes",
      "build_id",
      "job_id"
    )
  }

  test("tuning arrives as name=value pairs") {
    BuildIndexProcedure.parametersOf(None) shouldBe Map.empty
    BuildIndexProcedure.parametersOf(Some("  ")) shouldBe Map.empty
    BuildIndexProcedure.parametersOf(
      Some("M=16, efConstruction=200")
    ) shouldBe Map("M" -> "16", "efConstruction" -> "200")
    BuildIndexProcedure.parametersOf(Some("sq_type=SQ4U,refine=true")) shouldBe
      Map("sq_type" -> "SQ4U", "refine" -> "true")
  }

  test("a malformed parameter list is refused") {
    Seq("M", "M=", "=16", "M=16=32").foreach { bad =>
      the[IllegalArgumentException] thrownBy BuildIndexProcedure.parametersOf(
        Some(bad)
      )
    }
  }
}
