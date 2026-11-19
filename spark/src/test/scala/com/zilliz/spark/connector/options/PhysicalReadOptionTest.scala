package com.zilliz.spark.connector.options

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The options of a physical read and the column write. */
class PhysicalReadOptionTest extends AnyFunSuite with Matchers {

  test(
    "the default mode reads live rows and physical mode reads all stored rows"
  ) {
    MilvusOption.isPhysicalRead(Map.empty[String, String]) shouldBe false
    MilvusOption.isWriteResultRead(Map.empty[String, String]) shouldBe false
    MilvusOption.isPhysicalRead(
      Map(MilvusOption.ReadMode -> " PHYSICAL ")
    ) shouldBe true
  }

  test("an unknown read mode is refused with the supported ones") {
    val error = intercept[IllegalArgumentException](
      MilvusOption.isPhysicalRead(Map(MilvusOption.ReadMode -> "deleted"))
    )
    error.getMessage should (include("physical") and include("write_result"))
  }

  test("replace_columns is the one write operation") {
    MilvusOption.writeOperation(Map.empty[String, String]) shouldBe None
    MilvusOption.writeOperation(
      Map(MilvusOption.WriteOperation -> "Replace_Columns")
    ) shouldBe Some(MilvusOption.WriteOperationReplaceColumns)
    val error = intercept[IllegalArgumentException](
      MilvusOption.writeOperation(Map(MilvusOption.WriteOperation -> "append"))
    )
    error.getMessage should include("replace_columns")
  }
}
