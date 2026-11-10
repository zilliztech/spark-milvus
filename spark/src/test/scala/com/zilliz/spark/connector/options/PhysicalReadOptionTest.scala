package com.zilliz.spark.connector.options

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The options of a physical read, its row identity and the column write. */
class PhysicalReadOptionTest extends AnyFunSuite with Matchers {

  private val physical = Map(MilvusOption.ReadMode -> "physical")

  test("an unset read mode reads live rows and applies deletes") {
    MilvusOption.isPhysicalRead(Map.empty[String, String]) shouldBe false
    MilvusOption.isWriteResultRead(Map.empty[String, String]) shouldBe false
    MilvusOption.readApplyDeletes(Map.empty[String, String]) shouldBe true
  }

  test("a physical read keeps rows marked deleted") {
    MilvusOption.isPhysicalRead(physical) shouldBe true
    MilvusOption.readApplyDeletes(physical) shouldBe false
    MilvusOption.readApplyDeletes(
      physical + (MilvusOption.ReadApplyDeletes -> "false")
    ) shouldBe false
    MilvusOption.isPhysicalRead(
      Map(MilvusOption.ReadMode -> " PHYSICAL ")
    ) shouldBe true
  }

  test("a physical read cannot also apply deletes") {
    val error = intercept[IllegalArgumentException](
      MilvusOption.readApplyDeletes(
        physical + (MilvusOption.ReadApplyDeletes -> "true")
      )
    )
    error.getMessage should include(MilvusOption.ReadApplyDeletes)
  }

  test("an unknown read mode is refused with the supported ones") {
    val error = intercept[IllegalArgumentException](
      MilvusOption.isPhysicalRead(Map(MilvusOption.ReadMode -> "deleted"))
    )
    error.getMessage should (include("physical") and include("write_result"))
  }

  test("the row identity is added after the listed extra columns") {
    val options = physical ++ Map(
      MilvusOption.ReadIncludeRowId -> "true",
      MilvusOption.MilvusExtraColumns -> "_segment_id"
    )
    MilvusOption.extraColumns(options) shouldBe Seq(
      MilvusOption.MilvusExtraColumnSegmentID,
      MilvusOption.MilvusRowIdColumn
    )
    MilvusOption.extraColumns(physical) shouldBe Seq.empty
  }

  test("the row identity requires a physical read") {
    val error = intercept[IllegalArgumentException](
      MilvusOption.extraColumns(Map(MilvusOption.ReadIncludeRowId -> "true"))
    )
    error.getMessage should include(MilvusOption.ReadModePhysical)
  }

  test("the row identity cannot be listed as an extra column") {
    val error = intercept[IllegalArgumentException](
      MilvusOption.extraColumns(
        physical + (MilvusOption.MilvusExtraColumns -> "_milvus_row_id")
      )
    )
    error.getMessage should include(MilvusOption.ReadIncludeRowId)
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
