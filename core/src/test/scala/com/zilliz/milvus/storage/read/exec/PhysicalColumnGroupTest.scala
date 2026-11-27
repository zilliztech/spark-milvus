package com.zilliz.milvus.storage.read.exec

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.plan.ReadLimits
import com.zilliz.milvus.storage.snapshot.SegmentLayout

class PhysicalColumnGroupTest extends AnyFunSuite with Matchers {

  private def open(
      groups: Seq[PhysicalColumnGroup],
      rows: Option[Long]
  ): SegmentReader =
    SegmentReader.openColumnGroups(
      groups,
      null,
      Seq("104"),
      Map.empty,
      ReadLimits.Default,
      rows,
      7L,
      null
    )

  private def layout(groups: Seq[PhysicalColumnGroup]) = {
    val fields =
      Seq("103", "104").map(Field.nullable(_, ArrowType.Binary.INSTANCE))
    SegmentLayout.PhysicalColumnGroups(
      groups,
      new Schema(fields.asJava).toJson,
      Map(103L -> "103", 104L -> "104"),
      Map.empty
    )
  }

  test(
    "physical descriptors reject incomplete and overflowing file row counts"
  ) {
    Seq(Seq.empty[Long], Seq(-1L), Seq(1L, 2L)).foreach { counts =>
      intercept[IllegalArgumentException] {
        PhysicalColumnGroup(Seq("id"), Seq("part.parquet"), counts)
      }
    }
    intercept[IllegalArgumentException] {
      PhysicalColumnGroup(
        Seq("id"),
        Seq("first.parquet", "second.parquet"),
        Seq(Long.MaxValue, 1L)
      )
    }
  }

  test(
    "physical descriptors require distinct column names and nonempty file paths"
  ) {
    Seq(Seq.empty[String], Seq(""), Seq("id", "id")).foreach { columns =>
      intercept[IllegalArgumentException] {
        PhysicalColumnGroup(columns, Seq("part.parquet"), Seq(1L))
      }
    }
    intercept[IllegalArgumentException] {
      PhysicalColumnGroup(Seq("id"), Seq(""), Seq(1L))
    }
  }

  test(
    "resolved layouts and direct reads reject a column in two groups in either order"
  ) {
    val old =
      PhysicalColumnGroup(Seq("103", "104"), Seq("old.parquet"), Seq(4L))
    val replacement =
      PhysicalColumnGroup(Seq("104"), Seq("new.parquet"), Seq(4L))
    Seq(Seq(old, replacement), Seq(replacement, old)).foreach { groups =>
      val invalidLayout = intercept[IllegalArgumentException](layout(groups))
      val invalidRead = intercept[IllegalArgumentException](open(groups, None))
      Seq(invalidLayout, invalidRead).foreach { error =>
        error.getMessage should include(
          "'104' belongs to multiple physical column groups"
        )
      }
    }
  }

  test("a column group can span multiple files in physical row order") {
    val paths = Seq("part-a.parquet", "part-b.parquet")
    val split = PhysicalColumnGroup(Seq("104"), paths, Seq(3L, 1L))
    layout(Seq(split)).groups.head.filePaths shouldBe paths
  }

  test(
    "physical reads reject empty sources and negative expected counts before native access"
  ) {
    intercept[IllegalArgumentException](open(Seq.empty, None))
    intercept[IllegalArgumentException] {
      open(
        Seq(PhysicalColumnGroup(Seq("104"), Seq("part.parquet"), Seq(1L))),
        Some(-1L)
      )
    }
  }
}
