package com.zilliz.milvus.storage.read.exec

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.plan.ReadLimits

class PhysicalColumnGroupTest extends AnyFunSuite with Matchers {

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
    "physical reads reject empty sources and negative expected counts before native access"
  ) {
    def open(groups: Seq[PhysicalColumnGroup], rows: Option[Long]): Unit = {
      SegmentReader.openColumnGroups(
        groups,
        null,
        Seq("id"),
        Map.empty,
        ReadLimits.Default,
        rows,
        7L,
        null
      )
      ()
    }
    intercept[IllegalArgumentException](open(Seq.empty, None))
    intercept[IllegalArgumentException] {
      open(
        Seq(PhysicalColumnGroup(Seq("id"), Seq("part.parquet"), Seq(1L))),
        Some(-1L)
      )
    }
  }
}
