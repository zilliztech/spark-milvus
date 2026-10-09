package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.{
  SupportsPushDownFilters,
  SupportsPushDownV2Filters
}
import org.scalatest.funsuite.AnyFunSuite

class RowFiltersTest extends AnyFunSuite {
  test("the scan builder exposes only DataSource V2 predicate pushdown") {
    assert(
      classOf[SupportsPushDownV2Filters].isAssignableFrom(
        classOf[MilvusScanBuilder]
      )
    )
    assert(
      !classOf[SupportsPushDownFilters].isAssignableFrom(
        classOf[MilvusScanBuilder]
      )
    )
  }
}
