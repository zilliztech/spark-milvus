package com.zilliz.spark.connector.operations.backfill

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BackfillResultTest extends AnyFunSuite with Matchers {

  test("result JSON contains snapshot collection schema version") {
    val segment = SegmentBackfillResult(
      segmentId = 10L,
      rowCount = 3L,
      manifestPaths = Seq("s3a://bucket/manifest/1"),
      outputPath = "s3a://bucket/segment/10",
      executionTimeMs = 5L,
      committedVersion = 1L,
      sourceRowCount = 3L,
      matchedRowCount = 2L
    )
    val result = BackfillResult.success(
      segmentResults = Map(10L -> segment),
      executionTimeMs = 10L,
      collectionId = 1L,
      partitionId = 2L,
      schemaVersion = 7,
      newFieldNames = Seq("embedding")
    )

    val json = new ObjectMapper().readTree(result.toJson)

    json.get("schemaVersion").asInt() shouldBe 7
  }

  test("summary does not report a data-file rate for repeated source keys") {
    val segment = SegmentBackfillResult(
      segmentId = 10L,
      rowCount = 100L,
      manifestPaths = Seq.empty,
      outputPath = "s3a://bucket/segment/10",
      executionTimeMs = 5L,
      sourceRowCount = 100L,
      matchedRowCount = 100L
    )
    val result = BackfillResult.success(
      segmentResults = Map(10L -> segment),
      executionTimeMs = 10L,
      collectionId = 1L,
      partitionId = 2L,
      schemaVersion = 7,
      newFieldNames = Seq("value"),
      totalBackfillDataRows = 1L
    )

    result.summary should include(
      "Total Matched Source Rows: 100 (100.00% of source rows)"
    )
    result.summary should not include "10000.00%"
    result.summary should not include "of data file"
  }
}
