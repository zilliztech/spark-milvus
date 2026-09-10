package com.zilliz.spark.connector.operations.backfill

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The top-level `partitionId` is consumed by Milvus `CommitBackfillResult`,
  * which only treats `0` as "no partition restriction". Any other value is
  * compared against every segment's partition.
  */
class BackfillPartitionIdTest extends AnyFunSuite with Matchers {

  test("single partition reports that partition ID") {
    MilvusBackfill.resolveResultPartitionId(
      Set(450000000002L)
    ) shouldBe 450000000002L
  }

  test("segments spanning several partitions report 0, not -1") {
    MilvusBackfill.resolveResultPartitionId(
      Set(450000000002L, 450000000003L, 450000000004L)
    ) shouldBe 0L
  }

  test("no partitions reports 0") {
    MilvusBackfill.resolveResultPartitionId(Set.empty) shouldBe 0L
  }

  test("result JSON carries 0 for a multi-partition backfill") {
    val segment = SegmentBackfillResult(
      segmentId = 10L,
      rowCount = 3L,
      manifestPaths = Seq("s3a://bucket/manifest"),
      executionTimeMs = 1L,
      outputPath = "s3a://bucket/output"
    )
    val result = BackfillResult.success(
      segmentResults = Map(10L -> segment),
      executionTimeMs = 1L,
      collectionId = 1L,
      partitionId = MilvusBackfill.resolveResultPartitionId(Set(2L, 3L)),
      schemaVersion = 1,
      newFieldNames = Seq("f")
    )
    val json = new ObjectMapper().readTree(result.toJson)
    json.get("partitionId").asLong() shouldBe 0L
  }
}
