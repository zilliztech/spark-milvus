package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MilvusDeltaLogReaderTest extends AnyFunSuite with Matchers {
  test("mergeInheritedDeletePlans keeps L0 deletes within the same partition") {
    val dataSegments = Seq(
      V2SegmentInfo(
        segmentId = 2L,
        partitionId = 10L,
        numOfRows = 100L,
        storageVersion = 2L,
        columnGroups = Seq(
          V2ColumnGroup(Seq(100L, 1L), Seq("s3a://bucket/data-p10"), Seq(100L))
        )
      ),
      V2SegmentInfo(
        segmentId = 3L,
        partitionId = 11L,
        numOfRows = 100L,
        storageVersion = 2L,
        columnGroups = Seq(
          V2ColumnGroup(Seq(100L, 1L), Seq("s3a://bucket/data-p11"), Seq(100L))
        )
      )
    )

    val inheritedPlansByPartition = Map(
      10L -> MilvusDeletePlan.fromLongPks(Map(7L -> 100L))
    )
    val ownPlansBySegment = Map(
      2L -> MilvusDeletePlan.fromLongPks(Map(9L -> 200L)),
      3L -> MilvusDeletePlan.empty
    )

    val merged = MilvusDeltaLogReader.mergeInheritedDeletePlans(
      dataSegments,
      inheritedPlansByPartition,
      ownPlansBySegment
    )

    merged(2L).containsLongPk(7L, 50L) shouldBe true
    merged(2L).containsLongPk(9L, 150L) shouldBe true
    merged(3L).containsLongPk(7L, 50L) shouldBe false
  }

  test("delete plan union keeps the latest delete timestamp per PK") {
    val merged = MilvusDeletePlan.union(
      MilvusDeletePlan.fromLongPks(Map(7L -> 100L)),
      MilvusDeletePlan.fromLongPks(Map(7L -> 200L, 8L -> 150L))
    )

    merged.containsLongPk(7L, 150L) shouldBe true
    merged.containsLongPk(7L, 250L) shouldBe false
    merged.containsLongPk(8L, 140L) shouldBe true
  }

  test("collection-wide L0 delete plan applies to every partition") {
    val inheritedPlansByPartition = Map(
      -1L -> MilvusDeletePlan.fromLongPks(Map(7L -> 100L)),
      10L -> MilvusDeletePlan.fromLongPks(Map(9L -> 200L))
    )

    val partition10Plan = MilvusDeltaLogReader.effectiveInheritedDeletePlan(
      10L,
      inheritedPlansByPartition
    )
    val partition11Plan = MilvusDeltaLogReader.effectiveInheritedDeletePlan(
      11L,
      inheritedPlansByPartition
    )

    partition10Plan.containsLongPk(7L, 50L) shouldBe true
    partition10Plan.containsLongPk(9L, 150L) shouldBe true
    partition11Plan.containsLongPk(7L, 50L) shouldBe true
    partition11Plan.containsLongPk(9L, 150L) shouldBe false
    MilvusDeltaLogReader.inheritedDeletePlanPartitionMarker(
      11L,
      inheritedPlansByPartition
    ) shouldBe Some(11L)
  }
}
