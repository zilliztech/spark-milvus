package com.zilliz.milvus.storage.delete

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.snapshot.Segment
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup

class DeltaLogReaderTest extends AnyFunSuite with Matchers {
  test("mergeInheritedDeletePlans keeps L0 deletes within the same partition") {
    val dataSegments = Seq(
      Segment.v2(
        id = 2L,
        partitionId = 10L,
        rows = 100L,
        columnGroups = Seq(
          V2ColumnGroup(Seq(100L, 1L), Seq("s3a://bucket/data-p10"), Seq(100L))
        )
      ),
      Segment.v2(
        id = 3L,
        partitionId = 11L,
        rows = 100L,
        columnGroups = Seq(
          V2ColumnGroup(Seq(100L, 1L), Seq("s3a://bucket/data-p11"), Seq(100L))
        )
      )
    )

    val inheritedPlansByPartition = Map(
      10L -> DeletePlan.fromLongPks(Map(7L -> 100L))
    )
    val ownPlansBySegment = Map(
      2L -> DeletePlan.fromLongPks(Map(9L -> 200L)),
      3L -> DeletePlan.empty
    )

    val merged = DeltaLogReader.mergeInheritedDeletePlans(
      dataSegments,
      inheritedPlansByPartition,
      ownPlansBySegment
    )

    merged(2L).containsLongPk(7L, 50L) shouldBe true
    merged(2L).containsLongPk(9L, 150L) shouldBe true
    merged(3L).containsLongPk(7L, 50L) shouldBe false
  }

  test("delete plan union keeps the latest delete timestamp per PK") {
    val merged = DeletePlan.union(
      DeletePlan.fromLongPks(Map(7L -> 100L)),
      DeletePlan.fromLongPks(Map(7L -> 200L, 8L -> 150L))
    )

    merged.containsLongPk(7L, 150L) shouldBe true
    merged.containsLongPk(7L, 250L) shouldBe false
    merged.containsLongPk(8L, 140L) shouldBe true
  }

  test("collection-wide L0 delete plan applies to every partition") {
    val inheritedPlansByPartition = Map(
      -1L -> DeletePlan.fromLongPks(Map(7L -> 100L)),
      10L -> DeletePlan.fromLongPks(Map(9L -> 200L))
    )

    val partition10Plan = DeltaLogReader.effectiveInheritedDeletePlan(
      10L,
      inheritedPlansByPartition
    )
    val partition11Plan = DeltaLogReader.effectiveInheritedDeletePlan(
      11L,
      inheritedPlansByPartition
    )

    partition10Plan.containsLongPk(7L, 50L) shouldBe true
    partition10Plan.containsLongPk(9L, 150L) shouldBe true
    partition11Plan.containsLongPk(7L, 50L) shouldBe true
    partition11Plan.containsLongPk(9L, 150L) shouldBe false
    DeltaLogReader.inheritedDeletePlanPartitionMarker(
      11L,
      inheritedPlansByPartition
    ) shouldBe Some(11L)
  }

  test("a V3 segment's bare-parquet delete file is read by its header") {
    import com.zilliz.milvus.storage.io.LocalObjectStore
    import com.zilliz.milvus.storage.snapshot.DeltaLogFile
    import io.milvus.grpc.schema.{DataType, FieldSchema}

    // The file Milvus wrote under {segment}/_delta/ on the UAT instance once
    // it folded an L0 delete of ids 0..9 into a V3 segment: a plain parquet
    // file with a pk and a ts column and no event header.
    val plan = DeltaLogReader
      .loadDeletePlan(
        Seq(
          DeltaLogFile(
            469093182140298464L,
            "v3-delta-469093182140298464.parquet",
            10L
          )
        ),
        FieldSchema(
          fieldID = 100,
          name = "id",
          dataType = DataType.Int64,
          isPrimaryKey = true
        ),
        "",
        new LocalObjectStore("core/src/test/data")
      )
      .fold(e => throw e, identity)
    val deleteTs = 469093213902995459L
    (0L until 10L).foreach { id =>
      plan.containsLongPk(
        id,
        deleteTs + 1
      ) shouldBe false // deleted after the row
      plan.containsLongPk(id, deleteTs - 1) shouldBe true
    }
    plan.containsLongPk(10L, deleteTs - 1) shouldBe false
  }
}
