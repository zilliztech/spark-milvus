package com.zilliz.milvus.storage.index

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.milvus.storage.snapshot.SegmentLayout

/** How one search becomes first-stage tasks: vector-search.html section 2.1. */
class SearchPlanTest extends AnyFunSuite with Matchers {

  private val layout = VectorLayout(VectorElementType.Float32, 4)

  private def task(segmentId: Long, rows: Long): SegmentReadTask =
    SegmentReadTask(
      segmentId = segmentId,
      partitionId = 1L,
      layout = SegmentLayout.Manifest("segments/" + segmentId, 3L),
      schemaBytes = Array.emptyByteArray,
      properties = Map.empty,
      snapshotRows = Some(rows)
    )

  private def segments(sets: Seq[Seq[SegmentReadTask]]): Seq[Seq[Long]] =
    sets.map(_.map(_.segmentId))

  test("a group holds as many queries as the byte limit allows") {
    SearchPlan.queriesPerGroup(k = 10, groupMaxBytes = 2800L) shouldBe 10
    SearchPlan.queriesPerGroup(k = 100, groupMaxBytes = 2800L) shouldBe 1
    SearchPlan.queriesPerGroup(k = 3, groupMaxBytes = 536870912L) shouldBe
      536870912L / (3 * SearchPlan.CandidateBytes)
  }

  test("a query whose own candidates exceed the limit is refused") {
    val failure = the[IllegalArgumentException] thrownBy SearchPlan
      .queriesPerGroup(k = 101, groupMaxBytes = 2800L)

    failure.getMessage should include("over the group limit")
  }

  test("queries are cut into groups in query order, the last one shorter") {
    val groups = SearchPlan.groups(queries = 25, k = 10, groupMaxBytes = 2800L)

    groups.map(group => (group.firstQuery, group.queries)) shouldBe Seq(
      (0, 10),
      (10, 10),
      (20, 5)
    )
    groups.last.untilQuery shouldBe 25
  }

  test("one group covers a query set that fits") {
    SearchPlan.groups(queries = 4, k = 10, groupMaxBytes = 2800L) shouldBe Seq(
      SearchPlan.QueryGroup(0, 4)
    )
  }

  test("segments are balanced over the sets by the bytes they hold") {
    val tasks = Seq(
      task(1L, 100L),
      task(2L, 10L),
      task(3L, 60L),
      task(4L, 30L)
    )

    val sets = SearchPlan.segmentSets(tasks, layout, sets = 2)

    segments(sets) shouldBe Seq(Seq(1L), Seq(2L, 3L, 4L))
  }

  test(
    "a set holds at most one segment when there are more sets than segments"
  ) {
    val tasks = Seq(task(1L, 10L), task(2L, 10L))

    segments(SearchPlan.segmentSets(tasks, layout, sets = 8)) shouldBe Seq(
      Seq(1L),
      Seq(2L)
    )
  }

  test("segments without a row count still spread over the sets") {
    val tasks = Seq(
      SegmentReadTask(
        1L,
        1L,
        SegmentLayout.Manifest("segments/1", 3L),
        Array.emptyByteArray,
        Map.empty
      ),
      SegmentReadTask(
        2L,
        1L,
        SegmentLayout.Manifest("segments/2", 3L),
        Array.emptyByteArray,
        Map.empty
      )
    )

    segments(SearchPlan.segmentSets(tasks, layout, sets = 2)) shouldBe Seq(
      Seq(1L),
      Seq(2L)
    )
  }

  test("a search over no segments plans no tasks") {
    SearchPlan.segmentSets(Seq.empty, layout, sets = 4) shouldBe empty
    SearchPlan.of(
      Seq.empty,
      layout,
      executors = 4,
      queries = 2,
      k = 5,
      groupMaxBytes = 2800L
    ) shouldBe empty
  }

  test("every segment set answers every query group") {
    val tasks = Seq(task(1L, 10L), task(2L, 10L), task(3L, 10L))

    val plan = SearchPlan.of(
      tasks,
      layout,
      executors = 2,
      queries = 3,
      k = 10,
      groupMaxBytes = 560L
    )

    plan should have size 4
    plan.map(_.group.firstQuery) shouldBe Seq(0, 2, 0, 2)
    plan.map(_.segments.map(_.segmentId)).distinct should have size 2
    plan.flatMap(_.segments.map(_.segmentId)).distinct.sorted shouldBe Seq(
      1L,
      2L,
      3L
    )
  }
}
