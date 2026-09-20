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
    // 16 bytes of vector and 480 of candidates make 496 bytes a query.
    SearchPlan.bytesPerQuery(layout, k = 10) shouldBe 496L
    SearchPlan.queriesPerGroup(
      layout,
      k = 10,
      groupMaxBytes = 4960L
    ) shouldBe 10
    SearchPlan.queriesPerGroup(layout, k = 10, groupMaxBytes = 496L) shouldBe 1
  }

  test("the query matrix counts, not only the candidates") {
    val wide = VectorLayout(VectorElementType.Float32, 768)

    SearchPlan.bytesPerQuery(wide, k = 10) shouldBe 3072L + 480L
    SearchPlan.queriesPerGroup(
      wide,
      k = 10,
      groupMaxBytes = 536870912L
    ) shouldBe 536870912L / 3552L
  }

  test("a query that does not fit a group on its own is refused") {
    val failure = the[IllegalArgumentException] thrownBy SearchPlan
      .queriesPerGroup(layout, k = 101, groupMaxBytes = 2800L)

    failure.getMessage should include("over the group limit")
  }

  test("queries are cut into groups in query order, the last one shorter") {
    val groups =
      SearchPlan.groups(queries = 25, layout, k = 10, groupMaxBytes = 4960L)

    groups.map(group => (group.firstQuery, group.queries)) shouldBe Seq(
      (0, 10),
      (10, 10),
      (20, 5)
    )
    groups.last.untilQuery shouldBe 25
  }

  test("one group covers a query set that fits") {
    SearchPlan.groups(
      queries = 4,
      layout,
      k = 10,
      groupMaxBytes = 2960L
    ) shouldBe Seq(SearchPlan.QueryGroup(0, 4))
  }

  test("a task keeps the segment set the planner sized for it") {
    val set = Seq(task(1L, 100L), task(2L, 250L))

    SearchPlan.retainedBytes(set, layout, 1L << 31) shouldBe 350L * 16L
    SearchPlan.retainedBytes(set, layout, 1000L) shouldBe 1000L
    SearchPlan.retainedBytes(Seq.empty, layout, 1000L) shouldBe 16L
  }

  test("segments are balanced over the sets by the bytes they hold") {
    val tasks = Seq(
      task(1L, 100L),
      task(2L, 10L),
      task(3L, 60L),
      task(4L, 30L)
    )

    val sets =
      SearchPlan.segmentSets(
        tasks,
        layout,
        executors = 2,
        vectorsMaxBytes = 1L << 31
      )

    segments(sets) shouldBe Seq(Seq(1L), Seq(2L, 3L, 4L))
  }

  test("a set holds no more vectors than a task keeps") {
    val tasks = Seq(
      task(1L, 100L),
      task(2L, 100L),
      task(3L, 100L),
      task(4L, 100L)
    )

    // 100 rows of 16 bytes make 1600 bytes a segment, so 6400 bytes of vectors
    // need two sets of 4000 bytes, and equal segments alternate between them.
    val sets =
      SearchPlan.segmentSets(
        tasks,
        layout,
        executors = 1,
        vectorsMaxBytes = 4000L
      )

    segments(sets) shouldBe Seq(Seq(1L, 3L), Seq(2L, 4L))
    sets.foreach(set =>
      SearchPlan.retainedBytes(set, layout, 4000L) should be <= 4000L
    )
  }

  test("a segment larger than the retained limit is a set of its own") {
    val tasks = Seq(task(1L, 500L), task(2L, 10L))

    val sets =
      SearchPlan.segmentSets(
        tasks,
        layout,
        executors = 1,
        vectorsMaxBytes = 4000L
      )

    segments(sets) shouldBe Seq(Seq(1L), Seq(2L))
  }

  test(
    "a set holds at most one segment when there are more sets than segments"
  ) {
    val tasks = Seq(task(1L, 10L), task(2L, 10L))

    segments(
      SearchPlan.segmentSets(
        tasks,
        layout,
        executors = 8,
        vectorsMaxBytes = 1L << 31
      )
    ) shouldBe Seq(Seq(1L), Seq(2L))
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

    segments(
      SearchPlan.segmentSets(
        tasks,
        layout,
        executors = 2,
        vectorsMaxBytes = 1L << 31
      )
    ) shouldBe Seq(Seq(1L), Seq(2L))
  }

  test("a search over no segments plans no tasks") {
    SearchPlan.segmentSets(
      Seq.empty,
      layout,
      executors = 4,
      vectorsMaxBytes = 1L << 31
    ) shouldBe empty
    SearchPlan
      .of(
        Seq.empty,
        layout,
        executors = 4,
        queries = 2,
        k = 5,
        groupMaxBytes = 2800L,
        vectorsMaxBytes = 1L << 31
      )
      .isEmpty shouldBe true
  }

  test("every segment set answers every query group") {
    val tasks = Seq(task(1L, 10L), task(2L, 10L), task(3L, 10L))

    val plan = SearchPlan.of(
      tasks,
      layout,
      executors = 2,
      queries = 3,
      k = 10,
      groupMaxBytes = 992L,
      vectorsMaxBytes = 1L << 31
    )

    plan.tasks shouldBe 2
    plan.groups.map(_.firstQuery) shouldBe Seq(0, 2)
    plan.sets.flatMap(_.map(_.segmentId)).sorted shouldBe Seq(1L, 2L, 3L)
  }
}
