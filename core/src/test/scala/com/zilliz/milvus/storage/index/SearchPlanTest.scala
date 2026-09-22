package com.zilliz.milvus.storage.index

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.milvus.storage.snapshot.{
  SegmentIndex,
  SegmentIndexes,
  SegmentLayout
}

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
        slots = 2,
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
        slots = 1,
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
        slots = 1,
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
        slots = 8,
        vectorsMaxBytes = 1L << 31
      )
    ) shouldBe Seq(Seq(1L), Seq(2L))
  }

  private def unsized(segmentId: Long): SegmentReadTask =
    SegmentReadTask(
      segmentId,
      1L,
      SegmentLayout.Manifest("segments/" + segmentId, 3L),
      Array.emptyByteArray,
      Map.empty
    )

  test("segments without a row count still spread over the sets") {
    val tasks = Seq(unsized(1L), unsized(2L))

    segments(
      SearchPlan.segmentSets(
        tasks,
        layout,
        slots = 2,
        vectorsMaxBytes = 1L << 31
      )
    ) shouldBe Seq(Seq(1L), Seq(2L))
  }

  test("a segment without a row count is planned at half the budget") {
    // Four unsized segments at 2000 bytes each fill two sets of 4000; before,
    // they counted as nothing and all four went into one set.
    val tasks = Seq(unsized(1L), unsized(2L), unsized(3L), unsized(4L))

    val sets =
      SearchPlan.segmentSets(
        tasks,
        layout,
        slots = 1,
        vectorsMaxBytes = 4000L
      )

    segments(sets) shouldBe Seq(Seq(1L, 3L), Seq(2L, 4L))
    SearchPlan.retainedBytes(sets.head, layout, 4000L) shouldBe 4000L
    // An unsized segment beside sized ones takes 2000 bytes of room: 2400,
    // 2000 and 1600 bytes need two sets, and the 1600 join the lighter one.
    segments(
      SearchPlan.segmentSets(
        Seq(task(1L, 150L), unsized(2L), task(3L, 100L)),
        layout,
        slots = 1,
        vectorsMaxBytes = 4000L
      )
    ) shouldBe Seq(Seq(1L), Seq(2L, 3L))
  }

  test("the plan names the segments it estimated, and a set's known bytes") {
    val plan = SearchPlan.of(
      Seq(task(1L, 10L), unsized(2L), task(3L, 10L)),
      layout,
      slots = 1,
      queries = 1,
      k = 1,
      groupMaxBytes = 4000L,
      vectorsMaxBytes = 1L << 31
    )

    plan.estimated shouldBe Seq(2L)
    SearchPlan.knownBytes(Seq(task(1L, 10L), task(3L, 10L)), layout) shouldBe
      Some(320L)
    SearchPlan.knownBytes(Seq(task(1L, 10L), unsized(2L)), layout) shouldBe None
    SearchPlan.knownBytes(Seq.empty, layout) shouldBe Some(0L)
  }

  test("a persisted index's row count sizes a segment the snapshot did not") {
    val indexed = unsized(7L).copy(indexes =
      SegmentIndexes.Available(
        Vector(
          SegmentIndex(
            collectionId = 1L,
            partitionId = 1L,
            segmentId = 7L,
            fieldId = 100L,
            indexId = 1L,
            buildId = 1L,
            name = "v",
            parameters = Map("index_type" -> "HNSW"),
            filePaths = Vector("index/7"),
            rowCount = 25L,
            serializedSize = 1L,
            indexVersion = 1L,
            currentIndexVersion = None,
            indexStorePathVersion = None
          )
        )
      )
    )

    SearchPlan.knownRows(indexed) shouldBe Some(25L)
    SearchPlan.knownRows(unsized(8L)) shouldBe None
    SearchPlan.knownBytes(Seq(indexed), layout) shouldBe Some(400L)
  }

  test("a search over no segments plans no tasks") {
    SearchPlan.segmentSets(
      Seq.empty,
      layout,
      slots = 4,
      vectorsMaxBytes = 1L << 31
    ) shouldBe empty
    SearchPlan
      .of(
        Seq.empty,
        layout,
        slots = 4,
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
      slots = 2,
      queries = 3,
      k = 10,
      groupMaxBytes = 992L,
      vectorsMaxBytes = 1L << 31
    )

    plan.tasks shouldBe 2
    plan.groups.map(_.firstQuery) shouldBe Seq(0, 2)
    plan.sets.flatMap(_.map(_.segmentId)).sorted shouldBe Seq(1L, 2L, 3L)
  }

  test(
    "an exact search with fewer sets than slots cuts its groups into ranges"
  ) {
    // 2 sets on 16 slots: 8 ranges of 2 groups each, 16 tasks.
    SearchPlan.queryRanges(
      groups = 16,
      sets = 2,
      slots = 16,
      split = true
    ) shouldBe
      (0 until 8).map(range => (range * 2) until (range * 2 + 2))
    // Never more ranges than groups.
    SearchPlan.queryRanges(
      groups = 3,
      sets = 2,
      slots = 16,
      split = true
    ) shouldBe
      Seq(0 until 1, 1 until 2, 2 until 3)
    // Enough sets already: one range.
    SearchPlan.queryRanges(
      groups = 16,
      sets = 16,
      slots = 16,
      split = true
    ) shouldBe
      Seq(0 until 16)
    // Uneven counts put the longer ranges first.
    SearchPlan.queryRanges(
      groups = 5,
      sets = 1,
      slots = 3,
      split = true
    ) shouldBe
      Seq(0 until 2, 2 until 4, 4 until 5)
  }

  test("an index search keeps every group in one range") {
    SearchPlan.queryRanges(
      groups = 16,
      sets = 2,
      slots = 16,
      split = false
    ) shouldBe
      Seq(0 until 16)
  }

  test(
    "the tasks are the sets times the ranges, and the ranges cover the groups"
  ) {
    val tasks = Seq(task(1L, 10L), task(2L, 10L))
    val plan = SearchPlan.of(
      tasks,
      layout,
      slots = 8,
      queries = 8,
      k = 10,
      groupMaxBytes = 992L,
      vectorsMaxBytes = 1L << 31,
      splitQueries = true
    )
    plan.sets.size shouldBe 2
    plan.groups.size shouldBe 4
    plan.queryRanges shouldBe Seq(0 until 1, 1 until 2, 2 until 3, 3 until 4)
    plan.tasks shouldBe 8
    an[IllegalArgumentException] should be thrownBy
      plan.copy(ranges = Seq(0 until 2, 3 until 4))
  }
}
