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

  /** Vectors scanned in 64-byte blocks: what an exact search's segments cost.
    */
  private val vectors: SegmentReadTask => SearchPlan.Footprint =
    SearchPlan.Footprint.vectors(_, layout, 64L)

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
        slots = 2,
        capacity = 1L << 31,
        size = SearchPlan.vectorBytes(layout, 1L << 31)
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
        slots = 1,
        capacity = 4000L,
        size = SearchPlan.vectorBytes(layout, 4000L)
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
        slots = 1,
        capacity = 4000L,
        size = SearchPlan.vectorBytes(layout, 4000L)
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
        slots = 8,
        capacity = 1L << 31,
        size = SearchPlan.vectorBytes(layout, 1L << 31)
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
        slots = 2,
        capacity = 1L << 31,
        size = SearchPlan.vectorBytes(layout, 1L << 31)
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
        slots = 1,
        capacity = 4000L,
        size = SearchPlan.vectorBytes(layout, 4000L)
      )

    segments(sets) shouldBe Seq(Seq(1L, 3L), Seq(2L, 4L))
    SearchPlan.retainedBytes(sets.head, layout, 4000L) shouldBe 4000L
    // An unsized segment beside sized ones takes 2000 bytes of room: 2400,
    // 2000 and 1600 bytes need two sets, and the 1600 join the lighter one.
    segments(
      SearchPlan.segmentSets(
        Seq(task(1L, 150L), unsized(2L), task(3L, 100L)),
        slots = 1,
        capacity = 4000L,
        size = SearchPlan.vectorBytes(layout, 4000L)
      )
    ) shouldBe Seq(Seq(1L), Seq(2L, 3L))
  }

  test("the plan names the segments it estimated, and a set's known bytes") {
    val plan = SearchPlan.of(
      Seq(task(1L, 10L), unsized(2L), task(3L, 10L)),
      layout,
      concurrency = 1,
      queries = 1,
      k = 1,
      groupMaxBytes = 4000L,
      budget = SearchPlan.Budget(1L << 31, 0L),
      footprint = vectors,
      shuffled = false
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
      slots = 4,
      capacity = 1L << 31,
      size = SearchPlan.vectorBytes(layout, 1L << 31)
    ) shouldBe empty
    SearchPlan
      .of(
        Seq.empty,
        layout,
        concurrency = 4,
        queries = 2,
        k = 5,
        groupMaxBytes = 2800L,
        budget = SearchPlan.Budget(1L << 31, 0L),
        footprint = vectors,
        shuffled = false
      )
      .isEmpty shouldBe true
  }

  test("every segment set answers every query group") {
    val tasks = Seq(task(1L, 10L), task(2L, 10L), task(3L, 10L))

    val plan = SearchPlan.of(
      tasks,
      layout,
      concurrency = 2,
      queries = 3,
      k = 10,
      groupMaxBytes = 992L,
      budget = SearchPlan.Budget(1L << 31, 0L),
      footprint = vectors,
      shuffled = false
    )

    plan.tasks shouldBe 2
    plan.groups.map(_.firstQuery) shouldBe Seq(0, 2)
    plan.sets.flatMap(_.map(_.segmentId)).sorted shouldBe Seq(1L, 2L, 3L)
  }

  test(
    "an exact search with fewer sets than slots cuts ranges down to a floor"
  ) {
    // 2 sets on 16 slots: 8 ranges when the queries allow.
    SearchPlan.rangeCount(
      queries = 349525,
      sets = 2,
      slots = 16,
      split = true
    ) shouldBe 8
    // No range under MinRangeQueries queries: 10,000 queries make 4.
    SearchPlan.rangeCount(
      queries = 10000,
      sets = 2,
      slots = 16,
      split = true
    ) shouldBe 4
    // Too few queries for a second range.
    SearchPlan.rangeCount(
      queries = 1000,
      sets = 2,
      slots = 16,
      split = true
    ) shouldBe 1
    // Enough sets already: one range.
    SearchPlan.rangeCount(
      queries = 349525,
      sets = 16,
      slots = 16,
      split = true
    ) shouldBe 1
  }

  test("ranges take whole groups, the longer ranges first") {
    SearchPlan.queryRanges(groups = 16, ranges = 8) shouldBe
      (0 until 8).map(range => (range * 2) until (range * 2 + 2))
    SearchPlan.queryRanges(groups = 5, ranges = 3) shouldBe
      Seq(0 until 2, 2 until 4, 4 until 5)
    an[IllegalArgumentException] should be thrownBy
      SearchPlan.queryRanges(groups = 3, ranges = 4)
  }

  test("an index search keeps every group in one range") {
    SearchPlan.rangeCount(
      queries = 349525,
      sets = 2,
      slots = 16,
      split = false
    ) shouldBe 1
    SearchPlan.queryRanges(groups = 16, ranges = 1) shouldBe Seq(0 until 16)
  }

  test("an even cut puts the longer groups first and covers every query") {
    val groups = SearchPlan.evenGroups(queries = 349525, count = 8)

    groups.map(_.queries) shouldBe Seq.fill(5)(43691) ++ Seq.fill(3)(43690)
    groups.head.firstQuery shouldBe 0
    groups.last.untilQuery shouldBe 349525
    groups.sliding(2).forall { case Seq(a, b) =>
      a.untilQuery == b.firstQuery
    } shouldBe true
  }

  test(
    "the tasks are the sets times the ranges, and the ranges cover the groups"
  ) {
    val tasks = Seq(task(1L, 10L), task(2L, 10L))
    // Four groups of 2,048 queries, 496 bytes each.
    val plan = SearchPlan.of(
      tasks,
      layout,
      concurrency = 8,
      queries = 4 * SearchPlan.MinRangeQueries,
      k = 10,
      groupMaxBytes = SearchPlan.MinRangeQueries * 496L,
      budget = SearchPlan.Budget(1L << 31, 0L),
      footprint = vectors,
      shuffled = false,
      splitQueries = true
    )
    plan.sets.size shouldBe 2
    plan.groups.size shouldBe 4
    plan.queryRanges shouldBe Seq(0 until 1, 1 until 2, 2 until 3, 3 until 4)
    plan.tasks shouldBe 8
    an[IllegalArgumentException] should be thrownBy
      plan.copy(ranges = Seq(0 until 2, 3 until 4))
  }

  test("a task that can keep its queries reads each segment once") {
    // Four segments, two tasks at once, and room for the queries: two sets,
    // each read once, and every group of the one range answered in one pass.
    val plan = SearchPlan.of(
      Seq(task(1L, 100L), task(2L, 100L), task(3L, 100L), task(4L, 100L)),
      layout,
      concurrency = 2,
      queries = 8,
      k = 1,
      groupMaxBytes = 128L,
      budget = SearchPlan.Budget(1L << 20, 1L << 20),
      footprint = vectors,
      shuffled = false
    )
    plan.resident shouldBe SearchPlan.Resident.Queries
    segments(plan.sets) shouldBe Seq(Seq(1L, 3L), Seq(2L, 4L))
    plan.groups.size shouldBe 4
    plan.queryRanges shouldBe Seq(0 until 4)
    plan.tasks shouldBe 2
    // 8 queries of 8 + 48 + 128 bytes on the heap; off it, their 16-byte rows
    // and one 64-byte block.
    plan.needs.map(_.queriesHeap) shouldBe Some(8L * 184L)
    plan.needs.map(_.queriesOffHeap) shouldBe Some(8L * 16L + 64L)
  }

  test("a task whose queries do not fit keeps its segment set") {
    val plan = SearchPlan.of(
      Seq(task(1L, 100L), task(2L, 100L), task(3L, 100L), task(4L, 100L)),
      layout,
      concurrency = 2,
      queries = 8,
      k = 1,
      groupMaxBytes = 128L,
      budget = SearchPlan.Budget(1L << 20, 100L),
      footprint = vectors,
      shuffled = false
    )
    plan.resident shouldBe SearchPlan.Resident.Segments
    segments(plan.sets) shouldBe Seq(Seq(1L, 3L), Seq(2L, 4L))
    plan.kept shouldBe Seq(Some(3200L), Some(3200L))
  }

  test(
    "a loaded index holds two copies of its bytes and loads beside a third"
  ) {
    val indexes: SegmentReadTask => SearchPlan.Footprint =
      _ => SearchPlan.Footprint.index(1000L)
    def keeping(segmentBytes: Long) = SearchPlan.of(
      Seq(task(1L, 10L), task(2L, 10L), task(3L, 10L), task(4L, 10L)),
      layout,
      concurrency = 2,
      queries = 2,
      k = 1,
      groupMaxBytes = 128L,
      budget = SearchPlan.Budget(segmentBytes, 0L),
      footprint = indexes,
      shuffled = false
    )
    // Two loaded indexes of 1000 recorded bytes hold 2000 each; one more
    // loading beside them holds its 1000 read bytes, and the one group's
    // matrix 32: 5032 bytes keep two segments a set.
    val two = keeping(5032L)
    two.resident shouldBe SearchPlan.Resident.Segments
    two.capacity shouldBe 4000L
    segments(two.sets) shouldBe Seq(Seq(1L, 3L), Seq(2L, 4L))
    two.needs.map(_.segmentsOffHeap) shouldBe Some(5032L)
    // A byte less and a set keeps one.
    keeping(5031L).sets.size shouldBe 4
    // Searching an index without keeping it still loads it whole: the query
    // matrix and three copies of the largest index while it loads.
    two.needs.map(_.queriesOffHeap) shouldBe Some(32L + 3000L)
  }

  test("an index whose size nothing recorded is planned at the budget share") {
    val plan = SearchPlan.of(
      Seq(task(1L, 10L), task(2L, 10L)),
      layout,
      concurrency = 2,
      queries = 2,
      k = 1,
      groupMaxBytes = 128L,
      budget = SearchPlan.Budget(4000L, 0L),
      footprint = _ => SearchPlan.Footprint.unsizedIndex,
      shuffled = false
    )
    // Half the budget kept, and the same again while it loads.
    plan.estimated shouldBe Seq(1L, 2L)
    plan.needs.map(_.queriesOffHeap) shouldBe Some(32L + 4000L)
  }

  test("queries arriving from the shuffle count one group on the heap") {
    def resident(shuffled: Boolean) = SearchPlan
      .of(
        Seq(task(1L, 10L), task(2L, 10L)),
        layout,
        concurrency = 2,
        queries = 2,
        k = 1,
        groupMaxBytes = 128L,
        budget = SearchPlan.Budget(1L << 20, 400L),
        footprint = vectors,
        shuffled = shuffled
      )
      .resident
    // 2 x 184 = 368 bytes fit 400; the arriving group's ids and rows, 2 x 24
    // bytes more, do not.
    resident(shuffled = false) shouldBe SearchPlan.Resident.Queries
    resident(shuffled = true) shouldBe SearchPlan.Resident.Segments
  }

  test("an exact search keeping its queries still cuts ranges for its tasks") {
    val plan = SearchPlan.of(
      Seq(task(1L, 10L), task(2L, 10L)),
      layout,
      concurrency = 8,
      queries = 4 * SearchPlan.MinRangeQueries,
      k = 10,
      groupMaxBytes = SearchPlan.MinRangeQueries * 496L,
      budget = SearchPlan.Budget(1L << 30, 1L << 30),
      footprint = vectors,
      shuffled = false,
      splitQueries = true
    )
    plan.resident shouldBe SearchPlan.Resident.Queries
    plan.sets.size shouldBe 2
    plan.queryRanges.size shouldBe 4
    plan.tasks shouldBe 8
  }

  private val wide = VectorLayout(VectorElementType.Float32, 768)

  /** 1 GiB of 768-dimension queries on two segments of 1.94 million rows and 16
    * slots: the 1g x 10g exact search (decision 32).
    */
  private def tenGiB(groupMaxBytes: Long): SearchPlan.Plan = SearchPlan.of(
    Seq(task(1L, 1940000L), task(2L, 1940000L)),
    wide,
    concurrency = 16,
    queries = 349525,
    k = 10,
    groupMaxBytes = groupMaxBytes,
    budget = SearchPlan.Budget(1L << 31, 1L << 31),
    footprint = SearchPlan.Footprint.vectors(_, wide, 32L << 20),
    shuffled = true,
    splitQueries = true
  )

  test(
    "groups fewer than the ranges are cut again, one group per range under the limit"
  ) {
    // 512 MiB groups hold 151,146 queries: the byte limit cuts 3, and the two
    // sets need 8 ranges to fill 16 slots.
    val plan = tenGiB(512L << 20)

    plan.resident shouldBe SearchPlan.Resident.Queries
    plan.sets.size shouldBe 2
    plan.groups.size shouldBe 8
    plan.queryRanges shouldBe (0 until 8).map(group => group until group + 1)
    plan.tasks shouldBe 16
    plan.groups.map(_.queries).max should be <=
      SearchPlan.queriesPerGroup(wide, k = 10, groupMaxBytes = 512L << 20)
    plan.groups.map(_.queries).sum shouldBe 349525
  }

  test(
    "groups the byte limit cuts plentifully are kept and shared by the ranges"
  ) {
    // 64 MiB groups hold 18,893 queries: 19 groups in 8 ranges, as before.
    val plan = tenGiB(64L << 20)

    plan.groups.size shouldBe 19
    plan.queryRanges.size shouldBe 8
    plan.tasks shouldBe 16
  }

  test("a query set too small for a second range keeps one range per set") {
    val plan = SearchPlan.of(
      Seq(task(1L, 1940000L), task(2L, 1940000L)),
      wide,
      concurrency = 16,
      queries = 1000,
      k = 10,
      groupMaxBytes = 512L << 20,
      budget = SearchPlan.Budget(1L << 31, 1L << 31),
      footprint = SearchPlan.Footprint.vectors(_, wide, 32L << 20),
      shuffled = false,
      splitQueries = true
    )

    plan.groups.size shouldBe 1
    plan.queryRanges shouldBe Seq(0 until 1)
    plan.tasks shouldBe 2
  }
}
