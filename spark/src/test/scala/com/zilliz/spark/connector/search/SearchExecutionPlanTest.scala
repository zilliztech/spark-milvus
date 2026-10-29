package com.zilliz.spark.connector.search

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.milvus.storage.snapshot.{
  SegmentIndex,
  SegmentIndexes,
  SegmentLayout,
  SegmentStatistics
}
import com.zilliz.milvus.storage.vector.{
  Candidate,
  CandidateBytes,
  CandidateBytesFixture,
  CandidateEncoding,
  CandidateRuns,
  IndexSearch,
  SearchPlan
}
import com.zilliz.spark.connector.options.{SearchLimits, TaskResources}

class SearchExecutionPlanTest extends AnyFunSuite with Matchers {
  private val layout = VectorLayout(VectorElementType.Float32, 2)

  private def segment(id: Long, rows: Option[Long]): SegmentReadTask =
    SegmentReadTask(
      id,
      2L,
      SegmentLayout.Manifest("unused", 1L),
      Array.emptyByteArray,
      Map.empty,
      snapshotRows = rows
    )

  private def withPlanningSpark(
      cores: Int,
      memoryFraction: Double = 0.6
  )(check: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .master(s"local[$cores]")
      .appName("candidate-capacity-planning")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.memory.fraction", memoryFraction)
      .getOrCreate()
    try check(spark)
    finally spark.stop()
  }

  private def indexed(id: Long, rows: Long = 10L): SegmentReadTask = {
    val descriptor = SegmentIndex(
      1L,
      2L,
      id,
      4L,
      5L,
      6L,
      "vector_index",
      Map("index_type" -> "HNSW", "metric_type" -> "L2"),
      Vector("unused/index"),
      rows,
      100L,
      1L,
      Some(8),
      Some(0)
    )
    segment(id, Some(rows))
      .copy(indexes = SegmentIndexes.Available(Vector(descriptor)))
  }

  private def capacityPlan(
      spark: SparkSession,
      tasks: Seq[SegmentReadTask],
      k: Int,
      capacity: Int
  ): SearchExecutionPlan.Milvus =
    SearchExecutionPlan.milvus(
      spark,
      SearchLimits.Default.copy(
        groupMaxBytes = Some(3L * SearchPlan.bytesPerQuery(layout, capacity))
      ),
      SearchQueries
        .Rows(spark.sparkContext.emptyRDD[(Long, Array[Float])], 20L),
      tasks,
      4L,
      layout,
      k,
      MetricType.L2,
      SearchMode.Exact,
      None,
      takeColumns = false
    )

  private def assertCandidateBudget(
      plan: SearchExecutionPlan.Milvus,
      capacity: Int
  ): Unit = {
    plan.segments.groups.foreach { group =>
      group.queries.toLong * SearchResources.groupBytesPerQuery(
        layout,
        capacity,
        plan.budget.pipelineGroups
      ) should be <= plan.budget.group.bytes
    }
    val rangeQueries = plan.segments.queryRanges
      .map(range =>
        range.map(index => plan.segments.groups(index).queries.toLong).sum
      )
      .max
    plan.segments.needs.get.queriesHeap shouldBe rangeQueries * (
      SearchPlan.QueryIdBytes.toLong + capacity.toLong * SearchPlan.CandidateBytes +
        SearchPlan.TopKQueueBytes
    )
  }

  test("one set budgets and retains the combined top-k of its small segments") {
    withPlanningSpark(1) { spark =>
      Seq(6 -> 6, Int.MaxValue -> 7).foreach { case (requested, capacity) =>
        val planned = capacityPlan(
          spark,
          Seq(segment(1L, Some(3L)), segment(2L, Some(4L))),
          requested,
          capacity
        )
        planned.segments.sets.map(_.size) shouldBe Seq(2)
        planned.tasks.map(_.topK).distinct shouldBe Vector(capacity)
        planned.segmentK shouldBe 4
        planned.setK shouldBe capacity
        planned.merge.k shouldBe capacity
        planned.segments.groups.map(_.queries).max shouldBe 3
        planned.segments.groups.map(_.queries).sum shouldBe 20
        assertCandidateBudget(planned, capacity)
      }
    }
  }

  test(
    "global merge budgets its output beyond every individual set's capacity"
  ) {
    withPlanningSpark(2) { spark =>
      val rows = Map(1L -> 3L, 2L -> 4L, 3L -> 3L, 4L -> 4L)
      val planned = capacityPlan(
        spark,
        rows.toSeq.sortBy(_._1).map { case (id, count) =>
          segment(id, Some(count))
        },
        Int.MaxValue,
        14
      )
      planned.segments.sets.size shouldBe 2
      val setCapacities = planned.segments.sets.map(set =>
        set.map(segment => rows(segment.segmentId)).sum.toInt
      )
      setCapacities.sum shouldBe 14
      setCapacities.max should be < 14
      planned.tasks.map(_.topK) shouldBe setCapacities.flatMap(capacity =>
        Seq.fill(planned.segments.queryRanges.size)(capacity)
      )
      planned.setK shouldBe setCapacities.max
      planned.merge.k shouldBe 14
      val footprint = SearchMerge.footprint(
        2,
        14,
        maxRuns = planned.mergeStage.maxRuns
      )
      planned.mergeStage.footprint shouldBe footprint
      planned.mergeStage.footprint.answerBytes shouldBe 14L * CandidateBytes.Width
      planned.merge.bytesPerQuery shouldBe footprint.bytesPerQuery
      planned.merge.maxRuns shouldBe planned.mergeStage.maxRuns
      assertCandidateBudget(planned, 14)
    }
  }

  test(
    "candidate dictionary uses all segment ids and rejects known oversized physical extents"
  ) {
    val maximum = CandidateBytes.MaxRows
    val dictionary = SearchExecutionPlan.candidateDictionary(
      Seq(segment(Long.MaxValue, Some(maximum)), segment(-1L, Some(0L)))
    )
    dictionary.segmentId(0L) shouldBe -1L
    dictionary.segmentId(1L) shouldBe Long.MaxValue
    Seq(None, Some(-1L)).foreach { rows =>
      val unknown = SearchExecutionPlan.candidateDictionary(
        Seq(segment(1L, Some(10L)), segment(2L, rows))
      )
      unknown.segmentId(0L) shouldBe 1L
      unknown.segmentId(1L) shouldBe 2L
      val encoding = CandidateEncoding(unknown, Seq(2L))
      val packed = CandidateBytesFixture.pack(
        Seq(Candidate(0, 2L, maximum - 1L, 1f)),
        MetricType.L2,
        encoding
      )
      CandidateBytes.rowOffset(packed, 0) shouldBe maximum - 1L
      intercept[IllegalArgumentException] {
        CandidateBytesFixture.pack(
          Seq(Candidate(0, 2L, maximum, 1f)),
          MetricType.L2,
          encoding
        )
      }
    }
    Seq(maximum + 1L, Long.MaxValue).foreach { rows =>
      intercept[IllegalArgumentException] {
        SearchExecutionPlan.candidateDictionary(
          Seq(segment(1L, Some(10L)), segment(2L, Some(rows)))
        )
      }.getMessage should include(
        "exceeding the UInt32 candidate address limit"
      )
    }
    SearchExecutionPlan
      .candidateDictionary(
        Seq(segment(1L, Some(10L)), segment(1L, Some(11L)))
      )
      .size shouldBe 1
  }

  test(
    "indexed exact and mixed searches share a fixed dictionary and conservative budgets"
  ) {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("candidate-address-planning")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    try {
      def plan(tasks: Seq[SegmentReadTask], mode: SearchMode, k: Int = 3) =
        SearchExecutionPlan.milvus(
          spark,
          SearchLimits.Default,
          SearchQueries
            .Rows(spark.sparkContext.emptyRDD[(Long, Array[Float])], 100L),
          tasks,
          4L,
          layout,
          k,
          MetricType.L2,
          mode,
          None,
          takeColumns = false
        )
      val first = indexed(1L)
      val second = indexed(2L)
      val full = plan(Seq(first, second), SearchMode.PreferIndex)
      full.candidateDictionary.size shouldBe 2
      val indexedCapacity =
        plan(Seq(first, second), SearchMode.PreferIndex, k = 100)
      indexedCapacity.merge.k shouldBe 20
      indexedCapacity.tasks.map(_.topK).distinct shouldBe Vector(10)
      indexedCapacity.segments.needs.get.queriesOffHeap shouldBe
        100L * layout.rowBytes + 200L + IndexSearch.resultBytes(
          100,
          10,
          SearchLimits.Default.callsInFlight
        )
      assertCandidateBudget(indexedCapacity, 20)
      val exactFull = plan(Seq(first, second), SearchMode.Exact)
      exactFull.candidateDictionary.size shouldBe 2
      plan(
        Seq(first, second.copy(indexes = SegmentIndexes.Unindexed)),
        SearchMode.PreferIndex
      ).candidateDictionary.size shouldBe 2

      // An index count can size exact search, but does not establish the
      // physical extent of a V3 manifest or permit unchecked index loading.
      val unproven = second.copy(snapshotRows = None)
      unproven.expectedRows shouldBe None
      unproven.knownRows shouldBe Some(10L)
      unproven.provenPhysicalRows shouldBe None
      intercept[IllegalArgumentException] {
        plan(Seq(first, unproven), SearchMode.PreferIndex)
      }.getMessage should include(
        "Index row count differs from the pinned segment 2"
      )
      val unknown = plan(Seq(first, unproven), SearchMode.Exact)
      unknown.candidateDictionary.size shouldBe 2
      unknown.candidateDictionary.segmentId(0L) shouldBe 1L
      unknown.candidateDictionary.segmentId(1L) shouldBe 2L
      unknown.segments shouldBe exactFull.segments
      unknown.merge shouldBe exactFull.merge
      plan(
        Seq(first, unproven.copy(indexes = SegmentIndexes.Unindexed)),
        SearchMode.PreferIndex
      ).candidateDictionary.size shouldBe 2

      // Neither a missing extent nor an index's estimate proves an upper
      // bound for the rows an exact scan can return.
      Seq(
        segment(2L, None),
        unproven
      ).foreach { unbounded =>
        val conservative =
          plan(Seq(first, unbounded), SearchMode.Exact, k = 100)
        conservative.segmentK shouldBe 100
        conservative.setK shouldBe 100
        conservative.merge.k shouldBe 100
        conservative.tasks.map(_.topK) should contain(100)
        conservative.mergeStage.footprint shouldBe SearchMerge.footprint(
          conservative.segments.sets.size,
          100,
          maxRuns = conservative.mergeStage.maxRuns
        )
        assertCandidateBudget(conservative, 100)
      }
    } finally spark.stop()
  }

  test("direct memory limits index groups by one segment's result width") {
    withPlanningSpark(1, memoryFraction = 0.1) { spark =>
      val heap = SearchResources.executorMemory(spark).heap
      val directRoom = SearchResources.directMemoryBytes(spark, heap) -
        SearchResources.DirectReserveBytes
      directRoom should be > (1L << 20)
      // Only planning runs. Scale the metadata to this JVM's direct limit:
      // vectors take two units and one segment's result takes one unit. With
      // nine live answers, three queries fit; using both segments' row count
      // for each native result would incorrectly cut groups to two queries.
      val unit = directRoom / 90L
      val wide = VectorLayout(VectorElementType.Float32, (unit / 2L).toInt)
      val rows = (unit / 24L).toInt
      val total = 2 * rows
      val planned = SearchExecutionPlan.milvus(
        spark,
        SearchLimits.Default.copy(callsInFlight = 8),
        SearchQueries.Rows(
          spark.sparkContext.emptyRDD[(Long, Array[Float])],
          7L
        ),
        Seq(indexed(1L, rows), indexed(2L, rows)),
        4L,
        wide,
        Int.MaxValue,
        MetricType.L2,
        SearchMode.PreferIndex,
        None,
        takeColumns = false
      )
      planned.budget.group.reason should include("direct cap")
      planned.segments.groups.map(_.queries) shouldBe Seq(3, 3, 1)
      planned.segmentK shouldBe rows
      planned.segments.sets.map(_.size) shouldBe Seq(2)
      planned.tasks.map(_.topK).distinct shouldBe Vector(total)
      planned.setK shouldBe total
      planned.merge.k shouldBe total
      planned.mergeStage.footprint.answerBytes shouldBe
        total.toLong * CandidateBytes.Width
    }
  }

  test(
    "index prefetch reserves segment results while mergers retain set totals"
  ) {
    withPlanningSpark(2) { spark =>
      val planned = SearchExecutionPlan.milvus(
        spark,
        SearchLimits.Default.copy(
          groupMaxBytes = Some(100L * SearchPlan.bytesPerQuery(layout, 40)),
          segmentsMaxBytes = Some(60000L),
          callsInFlight = 1
        ),
        SearchQueries.Rows(
          spark.sparkContext.emptyRDD[(Long, Array[Float])],
          100L
        ),
        (1L to 4L).map(id => indexed(id)),
        4L,
        layout,
        100,
        MetricType.L2,
        SearchMode.PreferIndex,
        None,
        takeColumns = false
      )
      // Each task has 30,000 bytes: two loaded indexes (400), query vectors
      // (800), and two rounded native answers of width 10 (24,576) fit. A
      // width-40 global answer would incorrectly disable query retention and
      // prefetch, although no index call can return more than its ten rows.
      planned.budget.segments.bytes shouldBe 30000L
      planned.segments.resident shouldBe SearchPlan.Resident.Queries
      planned.segments.groups.map(_.queries) shouldBe Seq(100)
      planned.segments.sets.map(_.size) shouldBe Seq(2, 2)
      planned.tasks.map(_.prefetchIndexes) shouldBe Vector(true, true)
      planned.tasks.map(_.topK) shouldBe Vector(20, 20)
      planned.segmentK shouldBe 10
      planned.setK shouldBe 20
      planned.merge.k shouldBe 40
      assertCandidateBudget(planned, 40)
    }
  }

  private val frameLayout = VectorLayout(VectorElementType.Float32, 1)
  private val frameTaskHeap =
    SearchResources.queryBudget(1L << 30, 0.6, 4).bytes
  private val frameBlockBytes = math.min(
    SearchResources.exactScanBatch(None).bytes,
    frameTaskHeap / 4L
  )
  private val frameAvailableHeap = frameTaskHeap - frameBlockBytes

  /** A task may keep different base rows for every query, and must still be
    * able to encode one query's answer while retaining those rows.
    */
  private def assertFrameBudget(
      groups: Seq[SearchPlan.QueryGroup],
      k: Int,
      rowBytes: Long,
      availableHeap: Long
  ): Unit = {
    val queryHeap = SearchPlan.bytesPerQuery(frameLayout, k) +
      SearchPlan.QueryIdBytes + SearchPlan.TopKQueueBytes
    val answerBytes =
      k.toLong * (SearchPlan.PackedCandidateReserveBytes + 4L + rowBytes)
    groups.foreach { group =>
      val rows = group.queries.toLong * k * rowBytes
      val required = group.queries.toLong * queryHeap +
        2L * math.max(rows, answerBytes)
      withClue(s"group=$group k=$k rowBytes=$rowBytes: ") {
        required should be <= availableHeap
      }
    }
  }

  test("frame groups fit a one-GiB heap shared by four tasks") {
    val queries = 1000000
    val rowBytes = FrameSearch.rowWidth(Seq(LongType), 1)
    val perQuery = SearchPlan.bytesPerQuery(frameLayout, 1) +
      SearchPlan.QueryIdBytes + SearchPlan.TopKQueueBytes + rowBytes
    val groupLimit = SearchResources
      .groupBytes(None, frameTaskHeap, 1, perQuery, Long.MaxValue)
      .bytes
    val groups = SearchExecutionPlan.frameGroups(
      queries,
      frameLayout,
      1,
      rowBytes,
      groupLimit,
      frameAvailableHeap
    )
    groups.map(_.queries.toLong).sum shouldBe queries.toLong
    groups.head.queries should be > 1
    groups.last.untilQuery shouldBe queries
    assertFrameBudget(groups, 1, rowBytes, frameAvailableHeap)
  }

  test("configured frame group limits still leave heap for retained rows") {
    val queries = 100000
    val k = 10
    val rowBytes = FrameSearch.rowWidth(Seq(LongType), 1)
    val perQuery = SearchPlan.bytesPerQuery(frameLayout, k) +
      SearchPlan.QueryIdBytes + SearchPlan.TopKQueueBytes + k.toLong * rowBytes
    val smallLimit = 3L * perQuery
    Seq(smallLimit, Long.MaxValue).foreach { limit =>
      val groups = SearchExecutionPlan.frameGroups(
        queries,
        frameLayout,
        k,
        rowBytes,
        limit,
        frameAvailableHeap
      )
      groups.map(_.queries.toLong).sum shouldBe queries.toLong
      assertFrameBudget(groups, k, rowBytes, frameAvailableHeap)
      groups.head.queries.toLong * perQuery should be <= limit
      if (limit == smallLimit) groups.head.queries shouldBe 3
      else groups.head.queries should be < queries
    }
  }

  test("wide output rows reduce frame groups to fit their retained payload") {
    def groups(rowBytes: Long): Seq[SearchPlan.QueryGroup] =
      SearchExecutionPlan.frameGroups(
        10000,
        frameLayout,
        10,
        rowBytes,
        Long.MaxValue,
        frameAvailableHeap
      )
    val narrow = groups(16L)
    val wide = groups(4096L)
    wide.head.queries should be < narrow.head.queries
    assertFrameBudget(narrow, 10, 16L, frameAvailableHeap)
    assertFrameBudget(wide, 10, 4096L, frameAvailableHeap)
  }

  test("a large-k frame group can encode hits even with zero output columns") {
    val k = 100000
    val rowBytes = FrameSearch.rowWidth(Seq.empty, 1)
    rowBytes shouldBe 0L
    val groups = SearchExecutionPlan.frameGroups(
      100,
      frameLayout,
      k,
      rowBytes,
      Long.MaxValue,
      frameAvailableHeap
    )
    groups.map(_.queries).sum shouldBe 100
    assertFrameBudget(groups, k, rowBytes, frameAvailableHeap)
  }

  test("one frame query remains indivisible when fewer than k rows may exist") {
    // A full k=10 answer reserves 440 bytes; one encoded Float hit needs 40.
    // The 1,499-byte budget leaves 439 bytes after the query's 620-byte state.
    Seq(1499L, 1500L).foreach { available =>
      SearchExecutionPlan
        .frameGroups(1, frameLayout, 10, 16L, Long.MaxValue, available)
        .map(_.queries) shouldBe Seq(1)
    }
    SearchExecutionPlan
      .frameGroups(1, frameLayout, 10, 16L, Long.MaxValue, 619L)
      .map(_.queries) shouldBe Seq(1)
  }

  test("the actual frame planner leaves room for low-dimensional candidates") {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("frame-planning-budget")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    try {
      val heap = SearchResources.executorMemory(spark).heap
      val resources = TaskResources.of(spark)
      val taskHeap = SearchResources
        .queryBudget(
          heap,
          SearchResources.memoryFraction(spark),
          resources.tasksPerExecutor
        )
        .bytes
      val blockBytes = math.min(
        SearchResources.exactScanBatch(None).bytes,
        taskHeap / 4L
      )
      val queryHeap = SearchPlan.bytesPerQuery(frameLayout, 1) +
        SearchPlan.QueryIdBytes + SearchPlan.TopKQueueBytes
      val count = math
        .max(400000L, (taskHeap - blockBytes) / queryHeap + 2L)
        .min(Int.MaxValue.toLong)
        .toInt
      // Only planning runs: the input count is already known and its RDD is
      // never evaluated, so this regression allocates no candidate payloads.
      val input = SearchQueries.Rows(
        spark.sparkContext.emptyRDD[(Long, Array[Float])],
        count.toLong
      )
      val rowBytes = FrameSearch.rowWidth(Seq(LongType), 1)
      val planned = SearchExecutionPlan.frame(
        spark,
        SearchLimits.Default,
        input,
        frameLayout,
        1,
        rowBytes,
        basePartitions = 1
      )
      val largest = planned.groups.map(_.queries.toLong).max
      val requiredRows = largest * rowBytes
      val encodedAnswer = SearchPlan.PackedCandidateReserveBytes + 4L + rowBytes
      withClue(s"heap=$heap taskHeap=$taskHeap groups=${planned.groups}: ") {
        planned.retainedMaxBytes should be >= requiredRows
        planned.retainedMaxBytes should be >= encodedAnswer
        largest * queryHeap + planned.blockMaxBytes +
          2L * planned.retainedMaxBytes should be <= taskHeap
      }
    } finally spark.stop()
  }

  test(
    "one query group keeps its queries even when the segment set exceeds capacity"
  ) {
    SearchExecutionPlan.retention(
      SearchPlan.Resident.Segments,
      1,
      Some(101L),
      100L
    ) shouldBe
      SearchExecutionPlan.Retention.Queries
    SearchExecutionPlan.retention(
      SearchPlan.Resident.Queries,
      3,
      Some(101L),
      100L
    ) shouldBe
      SearchExecutionPlan.Retention.Queries
  }

  test(
    "only a known oversized set skips retention; exact capacity and unknown size attempt it"
  ) {
    SearchExecutionPlan.retention(
      SearchPlan.Resident.Segments,
      3,
      Some(101L),
      100L
    ) shouldBe
      SearchExecutionPlan.Retention.Stream(101L)
    SearchExecutionPlan.retention(
      SearchPlan.Resident.Segments,
      3,
      Some(100L),
      100L
    ) shouldBe
      SearchExecutionPlan.Retention.Segments
    SearchExecutionPlan.retention(
      SearchPlan.Resident.Segments,
      3,
      None,
      100L
    ) shouldBe
      SearchExecutionPlan.Retention.Segments
  }

  test("index prefetch fits two loaded indexes inside remaining task memory") {
    // Each loaded index retains two copies; current loading adds no copy.
    val sizes = Seq(Some(1000L), Some(900L))
    SearchExecutionPlan.prefetchIndexes(
      true,
      sizes,
      6999L,
      1000L,
      2000L
    ) shouldBe false
    SearchExecutionPlan.prefetchIndexes(
      true,
      sizes,
      7000L,
      1000L,
      2000L
    ) shouldBe true
  }

  test("exact mode and any missing or unknown index size disable prefetch") {
    val sizes = Seq(Some(1000L), Some(900L))
    SearchExecutionPlan.prefetchIndexes(
      false,
      sizes,
      10000L,
      0L,
      0L
    ) shouldBe false
    SearchExecutionPlan.prefetchIndexes(
      true,
      sizes.take(1),
      10000L,
      0L,
      0L
    ) shouldBe false
    SearchExecutionPlan.prefetchIndexes(
      true,
      Seq(Some(1000L), None),
      10000L,
      0L,
      0L
    ) shouldBe false
    SearchExecutionPlan.prefetchIndexes(
      true,
      Seq(Some(1000L), Some(0L)),
      10000L,
      0L,
      0L
    ) shouldBe false
  }

  test(
    "packing reserves half a group's bytes as headroom and respects undeclarable resources"
  ) {
    val groups =
      Seq(SearchPlan.QueryGroup(0, 100), SearchPlan.QueryGroup(100, 100))
    val cluster = TaskResources(2, 8, 1, declarable = true)
    def cores(heap: Long): Option[Double] =
      SearchExecutionPlan
        .packingProfile(groups, layout, cluster, heap)
        .map(_.taskResources(ResourceProfile.CPUS).amount)
    // 100 queries * (8 vector bytes + 8 id bytes) * 1.5 = 2,400 bytes.
    cores(2400L) shouldBe Some(8.0)
    cores(4800L) shouldBe Some(4.0)
    SearchExecutionPlan.packingProfile(
      groups,
      layout,
      cluster.copy(declarable = false),
      2400L
    ) shouldBe None
  }

  test(
    "the merge stage keeps every slot busy while both heap pools hold a query"
  ) {
    val GiB = 1L << 30
    // An L executor of the bench: 32 cores, a 24 GiB heap, one core a task;
    // 16 executors give a default parallelism of 512.
    val large = TaskResources(16, 32, 1, declarable = true)
    def stage(
        k: Int,
        queries: Long,
        sources: Int = 16,
        resources: TaskResources = large
    ): SearchExecutionPlan.MergeStage =
      SearchExecutionPlan.mergeStage(
        512,
        resources,
        24L * GiB,
        0.6,
        sources,
        k,
        CandidateRuns.Plain,
        CandidateBytes.Width.toLong,
        queries,
        16
      )
    val userShare = SearchResources.queryBudget(24L * GiB, 0.6, 32)
    val executionShare = SearchResources.executionBudget(24L * GiB, 0.6, 32)
    // Up to k=1,000,000 a default slot's two shares hold a query's eight
    // retained runs and its temporaries: 32 tasks, undeclared, full batches.
    // A partition takes as many queries as three quarters of the execution
    // share hold runs for: 3,728 at k=1,000, 37 at k=100,000, 3 at
    // k=1,000,000.
    for ((k, perPartition) <- Seq(1000 -> 3728, 100000 -> 37, 1000000 -> 3)) {
      val full = stage(k, 1000000L)
      full.running shouldBe 32
      full.cores shouldBe 1
      full.profile shouldBe None
      full.maxRuns shouldBe SearchMerge.MaxRuns
      full.heap shouldBe userShare
      full.executionHeap shouldBe executionShare
      full.queriesPerPartition shouldBe perPartition
      full.partitions shouldBe math.max(
        512L,
        (1000000L + perPartition - 1) / perPartition
      )
      full.footprint shouldBe SearchMerge.footprint(16, k)
    }
    stage(100000, 1000000L).partitions shouldBe 27028
    stage(1000000, 10000L).partitions shouldBe 3334
    // k=10,000,000: a 120 MB answer. A slot's own share (318 MB) does not hold
    // the compaction output, the arriving answer and its block (360 MB), so
    // tasks take 2 cores and 16 run; their execution share (955 MB) holds
    // seven retained runs, one query a partition.
    val tenMillion = stage(10000000, 10000L)
    tenMillion.running shouldBe 16
    tenMillion.cores shouldBe 2
    tenMillion.maxRuns shouldBe 7
    tenMillion.queriesPerPartition shouldBe 1
    tenMillion.partitions shouldBe 10000
    tenMillion.profile shouldBe defined
    tenMillion.heap shouldBe SearchResources.queryBudget(24L * GiB, 0.6, 16)
    tenMillion.executionHeap shouldBe SearchResources
      .executionBudget(24L * GiB, 0.6, 16)
    // With blocks above 64 MiB fetched to disk the slot's own share holds the
    // two answers alone, so every slot runs, each retaining three runs.
    val spooled = SearchExecutionPlan.mergeStage(
      512,
      large,
      24L * GiB,
      0.6,
      16,
      10000000,
      CandidateRuns.Plain,
      CandidateBytes.Width.toLong,
      10000L,
      16,
      64L << 20
    )
    spooled.running shouldBe 32
    spooled.cores shouldBe 1
    spooled.maxRuns shouldBe 3
    spooled.queriesPerPartition shouldBe 1
    spooled.profile shouldBe None
    // k=100,000,000: 1.2 GB answers, 2.6 GB of temporaries a task (the block
    // is capped at what Spark fetches to memory): 3 tasks of 10 cores, whose
    // 5.1 GB execution share retains four runs.
    val hundredMillion = stage(100000000, 1000L)
    hundredMillion.running shouldBe 3
    hundredMillion.cores shouldBe 10
    hundredMillion.maxRuns shouldBe 4
    // Beyond about 415 million even one task alone does not hold a query:
    // planning fails and names the largest k that would.
    val largest = SearchExecutionPlan.largestMergeableK(
      SearchResources.queryBudget(24L * GiB, 0.6, 1).bytes,
      SearchResources.executionBudget(24L * GiB, 0.6, 1).bytes,
      16,
      CandidateRuns.Plain,
      CandidateBytes.Width.toLong
    )
    largest should be > 410000000L
    largest should be < 420000000L
    stage(largest.toInt, 10L).running shouldBe 1
    val failure =
      the[IllegalArgumentException] thrownBy stage(largest.toInt + 1, 10L)
    failure.getMessage should include(s"lower k to at most $largest")
    failure.getMessage should include("even when one task runs alone")
    the[IllegalArgumentException] thrownBy stage(Int.MaxValue, 10L)
    // A master that takes no declaration keeps its default slots: four tasks
    // of a local[4] hold k=10,000,000 with full batches, and fail as soon as
    // one of their shares does not hold a query.
    val local = TaskResources(1, 4, 1, declarable = false)
    val undeclared = stage(10000000, 100L, resources = local)
    undeclared.running shouldBe 4
    undeclared.profile shouldBe None
    undeclared.maxRuns shouldBe SearchMerge.MaxRuns
    undeclared.queriesPerPartition shouldBe 2
    val localFailure =
      the[IllegalArgumentException] thrownBy stage(
        100000000,
        100L,
        resources = local
      )
    localFailure.getMessage should include("takes no task cores declaration")
    // The smallest shares of all find no k.
    SearchExecutionPlan.largestMergeableK(
      0L,
      0L,
      16,
      CandidateRuns.Plain,
      CandidateBytes.Width.toLong
    ) shouldBe 0L
  }

  test(
    "candidate capacity uses a complete row bound and saturates before overflow"
  ) {
    SearchExecutionPlan.candidateK(100, Seq(Some(3L), Some(4L))) shouldBe 7
    SearchExecutionPlan.candidateK(6, Seq(Some(3L), Some(4L))) shouldBe 6
    Seq(Seq.empty, Seq(Some(0L)), Seq(Some(0L), Some(0L))).foreach { rows =>
      SearchExecutionPlan.candidateK(100, rows) shouldBe 1
    }
    Seq(None, Some(-1L)).foreach { unknown =>
      Seq(Seq(unknown), Seq(Some(3L), unknown)).foreach { rows =>
        SearchExecutionPlan.candidateK(100, rows) shouldBe 100
      }
    }
    SearchExecutionPlan.candidateK(
      Int.MaxValue,
      Seq(Some(Long.MaxValue), Some(Long.MaxValue))
    ) shouldBe Int.MaxValue
    SearchExecutionPlan.candidateK(
      Int.MaxValue,
      Seq(Some(Int.MaxValue.toLong - 1L), Some(2L))
    ) shouldBe Int.MaxValue
  }

  test("task policy is serializable without driver resources") {
    val policy = SearchExecutionPlan.Task(
      SearchExecutionPlan.Retention.Stream(101L),
      100L,
      32L << 20,
      8,
      2,
      prefetchIndexes = false,
      pipelineGroups = false,
      topK = 7
    )
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(policy)
    finally output.close()
    val input =
      new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    try input.readObject() shouldBe policy
    finally input.close()
  }
}
