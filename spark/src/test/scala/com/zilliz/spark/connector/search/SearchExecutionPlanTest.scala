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
      def indexed(id: Long): SegmentReadTask = {
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
          10L,
          100L,
          1L,
          Some(8),
          Some(0)
        )
        segment(id, Some(10L))
          .copy(indexes = SegmentIndexes.Available(Vector(descriptor)))
      }
      def plan(tasks: Seq[SegmentReadTask], mode: SearchMode) =
        SearchExecutionPlan.milvus(
          spark,
          SearchLimits.Default,
          SearchQueries
            .Rows(spark.sparkContext.emptyRDD[(Long, Array[Float])], 100L),
          tasks,
          4L,
          layout,
          3,
          MetricType.L2,
          mode,
          None,
          takeColumns = false
        )
      val first = indexed(1L)
      val second = indexed(2L)
      val full = plan(Seq(first, second), SearchMode.PreferIndex)
      full.candidateDictionary.size shouldBe 2
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
    } finally spark.stop()
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
      .groupBytes(None, frameTaskHeap, 1, perQuery)
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

  test("task policy is serializable without driver resources") {
    val policy = SearchExecutionPlan.Task(
      SearchExecutionPlan.Retention.Stream(101L),
      100L,
      32L << 20,
      8,
      2,
      prefetchIndexes = false,
      pipelineGroups = false
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
