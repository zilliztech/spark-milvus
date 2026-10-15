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

import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.milvus.storage.vector.{CandidateBytes, SearchPlan}
import com.zilliz.spark.connector.options.{SearchLimits, TaskResources}

class SearchExecutionPlanTest extends AnyFunSuite with Matchers {
  private val layout = VectorLayout(VectorElementType.Float32, 2)

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
    val answerBytes = k.toLong * (CandidateBytes.Width + 4L + rowBytes)
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
    // A full k=10 answer needs 440 bytes, but one actual hit needs only 44.
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
      val (_, heap) = SearchResources.executorMemory(spark)
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
      val encodedAnswer = CandidateBytes.Width + 4L + rowBytes
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
