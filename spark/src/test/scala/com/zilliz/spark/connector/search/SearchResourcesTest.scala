package com.zilliz.spark.connector.search

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.milvus.storage.vector.SearchPlan
import com.zilliz.spark.connector.options.{MilvusOption, SearchLimits}

/** The exact scan's base block and what one task may keep, off the heap and on
  * it, for an executor's memory. The exact rows are the design's worked
  * examples on the measured machine, 128 CPUs and 503 GiB; the index rows are
  * the UAT executors of the perf_laion_31m runs, a 4 GiB heap and 28 GiB
  * overhead.
  */
class SearchResourcesTest extends AnyFunSuite with Matchers {

  private val MiB = 1L << 20
  private val GiB = 1L << 30

  test("overlapping search and output fit the execution group budget") {
    val layout = VectorLayout(VectorElementType.Float32, 768)
    val k = 100000
    val perQuery = SearchResources.groupBytesPerQuery(layout, k, true)
    // Current primitive and packed candidates plus the next primitive merger,
    // and two groups' vectors, ids and queue metadata.
    perQuery shouldBe 7206416L
    val heap = SearchResources.queryBudget(24L * GiB, 0.6, 1).bytes
    val budget =
      SearchResources.groupBytes(None, heap, 32, perQuery, Long.MaxValue)
    val coreLimit =
      SearchResources.groupPlanBytes(budget.bytes, layout, k, true)
    val group = SearchPlan.groups(1000, layout, k, coreLimit).head
    group.queries shouldBe 256
    group.queries.toLong * perQuery should be <= budget.bytes
    val explicit = 512L * MiB
    val limited = SearchPlan
      .groups(
        1000,
        layout,
        k,
        SearchResources.groupPlanBytes(explicit, layout, k, true)
      )
      .head
    limited.queries shouldBe 74
    limited.queries.toLong * perQuery should be <= explicit
    (limited.queries.toLong + 1L) * perQuery should be > explicit
  }

  test("pipeline budgets retain the single-query and serial-path contracts") {
    val layout = VectorLayout(VectorElementType.Float32, 768)
    val k = 1000000
    val perQuery = SearchResources.groupBytesPerQuery(layout, k, true)
    val smallest =
      SearchResources.groupBytes(None, 0L, 32, perQuery, Long.MaxValue)
    SearchPlan
      .groups(
        2,
        layout,
        k,
        SearchResources.groupPlanBytes(smallest.bytes, layout, k, true)
      )
      .map(_.queries) shouldBe Seq(1, 1)
    intercept[IllegalArgumentException] {
      SearchResources.groupPlanBytes(perQuery - 1L, layout, k, true)
    }.getMessage should include("overlapping search and output")
    SearchResources.groupBytesPerQuery(layout, k, false) shouldBe
      SearchPlan.bytesPerQuery(layout, k)
    SearchResources.groupPlanBytes(
      512L * MiB,
      layout,
      k,
      false
    ) shouldBe 512L * MiB
  }

  test("executor heap and overhead use Spark's MiB default units") {
    SearchResources.memoryMiB("6144") shouldBe 6L * GiB
    SearchResources.memoryMiB("512") shouldBe 512L * MiB
    SearchResources.memoryMiB("6g") shouldBe 6L * GiB
    SearchResources.memoryMiB("512m") shouldBe 512L * MiB
  }

  test("an exact scan keeps half the off-heap room, shared by the tasks") {
    // The measured machine: 503 GiB, a 32 GiB heap, 16 tasks; the reserve is
    // capped at 8 GiB. (503 - 32 - 8) x 0.5 / 16 = 14.46875 GiB
    val local = SearchResources.segmentBudget(
      Some(503L * GiB),
      32L * GiB,
      0L,
      16,
      None,
      index = false
    )
    local.bytes shouldBe (463L * GiB / 2 / 16)
    local.reason shouldBe
      "limit=503GiB heap=32GiB sparkOffHeap=0MiB reserve=8GiB native=463GiB x0.5 / 16 tasks -> 14.5GiB (auto)"
    // A pod of 8 GiB with a 4 GiB heap and 4 tasks, the reserve at its 2 GiB
    // minimum: (8 - 4 - 2) x 0.5 / 4.
    SearchResources
      .segmentBudget(Some(8L * GiB), 4L * GiB, 0L, 4, None, index = false)
      .bytes shouldBe 256L * MiB
    // A container Spark sized with the default overhead: 4 GiB heap plus 410
    // MiB leaves nothing after the JVM's share, so the floor applies.
    val floored = SearchResources.segmentBudget(
      Some(4L * GiB + 410L * MiB),
      4L * GiB,
      0L,
      2,
      None,
      index = false
    )
    floored.bytes shouldBe SearchResources.MinSegmentBudgetBytes
    floored.reason should endWith("-> 64MiB (floor)")
  }

  test("an index search keeps the off-heap room less its working memory") {
    // 4 GiB heap and 28 GiB overhead, one search task per executor: the
    // reserve is 8% of 32 GiB = 2.56 GiB, 32 - 4 - 2.56 = 25.44 GiB off the
    // heap, less 1 GiB of working memory, all for the one task.
    val budget = SearchResources.segmentBudget(
      Some(32L * GiB),
      4L * GiB,
      0L,
      1,
      None,
      index = true
    )
    val reserve = (32L * GiB * 0.08).toLong
    reserve shouldBe SearchResources.jvmReserveBytes(32L * GiB)
    budget.bytes shouldBe (32L * GiB - 4L * GiB - reserve - 1L * GiB)
    budget.reason shouldBe
      "limit=32GiB heap=4GiB sparkOffHeap=0MiB reserve=2.6GiB native=25.4GiB -1GiB / 1 tasks -> 24.4GiB (auto)"
  }

  test("Spark's own off-heap pool is inside the limit but not the segments'") {
    // The bench executor: a 224 GiB pod of a 24 GiB heap, Spark's 64 GiB
    // off-heap pool for its shuffle sort pages and 136 GiB of overhead. The
    // reserve is capped at 8 GiB; the native room is 224 - 24 - 64 - 8 = 128
    // GiB, less 1 GiB of working memory for the one index task.
    val pooled = SearchResources.segmentBudget(
      Some(224L * GiB),
      24L * GiB,
      64L * GiB,
      1,
      None,
      index = true
    )
    pooled.bytes shouldBe 127L * GiB
    pooled.reason shouldBe
      "limit=224GiB heap=24GiB sparkOffHeap=64GiB reserve=8GiB native=128GiB -1GiB / 1 tasks -> 127GiB (auto)"
    // The same pod without the pool: the 64 GiB belong to the segments.
    SearchResources
      .segmentBudget(Some(224L * GiB), 24L * GiB, 0L, 1, None, index = true)
      .bytes shouldBe 191L * GiB
  }

  test("the JVM reserve is 8% of the limit between 2 and 8 GiB") {
    SearchResources.jvmReserveBytes(8L * GiB) shouldBe 2L * GiB
    SearchResources.jvmReserveBytes(
      30L * GiB
    ) shouldBe (30L * GiB * 0.08).toLong
    SearchResources.jvmReserveBytes(503L * GiB) shouldBe 8L * GiB
  }

  test("the queries' budget is the heap Spark does not manage") {
    // (4 GiB - 300 MiB) x (1 - 0.6), for one task and for four.
    val one = SearchResources.queryBudget(4L * GiB, 0.6, 1)
    one.bytes shouldBe ((4L * GiB - 300L * MiB) * 0.4).toLong
    one.reason shouldBe "heap=4GiB x (1 - 0.6) / 1 tasks -> 1.5GiB"
    SearchResources.queryBudget(4L * GiB, 0.6, 4).bytes shouldBe
      ((4L * GiB - 300L * MiB) * 0.4).toLong / 4
    SearchResources.queryBudget(200L * MiB, 0.6, 1).bytes shouldBe 0L
  }

  test("a group feeds every core a few queries, within half the query budget") {
    // L x 16 of the scaling test: a 24 GiB heap, one task holding 32 cores, so
    // the budget is (24 GiB - 300 MiB) x 0.4 = 9.5 GiB and the cap 4.7 GiB.
    val budget = SearchResources.queryBudget(24L * GiB, 0.6, 1).bytes
    // k=100,000 at 768 dimensions: 3,072 + 100,000 x 48 = 4,803,072 bytes a
    // query; 8 x 32 queries want 1.23 GB, more than the 512 MiB default.
    val perQuery = 3072L + 100000L * 48L
    val noDirectCap = Long.MaxValue
    val large =
      SearchResources.groupBytes(None, budget, 32, perQuery, noDirectCap)
    large.bytes shouldBe 8L * 32L * perQuery
    large.reason should include("256 queries")
    large.reason should include("8 queries x 32 cores")
    // k=10,000: 483,072 bytes a query, 8 x 32 want 124 MB, so the default
    // 512 MiB stands and holds 1,111 queries, 35 a core.
    val small =
      SearchResources.groupBytes(
        None,
        budget,
        32,
        3072L + 10000L * 48L,
        noDirectCap
      )
    small.bytes shouldBe SearchLimits.DefaultGroupMaxBytes
    small.reason should include("(default;")
    // S x 16 at k=100,000: a 6 GiB heap gives a 2.3 GiB budget and a 1.14 GiB
    // cap; 8 x 8 queries want 307 MB, the default 512 MiB stands.
    val sBudget = SearchResources.queryBudget(6L * GiB, 0.6, 1).bytes
    SearchResources
      .groupBytes(None, sBudget, 8, perQuery, noDirectCap)
      .bytes shouldBe
      SearchLimits.DefaultGroupMaxBytes
    // A 1 GiB heap: the budget is 290 MB, the cap 145 MB wins over the default.
    val tiny = SearchResources.groupBytes(
      None,
      SearchResources.queryBudget(1L * GiB, 0.6, 1).bytes,
      8,
      perQuery,
      noDirectCap
    )
    tiny.bytes shouldBe (SearchResources
      .queryBudget(1L * GiB, 0.6, 1)
      .bytes * 0.5).toLong
    tiny.reason should include("heap cap")
    // k=1,000,000 on 32 cores would want 12 GB: with the JVM's default direct
    // memory (the 24 GiB heap) the answers of 342 queries fit, so the heap cap
    // of half the budget binds; a 3 GiB direct limit holds 29 queries' answers
    // and binds instead.
    val layout = VectorLayout(VectorElementType.Float32, 768)
    val hugePerQuery = 3072L + 1000000L * 48L
    val roomy = SearchResources
      .directGroupCap(24L * GiB, 1, 2, layout, 1000000, hugePerQuery)
    roomy shouldBe 342L * hugePerQuery
    val huge = SearchResources.groupBytes(None, budget, 32, hugePerQuery, roomy)
    huge.bytes shouldBe (budget * 0.5).toLong
    huge.reason should include("heap cap")
    val tight = SearchResources
      .directGroupCap(3L * GiB, 1, 2, layout, 1000000, hugePerQuery)
    tight shouldBe 29L * hugePerQuery
    val capped =
      SearchResources.groupBytes(None, budget, 32, hugePerQuery, tight)
    capped.bytes shouldBe tight
    capped.reason should include("direct cap")
    // Slots share the direct memory: eight tasks of one call each get 5 queries
    // of the 2 GiB left; the cap is never below one query.
    SearchResources.directGroupCap(
      3L * GiB,
      8,
      1,
      layout,
      1000000,
      hugePerQuery
    ) shouldBe
      5L * hugePerQuery
    SearchResources.directGroupCap(
      0L,
      8,
      1,
      layout,
      1000000,
      hugePerQuery
    ) shouldBe
      hugePerQuery
    // Never below one query, even with no budget at all.
    SearchResources
      .groupBytes(None, 0L, 8, perQuery, noDirectCap)
      .bytes shouldBe perQuery
    // The option is used as it is.
    SearchResources.groupBytes(
      Some(64L * MiB),
      budget,
      32,
      perQuery,
      1L
    ) shouldBe
      SearchResources.Choice(
        64L * MiB,
        s"${64L * MiB} (option milvus.search.group.max.bytes)"
      )
  }

  test("without a memory limit the budget is the old 2 GiB default per task") {
    SearchResources.segmentBudget(
      None,
      1L << 30,
      0L,
      4,
      None,
      index = false
    ) shouldBe
      SearchResources.Choice(
        512L * MiB,
        "memory limit unknown -> 2GiB / 4 tasks = 512MiB (default)"
      )
  }

  test("a configured segments limit is per executor and divided by the tasks") {
    SearchResources.segmentBudget(
      Some(1L << 40),
      1L << 30,
      0L,
      8,
      Some(1L << 31),
      index = true
    ) shouldBe
      SearchResources.Choice(
        256L * MiB,
        s"2147483648 / 8 tasks -> 256MiB (option ${MilvusOption.SearchSegmentsMaxBytes})"
      )
  }

  test(
    "the direct memory limit is the last -XX:MaxDirectMemorySize, else none"
  ) {
    SearchResources.maxDirectMemoryOption(
      Seq("-Djava.io.tmpdir=/tmp", "-XX:MaxDirectMemorySize=64g")
    ) shouldBe Some(64L * GiB)
    SearchResources.maxDirectMemoryOption(
      Seq("-XX:MaxDirectMemorySize=512m", "-XX:MaxDirectMemorySize=3G")
    ) shouldBe Some(3L * GiB)
    SearchResources.maxDirectMemoryOption(
      Seq("-XX:MaxDirectMemorySize=1048576")
    ) shouldBe
      Some(1L * MiB)
    SearchResources.maxDirectMemoryOption(Seq("-Xmx4g")) shouldBe None
  }

  test(
    "the block is 32 MiB unless the option set it, and the reason says which"
  ) {
    SearchResources.exactScanBatch(None) shouldBe
      SearchResources.Choice(32 * MiB, "32MiB (default)")
    SearchResources.exactScanBatch(Some(33554432L)) shouldBe
      SearchResources.Choice(
        33554432L,
        s"33554432 (option ${MilvusOption.ReadBatchMaxBytes})"
      )
    SearchResources.exactScanBatch(Some(4L * MiB)).bytes shouldBe 4 * MiB
  }
}
