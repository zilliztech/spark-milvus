package com.zilliz.spark.connector.options

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The exact scan's base block and what one task may keep, off the heap and on
  * it, for an executor's memory (docs/design/architecture/search-resources.html
  * sections 3.2 and 3.3). The exact rows are the design's worked examples on
  * the measured machine, 128 CPUs and 503 GiB; the index rows are the UAT
  * executors of the perf_laion_31m runs, a 4 GiB heap and 28 GiB overhead.
  */
class SearchResourcesTest extends AnyFunSuite with Matchers {

  private val MiB = 1L << 20
  private val GiB = 1L << 30

  test("an exact scan keeps half the off-heap room, shared by the tasks") {
    // The measured machine: 503 GiB, a 32 GiB heap, 16 tasks; the reserve is
    // capped at 8 GiB. (503 - 32 - 8) x 0.5 / 16 = 14.46875 GiB
    val local = SearchResources.segmentBudget(
      Some(503L * GiB),
      32L * GiB,
      16,
      None,
      index = false
    )
    local.bytes shouldBe (463L * GiB / 2 / 16)
    local.reason shouldBe
      "limit=503GiB heap=32GiB reserve=8GiB offheap=463GiB x0.5 / 16 tasks -> 14.5GiB (auto)"
    // A pod of 8 GiB with a 4 GiB heap and 4 tasks, the reserve at its 2 GiB
    // minimum: (8 - 4 - 2) x 0.5 / 4.
    SearchResources
      .segmentBudget(Some(8L * GiB), 4L * GiB, 4, None, index = false)
      .bytes shouldBe 256L * MiB
    // A container Spark sized with the default overhead: 4 GiB heap plus 410
    // MiB leaves nothing after the JVM's share, so the floor applies.
    val floored = SearchResources.segmentBudget(
      Some(4L * GiB + 410L * MiB),
      4L * GiB,
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
      1,
      None,
      index = true
    )
    val reserve = (32L * GiB * 0.08).toLong
    reserve shouldBe SearchResources.jvmReserveBytes(32L * GiB)
    budget.bytes shouldBe (32L * GiB - 4L * GiB - reserve - 1L * GiB)
    budget.reason shouldBe
      "limit=32GiB heap=4GiB reserve=2.6GiB offheap=25.4GiB -1GiB / 1 tasks -> 24.4GiB (auto)"
  }

  test("the JVM reserve is 8% of the limit between 2 and 8 GiB") {
    SearchResources.jvmReserveBytes(8L * GiB) shouldBe 2L * GiB
    SearchResources.jvmReserveBytes(30L * GiB) shouldBe (30L * GiB * 0.08).toLong
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

  test("without a memory limit the budget is the old 2 GiB default per task") {
    SearchResources.segmentBudget(
      None,
      1L << 30,
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
