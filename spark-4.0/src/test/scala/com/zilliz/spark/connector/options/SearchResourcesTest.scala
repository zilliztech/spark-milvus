package com.zilliz.spark.connector.options

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The exact scan's base block and the vectors a task keeps for an executor's
  * memory (docs/design/architecture/search-resources.html sections 3.2 and
  * 3.3). The budget rows are the design's worked examples on the measured
  * machine: 128 CPUs, 503 GiB.
  */
class SearchResourcesTest extends AnyFunSuite with Matchers {

  private val MiB = 1L << 20

  test("the resident budget is half the off-heap room, shared by the slots") {
    val GiB = 1L << 30
    // The measured machine: 503 GiB, a 32 GiB heap, 16 slots.
    // (503 - 32 - 1) x 0.5 / 16 = 14.6875 GiB
    val local = SearchResources.vectorsBudget(
      Some(503L * GiB),
      32L * GiB,
      16,
      None
    )
    local.bytes shouldBe (470L * GiB / 2 / 16)
    local.reason shouldBe
      "limit=503GiB heap=32GiB offheap=470GiB x0.5 / 16 slots -> 14.7GiB (auto)"
    // A pod of 8 GiB with a 4 GiB heap and 4 slots: (8 - 4 - 1) x 0.5 / 4.
    SearchResources
      .vectorsBudget(Some(8L * GiB), 4L * GiB, 4, None)
      .bytes shouldBe 384L * MiB
    // A container Spark sized with the default overhead: 4 GiB heap plus 410
    // MiB leaves nothing after the JVM's share, so the floor applies.
    val floored = SearchResources.vectorsBudget(
      Some(4L * GiB + 410L * MiB),
      4L * GiB,
      2,
      None
    )
    floored.bytes shouldBe SearchResources.MinVectorsBudgetBytes
    floored.reason should endWith("-> 64MiB (floor)")
  }

  test("without a memory limit the budget is the old 2 GiB default per slot") {
    SearchResources.vectorsBudget(None, 1L << 30, 4, None) shouldBe
      SearchResources.Choice(
        512L * MiB,
        "memory limit unknown -> 2GiB / 4 slots = 512MiB (default)"
      )
  }

  test("a configured vectors limit is per executor and divided by the slots") {
    SearchResources.vectorsBudget(
      Some(1L << 40),
      1L << 30,
      8,
      Some(1L << 31)
    ) shouldBe
      SearchResources.Choice(
        256L * MiB,
        s"2147483648 / 8 slots -> 256MiB (option ${MilvusOption.SearchVectorsMaxBytes})"
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
