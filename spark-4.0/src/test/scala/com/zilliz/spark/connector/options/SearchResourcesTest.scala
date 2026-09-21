package com.zilliz.spark.connector.options

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.index.MachineResources

/** The exact scan's base block for a machine and a concurrency, and the vectors
  * a task keeps for an executor's memory
  * (docs/design/architecture/search-resources.html sections 3.2 and 3.3). The
  * rows are the design's worked examples on the measured machine: two sockets
  * of 48 MiB L3, 128 CPUs, 503 GiB.
  */
class SearchResourcesTest extends AnyFunSuite with Matchers {

  private val MiB = 1L << 20
  private val measured =
    MachineResources(Some(96L * MiB), Some(128), 128, Some(503L << 30))

  test("the block shrinks with the tasks that share the executor") {
    SearchResources.exactScanBatch(measured, 16, None).bytes shouldBe 2 * MiB
    SearchResources.exactScanBatch(measured, 4, None).bytes shouldBe 8 * MiB
    SearchResources.exactScanBatch(measured, 64, None).bytes shouldBe 1 * MiB
    SearchResources.exactScanBatch(measured, 1, None).bytes shouldBe 32 * MiB
    SearchResources.exactScanBatch(measured, 16, None).reason shouldBe
      "L3=96MiB quota=128/128 slots=16 -> 2MiB (auto)"
  }

  test("a CPU quota scales the usable cache down") {
    val pod = measured.copy(availableCpus = 8)
    SearchResources.exactScanBatch(pod, 4, None).bytes shouldBe 1 * MiB
    SearchResources.exactScanBatch(pod, 4, None).reason should include(
      "quota=8/128"
    )
    val small =
      MachineResources(Some(32L * MiB), Some(32), 32, Some(64L << 30))
    SearchResources.exactScanBatch(small, 8, None).bytes shouldBe 2 * MiB
  }

  test(
    "without L3 the block is 4 MiB; without a quota the cache share halves"
  ) {
    val blind = MachineResources(None, None, 16, None)
    SearchResources.exactScanBatch(blind, 16, None) shouldBe
      SearchResources.Choice(4 * MiB, "L3 unknown -> 4MiB (default)")
    val noQuota = measured.copy(hostCpus = None)
    // 96 MiB x 0.3 / 16 = 1.8 MiB -> 1 MiB
    SearchResources.exactScanBatch(noQuota, 16, None).bytes shouldBe 1 * MiB
    SearchResources.exactScanBatch(noQuota, 16, None).reason should include(
      "quota=128/unknown"
    )
  }

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
      "limit=503GiB heap=32GiB offheap=470GiB x0.5 / 16 slots -> 15040MiB (auto)"
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

  test("a configured value is used as it is and says so") {
    SearchResources.exactScanBatch(measured, 64, Some(33554432L)) shouldBe
      SearchResources.Choice(
        33554432L,
        s"33554432 (option ${MilvusOption.ReadBatchMaxBytes})"
      )
  }

  test("the block stays inside its bounds and on a power of two") {
    val huge = MachineResources(Some(1L << 40), Some(1), 1, None)
    SearchResources.exactScanBatch(huge, 1, None).bytes shouldBe 32 * MiB
    val tiny = MachineResources(Some(1L * MiB), Some(1), 1, None)
    SearchResources.exactScanBatch(tiny, 1, None).bytes shouldBe 1 * MiB
    // 96 MiB x 0.6 / 3 = 19.2 MiB -> 16 MiB
    SearchResources.exactScanBatch(measured, 3, None).bytes shouldBe 16 * MiB
  }
}
