package com.zilliz.spark.connector.options

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.index.MachineResources

/** The exact scan's base block for a machine and a concurrency
  * (docs/design/architecture/search-resources.html section 3.2). The rows are
  * the design's worked examples on the measured machine: two sockets of 48 MiB
  * L3, 128 CPUs.
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
