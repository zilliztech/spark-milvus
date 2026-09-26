package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** How the take stage spreads its hits over tasks so that what one task
  * buffers fits its heap (docs/design/architecture/vector-search.html section
  * 2.1, 2026-09-25 decision).
  */
class SearchTakeTest extends AnyFunSuite with Matchers {
  private val MiB = 1L << 20

  test("a small search keeps the session's shuffle partitions and one bucket") {
    // 100k queries at k=100: 10 million hits, 292 MiB of heap a task holds
    // 1.19 million rows at half the budget, so 9 tasks would do; 16 stay.
    val p = SearchTake.partitioning(10000000L, 74, 292L * MiB, 16)
    p shouldBe SearchTake.Partitioning(16, 1)
  }

  test("a large k cuts the segments into buckets so a task's hits fit") {
    // 100k queries at k=10,000: a billion hits. 292 MiB a task, 128 bytes a
    // hit, half for skew: 1,196,032 rows a task -> 837 tasks, 12 buckets over
    // 74 segments. The failed run had 16 tasks of 62.5 million hits each.
    val p = SearchTake.partitioning(1000000000L, 74, 292L * MiB, 16)
    p.partitions shouldBe 837
    p.buckets shouldBe 12
    (1000000000L / p.partitions) should be < (292L * MiB / SearchTake.HitRowBytes)
  }

  test("the partition count never drops below the minimum or the buckets below one") {
    SearchTake.partitioning(0L, 74, 292L * MiB, 16) shouldBe SearchTake.Partitioning(16, 1)
    SearchTake.partitioning(1L, 1, 0L, 3) shouldBe SearchTake.Partitioning(3, 3)
  }

  test("an empty plan or budget is refused before it reaches the shuffle") {
    an[IllegalArgumentException] should be thrownBy SearchTake.partitioning(10L, 0, 1L, 1)
    an[IllegalArgumentException] should be thrownBy SearchTake.partitioning(10L, 1, 1L, 0)
    an[IllegalArgumentException] should be thrownBy SearchTake.Partitioning(0, 1)
  }
}
