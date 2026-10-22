package com.zilliz.spark.connector.search

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** How the take stage spreads its hits over tasks so that what one task buffers
  * fits its heap.
  */
class SearchTakeTest extends AnyFunSuite with Matchers {
  private val MiB = 1L << 20

  test(
    "requested positions preserve names, order and duplicates across reused rows"
  ) {
    val schema = StructType(
      SearchMerge.HitSchema.fields ++ Array(
        StructField("query_id", LongType, nullable = false),
        StructField("_score", StringType, nullable = false)
      )
    )
    val reused = new GenericInternalRow(schema.size)
    val rows = Iterator(1L, 2L).map { id =>
      reused.setLong(0, id)
      reused.setLong(1, 10L + id)
      reused.setLong(2, 20L + id)
      reused.setLong(3, 100L + id)
      reused.update(4, UTF8String.fromString(s"value$id"))
      reused
    }
    val found = SearchTake.project(rows, schema, Seq(4, 1, 3, 4)).toVector
    found.map { case (id, row) =>
      (
        id,
        row.getUTF8String(0).toString,
        row.getLong(1),
        row.getLong(2),
        row.getUTF8String(3).toString
      )
    } shouldBe Vector(
      (1L, "value1", 11L, 101L, "value1"),
      (2L, "value2", 12L, 102L, "value2")
    )

    val absent = new GenericInternalRow(schema.size)
    absent.setLong(0, 3L)
    val missing =
      SearchTake.project(Iterator(absent), schema, Seq(1, 3, 4)).next()
    missing._1 shouldBe 3L
    (0 until 3).forall(missing._2.isNullAt) shouldBe true
    val empty =
      SearchTake.project(Iterator(reused, absent), schema, Seq.empty).toVector
    empty.map { case (id, row) => id -> row.numFields } shouldBe Vector(
      2L -> 0,
      3L -> 0
    )
  }

  test(
    "a range bucket is a run of consecutive rows, never past the last bucket"
  ) {
    SearchTake.rangeBucket(0L, 1000L, 4) shouldBe 0
    SearchTake.rangeBucket(249L, 1000L, 4) shouldBe 0
    SearchTake.rangeBucket(250L, 1000L, 4) shouldBe 1
    SearchTake.rangeBucket(999L, 1000L, 4) shouldBe 3
    // an offset at or past the recorded row count stays in the last bucket
    SearchTake.rangeBucket(1000L, 1000L, 4) shouldBe 3
    SearchTake.rangeBucket(5L, 1000L, 1) shouldBe 0
    // 10^8 rows in 109 buckets, as the L x 4 k=100,000 plan cut them
    SearchTake.rangeBucket(99999999L, 100000000L, 109) shouldBe 108
    an[IllegalArgumentException] should be thrownBy
      SearchTake.rangeBucket(1L, 0L, 4)
  }

  test("a small search keeps the session's shuffle partitions and one bucket") {
    // 100k queries at k=100: 10 million hits, 292 MiB of heap a task holds
    // 1.19 million rows at half the budget, so 9 tasks would do; 16 stay.
    val p = SearchExecutionPlan.takePartitioning(10000000L, 74, 292L * MiB, 16)
    p shouldBe SearchTake.Partitioning(16, 1)
  }

  test("a large k cuts the segments into buckets so a task's hits fit") {
    // 100k queries at k=10,000: a billion hits. 292 MiB a task, 128 bytes a
    // hit, half for skew: 1,196,032 rows a task -> 837 tasks, 12 buckets over
    // 74 segments. The failed run had 16 tasks of 62.5 million hits each.
    val p =
      SearchExecutionPlan.takePartitioning(1000000000L, 74, 292L * MiB, 16)
    p.partitions shouldBe 837
    p.buckets shouldBe 12
    (1000000000L / p.partitions) should be < (292L * MiB / SearchTake.HitRowBytes)
  }

  test(
    "the partition count never drops below the minimum or the buckets below one"
  ) {
    SearchExecutionPlan.takePartitioning(
      0L,
      74,
      292L * MiB,
      16
    ) shouldBe SearchTake
      .Partitioning(16, 1)
    SearchExecutionPlan.takePartitioning(1L, 1, 0L, 3) shouldBe SearchTake
      .Partitioning(3, 3)
  }

  test("an empty plan or budget is refused before it reaches the shuffle") {
    an[IllegalArgumentException] should be thrownBy SearchExecutionPlan
      .takePartitioning(
        10L,
        0,
        1L,
        1
      )
    an[IllegalArgumentException] should be thrownBy SearchExecutionPlan
      .takePartitioning(
        10L,
        1,
        1L,
        0
      )
    an[IllegalArgumentException] should be thrownBy SearchTake.Partitioning(
      0,
      1
    )
  }
}
