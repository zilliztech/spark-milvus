package com.zilliz.spark.connector.search

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}

import org.apache.spark.resource.ResourceProfile
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.index.SearchPlan
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.spark.connector.options.TaskResources

class SearchExecutionPlanTest extends AnyFunSuite with Matchers {
  private val layout = VectorLayout(VectorElementType.Float32, 2)

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

  test("index prefetch keeps the existing two-loaded-index capacity boundary") {
    // Each loaded index retains two copies; current loading adds no copy.
    val sizes = Seq(Some(1000L), Some(900L))
    SearchExecutionPlan.prefetchIndexes(true, sizes, 3999L) shouldBe false
    SearchExecutionPlan.prefetchIndexes(true, sizes, 4000L) shouldBe true
  }

  test("exact mode and any missing or unknown index size disable prefetch") {
    val sizes = Seq(Some(1000L), Some(900L))
    SearchExecutionPlan.prefetchIndexes(false, sizes, 10000L) shouldBe false
    SearchExecutionPlan.prefetchIndexes(
      true,
      sizes.take(1),
      10000L
    ) shouldBe false
    SearchExecutionPlan.prefetchIndexes(
      true,
      Seq(Some(1000L), None),
      10000L
    ) shouldBe false
    SearchExecutionPlan.prefetchIndexes(
      true,
      Seq(Some(1000L), Some(0L)),
      10000L
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
      prefetchIndexes = false
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
