package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  LongType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.index.SearchPlan
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

/** How a query set too large to broadcast reaches the first stage: packed by
  * group on the executors, then delivered range by range to every segment set,
  * so a task reads its segments once and answers every group of its range on
  * them (section 2.1 of docs/design/architecture/vector-search.html).
  */
class SearchDeliveryTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("search-delivery")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val layout = VectorLayout(VectorElementType.Float32, 2)

  private val spec = SegmentSetSearch.Spec(
    vectorColumn = "v",
    layout = layout,
    fieldId = 100L,
    nullable = false,
    k = 1,
    metric = "L2",
    mode = "exact",
    filter = None,
    parameters = Map.empty,
    allowUnindexed = false,
    vectorsMaxBytes = 1L << 20,
    arrowMaxBytes = 1L << 20,
    slots = 2,
    batchMaxBytes = None
  )

  /** Eight queries the planner splits into four groups of two. */
  private val queries = 8
  private val groups: Seq[SearchPlan.QueryGroup] = SearchPlan.groups(
    queries,
    layout,
    spec.k,
    SearchPlan.bytesPerQuery(layout, spec.k) * 2
  )

  private def selected: org.apache.spark.sql.DataFrame =
    spark.createDataFrame(
      (0 until queries)
        .map(index =>
          Row(index.toLong, Seq(index.toFloat, index.toFloat + 0.5f))
        )
        .asJava,
      StructType(
        Seq(
          StructField(SearchQueries.IdColumn, LongType),
          StructField(SearchQueries.VectorColumn, ArrayType(FloatType))
        )
      )
    )

  test("the packed query set travels as one partition, so one task per set") {
    groups.size shouldBe 4

    val delivered =
      MilvusSearch.packedGroups(
        selected,
        SearchPlan.Plan(Seq.empty, groups),
        spec,
        layout
      )
    delivered.getNumPartitions shouldBe 1

    val sets = spark.sparkContext.parallelize(Seq(10, 20, 30), 3)
    val paired = sets.cartesian(delivered)
    paired.getNumPartitions shouldBe 3
    paired
      .mapPartitions(pairs => Iterator(pairs.size))
      .collect()
      .toSeq shouldBe Seq(4, 4, 4)
  }

  test("every task of one executor reads the same packed bytes, not its own") {
    val delivered =
      MilvusSearch.packedGroups(
        selected,
        SearchPlan.Plan(Seq.empty, groups),
        spec,
        layout
      )
    val sets = spark.sparkContext.parallelize(Seq(10, 20, 30), 3)
    // Identity, not equality: a recomputed right side is equal to the first
    // one and costs a second copy of every byte. These tasks share a JVM, so
    // the same array is the same array.
    val seen = sets
      .cartesian(delivered)
      .map { case (_, group) => System.identityHashCode(group.vectors) }
      .collect()
      .toSeq
    seen.size shouldBe sets.getNumPartitions * groups.size
    seen.distinct.size shouldBe groups.size
  }

  test("a task learns its segment set without holding a query group") {
    // Through the cartesian, a task reads its first pair to learn which set
    // it has, and a buffered iterator keeps that pair -- and the group in it
    // -- for as long as the task runs. Beside it, nothing of the pair
    // survives the group it carried.
    val delivered =
      MilvusSearch.packedGroups(
        selected,
        SearchPlan.Plan(Seq.empty, groups),
        spec,
        layout
      )
    val slots = spark.sparkContext.parallelize(0 until 3, 3)
    val seen = slots
      .cartesian(delivered)
      .mapPartitionsWithIndex { (index, pairs) =>
        // The index is the left partition's, which is the segment set's, and
        // it is known before a pair is read.
        Iterator((index, pairs.map(_._2.queries).sum))
      }
      .collect()
      .toSeq
      .sortBy(_._1)

    seen.map(_._1) shouldBe Seq(0, 1, 2)
    // Every task sees every group: four groups of two queries each.
    seen.map(_._2) shouldBe Seq(queries, queries, queries)
  }

  test("packing by group keeps every query, in the planner's order") {
    val delivered =
      MilvusSearch.packedGroups(
        selected,
        SearchPlan.Plan(Seq.empty, groups),
        spec,
        layout
      )
    val packed = delivered.collect().toSeq
    packed.size shouldBe groups.size
    packed.foreach(_.queries shouldBe 2)
    packed.flatMap(_.ids).sorted shouldBe (0L until queries.toLong)
  }

  test("a plan cut into query ranges travels as one partition per range") {
    val ranges = Seq(0 until 2, 2 until 4)
    val delivered =
      MilvusSearch.packedGroups(
        selected,
        SearchPlan.Plan(Seq.empty, groups, Seq.empty, ranges),
        spec,
        layout
      )
    delivered.getNumPartitions shouldBe 2
    // Partition r holds the groups of range r, in order; a group is known by
    // its first query id, since a delivered group starts at zero in its bytes.
    delivered
      .mapPartitions(packed => Iterator(packed.map(_.ids.head).toSeq))
      .collect()
      .toSeq shouldBe Seq(Seq(0L, 2L), Seq(4L, 6L))

    val sets = spark.sparkContext.parallelize(Seq(10, 20, 30), 3)
    val paired = sets.cartesian(delivered)
    paired.getNumPartitions shouldBe 6
    // Task `set × ranges + range` pairs that set with that range's groups.
    paired
      .mapPartitionsWithIndex { (index, pairs) =>
        val seen = pairs.toSeq
        Iterator(
          (
            index,
            seen.map(_._1).distinct,
            seen.map(_._2.ids.head)
          )
        )
      }
      .collect()
      .toSeq shouldBe Seq(
      (0, Seq(10), Seq(0L, 2L)),
      (1, Seq(10), Seq(4L, 6L)),
      (2, Seq(20), Seq(0L, 2L)),
      (3, Seq(20), Seq(4L, 6L)),
      (4, Seq(30), Seq(0L, 2L)),
      (5, Seq(30), Seq(4L, 6L))
    )
  }
}
