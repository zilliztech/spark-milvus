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
  * group on the executors, then delivered whole to every segment set, so a task
  * reads its segments once and answers every group on them (section 2.1 of
  * docs/design/architecture/vector-search.html).
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
    arrowMaxBytes = 1L << 20
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
}
