package com.zilliz.spark.connector.search

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  LongType,
  StructField,
  StructType
}
import org.apache.spark.storage.StorageLevel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.index.SearchPlan
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.spark.connector.options.TaskResources

/** How a query set reaches the first stage. Broadcast whole, or, when it is too
  * large for that, packed by group on the executors into a shuffle, one
  * partition per group, and read by every first-stage task one group at a time,
  * so a task holds the group it searches and the one being read, never the
  * query set (section 2.1 of docs/design/architecture/vector-search.html).
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

  private val request = SegmentSetSearch.Request(
    vectorColumn = "v",
    layout = layout,
    fieldId = 100L,
    nullable = false,
    k = 1,
    metric = MetricType.L2,
    mode = SearchMode.Exact,
    filter = None,
    parameters = Map.empty,
    arrowMaxBytes = 1L << 20
  )

  /** Eight queries the planner splits into four groups of two. */
  private val queries = 8
  private val groups: Seq[SearchPlan.QueryGroup] = SearchPlan.groups(
    queries,
    layout,
    request.k,
    SearchPlan.bytesPerQuery(layout, request.k) * 2
  )

  private def rows(count: Int = queries): Seq[Row] =
    (0 until count).map(index =>
      Row(index.toLong, Seq(index.toFloat, index.toFloat + 0.5f))
    )

  private def selected(count: Int = queries): DataFrame =
    spark.createDataFrame(
      rows(count).asJava,
      StructType(
        Seq(
          StructField(SearchQueries.IdColumn, LongType),
          StructField(SearchQueries.VectorColumn, ArrayType(FloatType))
        )
      )
    )

  /** `rows` packed as an executor packs a partition of them. */
  private def pack(
      rows: Seq[Row],
      metric: MetricType = request.metric
  ): (Array[Long], Array[Byte]) =
    SearchQueries.packPartition(
      rows.iterator.map(row =>
        InternalRow(
          row.getLong(0),
          UnsafeArrayData.fromPrimitiveArray(row.getSeq[Float](1).toArray)
        )
      ),
      layout,
      metric
    )

  private def packed(): RDD[SearchQueries.Group] =
    SearchQueries.packedGroups(selected(), groups, request.metric, layout)

  /** Checks that `delivered` holds one partition per group of `planned`, each
    * with its queries in order, starting at zero in its bytes.
    */
  private def deliversGroups(
      delivered: RDD[SearchQueries.Group],
      planned: Seq[SearchPlan.QueryGroup],
      count: Int
  ): Unit = {
    delivered.getNumPartitions shouldBe planned.size
    val seen = delivered
      .mapPartitionsWithIndex { (index, packed) =>
        packed.map(group =>
          (index, group.ids.toSeq, group.vectors.toSeq, group.firstQuery)
        )
      }
      .collect()
      .toSeq
    seen.map(_._1) shouldBe planned.indices
    planned.zip(seen).foreach { case (group, (_, ids, vectors, first)) =>
      val expected = rows(count).slice(group.firstQuery, group.untilQuery)
      val (expectedIds, expectedVectors) = pack(expected)
      ids shouldBe expectedIds.toSeq
      vectors shouldBe expectedVectors.toSeq
      first shouldBe 0
    }
  }

  test("packing gives one partition per group, each its queries in order") {
    groups.size shouldBe 4
    deliversGroups(packed(), groups, queries)
  }

  test("packing follows the planned groups when the longer ones come first") {
    // An even cut of ten queries into four groups (decision 32): a group's
    // start says which queries it takes, not its index times the size of the
    // first group, which puts query 8 in the third group.
    val even = SearchPlan.evenGroups(10, 4)
    even.map(_.queries) shouldBe Seq(3, 3, 2, 2)
    val delivered =
      SearchQueries.packedGroups(selected(10), even, request.metric, layout)
    deliversGroups(delivered, even, 10)
  }

  test("the packed groups are a shuffle's output, and nothing is stored") {
    val delivered = packed()
    delivered.getStorageLevel shouldBe StorageLevel.NONE
    // One narrow step above the shuffle that keys each packed group by its
    // group, so a group is read back from the executor that packed it.
    val shuffled = delivered.dependencies.head.rdd
    shuffled shouldBe a[ShuffledRDD[_, _, _]]
    shuffled.getNumPartitions shouldBe groups.size
  }

  test("a first-stage task reads the groups of its range, in order") {
    val ranges = Seq(0 until 2, 2 until 4)
    val streamed = new SearchQueryRanges(packed(), 3, ranges)
    streamed.getNumPartitions shouldBe 6
    // Task `set × ranges + range` holds that range's groups; a group is known
    // by its first query id, since a packed group starts at zero in its bytes.
    streamed
      .mapPartitionsWithIndex { (index, delivered) =>
        Iterator((index, delivered.map(_.ids.head).toSeq))
      }
      .collect()
      .toSeq shouldBe Seq(
      (0, Seq(0L, 2L)),
      (1, Seq(4L, 6L)),
      (2, Seq(0L, 2L)),
      (3, Seq(4L, 6L)),
      (4, Seq(0L, 2L)),
      (5, Seq(4L, 6L))
    )
  }

  test("one range covers every group, which is what an index search reads") {
    val streamed =
      new SearchQueryRanges(packed(), 2, Seq(groups.indices))
    streamed.getNumPartitions shouldBe 2
    streamed
      .mapPartitions(delivered => Iterator(delivered.map(_.queries).sum))
      .collect()
      .toSeq shouldBe Seq(queries, queries)
  }

  test("a group's reader opens when the group before it is handed on") {
    SearchDeliveryTest.opened.clear()
    val parent = new SearchDeliveryTest.Recorded(spark.sparkContext, 3)
    val steps = new SearchQueryRanges(parent, 1, Seq(0 until 3))
      .mapPartitions { delivered =>
        // Read in the task, which runs in this JVM: before the first group is
        // taken, the first reader is open; each group taken opens the next.
        val seen = Seq.newBuilder[Seq[Int]]
        seen += SearchDeliveryTest.opened.asScala.toSeq.map(_.intValue)
        while (delivered.hasNext) {
          delivered.next()
          seen += SearchDeliveryTest.opened.asScala.toSeq.map(_.intValue)
        }
        Iterator(seen.result())
      }
      .collect()
      .head
    steps shouldBe Seq(Seq(0), Seq(0, 1), Seq(0, 1, 2), Seq(0, 1, 2))
  }

  test("a range must cover the packed groups in order") {
    val parent = new SearchDeliveryTest.Recorded(spark.sparkContext, 3)
    an[IllegalArgumentException] should be thrownBy
      new SearchQueryRanges(parent, 1, Seq(0 until 2))
    an[IllegalArgumentException] should be thrownBy
      new SearchQueryRanges(parent, 1, Seq(1 until 3, 0 until 1))
  }

  private def packedById(
      ids: Array[Long],
      vectors: Array[Byte]
  ): Map[Long, Seq[Byte]] =
    ids.zipWithIndex.map { case (id, index) =>
      id -> vectors
        .slice(index * layout.rowBytes, (index + 1) * layout.rowBytes)
        .toSeq
    }.toMap

  test("the broadcast path packs on the executors what one pass would pack") {
    val spread = selected().repartition(3)

    val (ids, vectors) =
      SearchQueries.packOnExecutors(spread, layout, MetricType.L2)
    val (driverIds, driverVectors) = pack(rows(), MetricType.L2)

    ids.length shouldBe queries
    ids.toSet shouldBe (0L until queries.toLong).toSet
    vectors.length shouldBe queries * layout.rowBytes
    packedById(ids, vectors) shouldBe packedById(driverIds, driverVectors)
  }

  test("executor packing keeps partition order and crosses chunk boundaries") {
    val many = 5000
    val frame = spark
      .createDataFrame(
        (0 until many)
          .map(index => Row(index.toLong, Seq(index.toFloat, -index.toFloat)))
          .asJava,
        selected().schema
      )
      .coalesce(1)

    val (ids, vectors) =
      SearchQueries.packOnExecutors(frame, layout, MetricType.L2)
    val (driverIds, driverVectors) =
      pack(frame.collect().toSeq, MetricType.L2)

    ids.toSeq shouldBe driverIds.toSeq
    vectors.toSeq shouldBe driverVectors.toSeq
  }

  test("executor packing refuses a value that is not finite") {
    val bad = spark.createDataFrame(
      Seq(
        Row(1L, Seq(1.0f, 2.0f)),
        Row(2L, Seq(Float.NaN, 0.0f))
      ).asJava,
      selected().schema
    )

    val failure = the[org.apache.spark.SparkException] thrownBy
      SearchQueries.packOnExecutors(bad, layout, MetricType.L2)

    failure.getMessage should include("not finite")
  }

  test(
    "delivery broadcasts at the byte limit and always preserves driver-packed input"
  ) {
    val frame = SearchQueries.Frame(selected(), queries.toLong)
    val bytes = SearchQueries.bytes(frame.count, layout)
    SearchExecutionPlan.queryDelivery(frame, layout, bytes) shouldBe
      SearchQueries.Broadcast(frame.selected)
    SearchExecutionPlan.queryDelivery(frame, layout, bytes - 1L) shouldBe
      SearchQueries.Shuffled(frame.selected)
    val (ids, vectors) = pack(rows())
    val packedInput = SearchQueries.Packed(ids, vectors)
    SearchExecutionPlan.queryDelivery(
      packedInput,
      layout,
      1L
    ) should be theSameInstanceAs packedInput
  }

  test("every delivery hands SearchQueryRanges the same groups and bytes") {
    val (driverIds, driverVectors) = pack(rows())
    val frame = SearchQueries.Frame(selected(), queries.toLong)
    val packedInput = SearchQueries.Packed(driverIds, driverVectors)
    val deliveries = Seq(
      SearchExecutionPlan.queryDelivery(packedInput, layout, 0L),
      SearchExecutionPlan.queryDelivery(frame, layout, Long.MaxValue),
      SearchExecutionPlan.queryDelivery(frame, layout, 0L)
    )
    deliveries.map(_.describe) shouldBe Seq(
      "broadcast, packed on the driver",
      "broadcast",
      "shuffle"
    )
    val ranges = Seq(0 until 2, 2 until 4)
    val resources = TaskResources.of(spark)
    val rowBytes = layout.rowBytes
    val results = deliveries.map { delivery =>
      val delivered = delivery.groups(
        spark,
        groups,
        request.metric,
        layout,
        if (delivery.shuffled)
          SearchExecutionPlan
            .packingProfile(groups, layout, resources, 1L << 20)
        else None
      )
      new SearchQueryRanges(delivered, 2, ranges)
        .mapPartitionsWithIndex { (task, streamed) =>
          Iterator(
            task -> streamed.map { group =>
              val start = group.firstQuery * rowBytes
              val end = (group.firstQuery + group.queries) * rowBytes
              (group.ids.toSeq, group.vectors.slice(start, end).toSeq)
            }.toSeq
          )
        }
        .collect()
        .toSeq
    }
    results(1) shouldBe results.head
    results(2) shouldBe results.head
  }
}

object SearchDeliveryTest {

  /** The group partitions opened so far, in order. */
  val opened = new ConcurrentLinkedQueue[Integer]()
  final case class Slot(index: Int) extends Partition

  /** Query groups of one query each that record when they are opened. */
  final class Recorded(sc: SparkContext, groups: Int)
      extends RDD[SearchQueries.Group](sc, Nil) {
    override protected def getPartitions: Array[Partition] =
      Array.tabulate[Partition](groups)(Slot)
    override def compute(
        split: Partition,
        context: TaskContext
    ): Iterator[SearchQueries.Group] = {
      opened.add(split.index)
      Iterator(
        SearchQueries.Group(Array(split.index.toLong), new Array[Byte](8), 0)
      )
    }
  }
}
