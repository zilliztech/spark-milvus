package com.zilliz.spark.connector.read

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.index.SearchPlan
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.spark.connector.options.{MilvusOption, SearchLimits}

/** A query set that is Parquet files, recognized on the driver and read by the
  * tasks (docs/design/architecture/vector-search.html section 2.1).
  */
class QueryFilesTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var dir: Path = _

  private val layout = VectorLayout(VectorElementType.Float32, 2)
  private val total = 10
  private val many = 5000

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("query-files")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    dir = Files.createTempDirectory("query-files")
    // Two files, so that the sequence crosses a file boundary.
    frame(total).repartition(2).write.parquet(dir.resolve("set").toString)
    // One file of many small row groups, so that decoding runs on several
    // threads and groups straddle row groups.
    spark.sparkContext.hadoopConfiguration.setInt("parquet.block.size", 4096)
    spark.sparkContext.hadoopConfiguration.setInt("parquet.page.size", 1024)
    frame(many).coalesce(1).write.parquet(dir.resolve("many").toString)
    spark.sparkContext.hadoopConfiguration.unset("parquet.block.size")
    spark.sparkContext.hadoopConfiguration.unset("parquet.page.size")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val source = StructType(
    Seq(
      StructField("id", LongType),
      StructField("emb", ArrayType(FloatType))
    )
  )

  private def frame(count: Int): DataFrame = spark.createDataFrame(
    (0 until count)
      .map(index => Row(index.toLong, Seq(index.toFloat, -index.toFloat)))
      .asJava,
    source
  )

  /** What the benchmark hands the search: the columns cast and renamed. */
  private def selected(read: DataFrame): DataFrame = SearchQueries.selected(
    read.select(
      col("id").cast(LongType).as(SearchQueries.IdColumn),
      col("emb").cast(ArrayType(FloatType)).as(SearchQueries.VectorColumn)
    )
  )

  private def set: DataFrame = spark.read.parquet(dir.resolve("set").toString)

  /** The rows in the order the files give them: files by path, rows by file. */
  private def expectedRows(files: QueryFiles): Seq[Row] =
    files.files.flatMap(file =>
      selected(spark.read.parquet(file.path)).collect().toSeq
    )

  test(
    "a plain scan of parquet files is recognized, with its footers' counts"
  ) {
    val files = QueryFiles.of(spark, selected(set)).get
    files.files.size shouldBe 2
    files.files.map(_.path) shouldBe files.files.map(_.path).sorted
    files.files.map(_.rows).sum shouldBe total.toLong
    files.queries shouldBe total.toLong
    files.files.foreach(file => file.length should be > 0L)
  }

  test("the tasks pack the planned groups from the files, in file order") {
    val files = QueryFiles.of(spark, selected(set)).get
    val planned = Seq(
      SearchPlan.QueryGroup(0, 3),
      SearchPlan.QueryGroup(3, 4),
      SearchPlan.QueryGroup(7, 3)
    )
    val groups = files.groups(0 until 3, planned, layout, "L2").toSeq
    val rows = expectedRows(files)
    groups.map(_.queries) shouldBe Seq(3, 4, 3)
    groups.foreach(_.firstQuery shouldBe 0)
    groups.zip(planned).foreach { case (group, plan) =>
      val (ids, vectors) = SearchQueries.pack(
        rows.slice(plan.firstQuery, plan.untilQuery),
        layout,
        "L2"
      )
      group.ids.toSeq shouldBe ids.toSeq
      group.vectors.toSeq shouldBe vectors.toSeq
    }
    groups.flatMap(_.ids).sorted shouldBe (0L until total.toLong)
  }

  test("a range that starts later skips the rows before its first group") {
    val files = QueryFiles.of(spark, selected(set)).get
    val planned = Seq(SearchPlan.QueryGroup(0, 4), SearchPlan.QueryGroup(4, 6))
    val groups = files.groups(1 until 2, planned, layout, "L2").toSeq
    groups.size shouldBe 1
    groups.head.ids.toSeq shouldBe expectedRows(files).drop(4).map(_.getLong(0))
  }

  test("a plan that disagrees with the files is refused") {
    val files = QueryFiles.of(spark, selected(set)).get
    val planned = Seq(SearchPlan.QueryGroup(0, total + 1))
    val failure = the[IllegalArgumentException] thrownBy
      files.groups(0 until 1, planned, layout, "L2").toSeq
    failure.getMessage should include(s"planned as ${total + 1}")
  }

  test("a filtered, computed or converted frame is not a plain scan") {
    QueryFiles.of(
      spark,
      selected(set).filter(col(SearchQueries.IdColumn) > 2)
    ) shouldBe None
    QueryFiles.of(spark, selected(frame(total))) shouldBe None
    val ints = dir.resolve("ints").toString
    frame(total)
      .select(col("id").cast(IntegerType).as("id"), col("emb"))
      .write
      .parquet(ints)
    QueryFiles.of(spark, selected(spark.read.parquet(ints))) shouldBe None
    QueryFiles.of(spark, selected(set).limit(3)) shouldBe None
  }

  private def matrixRows(
      matrix: com.zilliz.milvus.storage.index.QueryMatrix
  ): Seq[Seq[Float]] =
    (0 until matrix.queries).map { row =>
      val buffer = matrix.buffer
      (0 until layout.dimension).map(d =>
        buffer.getFloat(row * layout.rowBytes + d * 4)
      )
    }

  test(
    "a file of many row groups decodes on several threads into the same groups as the packer"
  ) {
    val files = QueryFiles
      .of(spark, selected(spark.read.parquet(dir.resolve("many").toString)))
      .get
    files.rowGroups.size should be > 2
    files.rowGroups.map(_.rows).sum shouldBe many.toLong
    files.rowGroups.map(_.firstRow) shouldBe files.rowGroups
      .map(_.rows)
      .scanLeft(0L)(_ + _)
      .init
    val planned = Seq(
      SearchPlan.QueryGroup(0, 1200),
      SearchPlan.QueryGroup(1200, 1300),
      SearchPlan.QueryGroup(2500, 2500)
    )
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val decoded =
        files.decode(0 until 3, planned, layout, "L2", allocator, threads = 4)
      val packed = files.groups(0 until 3, planned, layout, "L2").toSeq
      try {
        decoded.size shouldBe 3
        decoded.zip(packed).foreach { case ((ids, matrix), group) =>
          ids.toSeq shouldBe group.ids.toSeq
          matrix.queries shouldBe group.queries
          val expected = (0 until group.queries).map(q =>
            (0 until layout.dimension).map(d =>
              java.nio.ByteBuffer
                .wrap(group.vectors)
                .order(java.nio.ByteOrder.nativeOrder())
                .getFloat(q * layout.rowBytes + d * 4)
            )
          )
          matrixRows(matrix) shouldBe expected
        }
      } finally decoded.foreach(_._2.close())
    } finally allocator.close()
  }

  test("a range in the middle decodes only its rows") {
    val files = QueryFiles
      .of(spark, selected(spark.read.parquet(dir.resolve("many").toString)))
      .get
    val planned = Seq(
      SearchPlan.QueryGroup(0, 2000),
      SearchPlan.QueryGroup(2000, 1500),
      SearchPlan.QueryGroup(3500, 1500)
    )
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val decoded =
        files.decode(1 until 2, planned, layout, "L2", allocator, threads = 3)
      try {
        decoded.size shouldBe 1
        decoded.head._1.toSeq shouldBe (2000L until 3500L)
        matrixRows(decoded.head._2).head shouldBe Seq(2000f, -2000f)
      } finally decoded.foreach(_._2.close())
    } finally allocator.close()
  }

  test(
    "a decode that disagrees with the plan releases its matrices and fails"
  ) {
    val files = QueryFiles.of(spark, selected(set)).get
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      val failure = the[IllegalArgumentException] thrownBy
        files.decode(
          0 until 1,
          Seq(SearchPlan.QueryGroup(0, total + 3)),
          layout,
          "L2",
          allocator,
          threads = 2
        )
      failure.getMessage should include("planned as")
      allocator.getAllocatedMemory shouldBe 0L
    } finally allocator.close()
  }

  test("the direct read is on by default and switched by its option") {
    SearchLimits.from(Map.empty).queriesDirect shouldBe true
    SearchLimits
      .from(Map(MilvusOption.SearchQueriesDirect -> "false"))
      .queriesDirect shouldBe false
    the[IllegalArgumentException] thrownBy SearchLimits.from(
      Map(MilvusOption.SearchQueriesDirect -> "maybe")
    )
  }
}
