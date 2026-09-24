package com.zilliz.spark.connector.read

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

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
