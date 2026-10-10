package com.zilliz.spark.connector.implicits

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{call_function, col}
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.spark.connector.extensions.{
  MilvusNearestByJoinExec,
  MilvusSparkPlugin,
  MilvusSparkSessionExtensions
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.testkit.LocalMilvusCollection
import com.zilliz.spark.connector.testkit.LocalMilvusCollection.{
  Entity,
  Segment
}

/** `buildIndex` and `writeSnapshot` on a Milvus table's DataFrame, and the
  * `table` argument of `CALL milvus.system.build_index` and `write_snapshot`
  * they run through (docs/design/architecture/dataframe-api.html section 9), on
  * every Spark line.
  *
  * The collection is two local segments of eight rows, each with an L2 HNSW
  * index; the builds here are for the inner product, so a search through the
  * snapshot a build delivers shows whether the new indexes are the ones used.
  */
class MilvusDataFrameTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with AdaptiveSparkPlanHelper {

  private val line =
    org.apache.spark.SPARK_VERSION.split('.').take(2).mkString(".")

  /** Arrow 12, which Spark 3.5 ships, cannot allocate on JDK 21. */
  private val arrowAllocates =
    line != "3.5" || Runtime.version().feature() < 21

  private var available = true
  private var directory: Path = _
  private var options: Map[String, String] = Map.empty
  private var spark: SparkSession = _

  private val segments = Seq(30L, 31L).map(segment =>
    Segment(
      segment,
      (0 until 8).map(row =>
        Entity(
          segment * 100 + row,
          Array(row.toFloat + 1f, (segment - 29).toFloat),
          Some(row % 2L)
        )
      )
    )
  )

  override def beforeAll(): Unit = {
    try {
      NativeStorageLibrary.load()
      NativeVectorLibrary.load()
    } catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError |
          _: RuntimeException =>
        available = false
    }
    if (available && arrowAllocates) {
      directory = Files.createTempDirectory("milvus-dataframe-")
      options = LocalMilvusCollection.write(directory, 2, segments)
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("milvus-dataframe")
        .config(
          "spark.sql.extensions",
          classOf[MilvusSparkSessionExtensions].getName
        )
        .config("spark.plugins", classOf[MilvusSparkPlugin].getName)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
    }
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    if (directory != null) {
      val paths = Files.walk(directory)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }

  private def needsNative(): Unit = {
    assume(available, "the native libraries are not on this machine")
    assume(
      arrowAllocates,
      "Spark 3.5's Arrow 12 cannot allocate on JDK 21: name a JDK 17 in SPARK35_TEST_JAVA_HOME"
    )
  }

  private def docs(read: Map[String, String] = Map.empty): DataFrame =
    spark.read.format("milvus").options(options ++ read).load()

  /** The connection and storage options as backquoted CALL arguments. */
  private def optionArguments: String =
    options.toSeq.sorted
      .map { case (key, value) => s"`$key` => '$value'" }
      .mkString(", ")

  /** What a build_index row says about a segment, without the job id, which
    * names the call, and the byte count, which the index's own layout decides.
    */
  private def built(frame: DataFrame): Seq[(Long, Long, Long, Int, Long)] =
    frame
      .collect()
      .map(row =>
        (
          row.getAs[Long]("segment_id"),
          row.getAs[Long]("partition_id"),
          row.getAs[Long]("row_count"),
          row.getAs[Int]("objects"),
          row.getAs[Long]("build_id")
        )
      )
      .toSeq
      .sorted

  private def jobOf(frame: DataFrame): String =
    frame.select("job_id").distinct().collect().map(_.getString(0)) match {
      case Array(job) => job
      case jobs => fail(s"one build is one job, not ${jobs.mkString(", ")}")
    }

  private def failure(run: => Any): String = {
    val thrown = the[Exception] thrownBy run
    Iterator
      .iterate[Throwable](thrown)(_.getCause)
      .takeWhile(_ != null)
      .map(e => String.valueOf(e.getMessage))
      .mkString(" ")
  }

  private def temporaryViews: Seq[String] =
    spark.catalog
      .listTables()
      .collect()
      .map(_.name)
      .filter(_.startsWith("milvus_input_"))
      .toSeq

  test("an argument is written as a constant of the CALL grammar") {
    MilvusDataFrame.constant("""it's \ x""") shouldBe """'it\'s \\ x'"""
    MilvusDataFrame.constant(java.lang.Long.valueOf(7)) shouldBe "7"
    MilvusDataFrame.constant(Integer.valueOf(-3)) shouldBe "-3"
    MilvusDataFrame.constant(java.lang.Boolean.TRUE) shouldBe "true"
  }

  test(
    "buildIndex gives the rows a CALL on the table or the collection gives"
  ) {
    needsNative()
    val coll = docs()
    val byMethod = coll.buildIndex(
      "vector",
      "built/method",
      indexType = "HNSW",
      metric = "IP",
      params = Map("M" -> "8", "efConstruction" -> "64"),
      buildId = 7001L
    )
    coll.createOrReplaceTempView("docs_for_call")
    val byTable = spark.sql(
      "CALL milvus.system.build_index(table => 'docs_for_call', field => 'vector', " +
        "output => 'built/table', index_type => 'HNSW', metric => 'IP', " +
        "params => 'M=8,efConstruction=64', build_id => 7001)"
    )
    val byCollection = spark.sql(
      "CALL milvus.system.build_index(collection => 'local_collection', field => 'vector', " +
        "output => 'built/collection', index_type => 'HNSW', metric => 'IP', " +
        s"params => 'M=8,efConstruction=64', build_id => 7001, $optionArguments)"
    )

    built(byMethod).map(row => (row._1, row._2, row._3, row._5)) shouldBe Seq(
      (30L, 20L, 8L, 7001L),
      (31L, 20L, 8L, 7001L)
    )
    built(byMethod).foreach(_._4 should be > 0)
    built(byTable) shouldBe built(byMethod)
    built(byCollection) shouldBe built(byMethod)
    byMethod.collect().foreach(_.getAs[Long]("bytes") should be > 0L)
    temporaryViews shouldBe empty
  }

  test("writeSnapshot delivers the build, and APPROX searches its indexes") {
    needsNative()
    val coll = docs()
    val job = jobOf(coll.buildIndex("vector", "built/delivered", metric = "IP"))
    val written = coll
      .writeSnapshot(
        job,
        "built/delivered",
        snapshotId = 5001L,
        snapshotName = "delivered",
        restorable = false
      )
      .collect()
      .toSeq
    written.map(row =>
      (
        row.getAs[Long]("snapshot_id"),
        row.getAs[String]("snapshot_name"),
        row.getAs[Int]("segments"),
        row.getAs[Int]("indexes")
      )
    ) shouldBe Seq((5001L, "delivered", 2, 2))

    // The same write through CALL, to another prefix.
    coll.createOrReplaceTempView("docs_for_write")
    val byCall = spark
      .sql(
        s"CALL milvus.system.write_snapshot(table => 'docs_for_write', job => '$job', " +
          "input => 'built/delivered', output => 'built/delivered-call', " +
          "snapshot_id => 5001, snapshot_name => 'delivered', restorable => false)"
      )
      .head()
    (byCall.getAs[Int]("segments"), byCall.getAs[Int]("indexes")) shouldBe
      ((2, 2))

    val delivered =
      docs(
        Map(MilvusOption.SnapshotPath -> written.head.getAs[String]("snapshot"))
      )
    delivered.count() shouldBe coll.count()
    val queries = spark.createDataFrame(
      Seq(Row("q1", Seq(1f, 3f))).asJava,
      StructType(
        Seq(
          StructField("qid", StringType),
          StructField("qv", ArrayType(FloatType))
        )
      )
    )
    val hits = queries.nearestByJoin(
      delivered,
      call_function("vector_inner_product", queries("qv"), delivered("vector")),
      3,
      "approx",
      "similarity"
    )
    val node = collectFirst(hits.queryExecution.executedPlan) {
      case exec: MilvusNearestByJoinExec => exec
    }.getOrElse(fail("the join over the delivered snapshot is not taken over"))
    node.simpleString(100) should include("2 of 2 segments by index")
    // The original snapshot's indexes are for L2.
    hits.select("id").collect().map(_.getLong(0)).toSet shouldBe
      Set(3107L, 3106L, 3105L)
  }

  test("quotes and backslashes in an argument reach the procedure as written") {
    needsNative()
    val coll = docs()
    val job = jobOf(coll.buildIndex("vector", "built/quoted", metric = "IP"))
    val name = """it's a \ 'name' \\ with "quotes""""
    coll
      .writeSnapshot(
        job,
        "built/quoted",
        snapshotName = name,
        restorable = false
      )
      .head()
      .getAs[String]("snapshot_name") shouldBe name
  }

  test("Java passes the other arguments in a map under their CALL names") {
    needsNative()
    val coll = new MilvusDataFrame(docs())
    val arguments = new java.util.HashMap[String, Object]()
    arguments.put("index_type", "HNSW")
    arguments.put("metric", "IP")
    arguments.put("params", java.util.Map.of("M", "8", "efConstruction", "64"))
    arguments.put("build_id", Integer.valueOf(7003))
    val rows = coll.buildIndex("vector", "built/java", arguments)
    built(rows).map(_._5).distinct shouldBe Seq(7003L)

    val written = new java.util.HashMap[String, Object]()
    written.put("output", "built/java-snapshot")
    written.put("restorable", java.lang.Boolean.FALSE)
    coll
      .writeSnapshot(jobOf(rows), "built/java", written)
      .head()
      .getAs[Int]("indexes") shouldBe 2

    failure(
      coll.buildIndex("vector", "built/java", java.util.Map.of("field", "id"))
    ) should include("'field' is not an argument of the map")
    failure(
      coll.buildIndex(
        "vector",
        "built/java",
        java.util.Map.of("build_id", java.lang.Double.valueOf(1.5))
      )
    ) should include("A CALL argument is a string, an integer or a boolean")
    failure(
      coll.buildIndex(
        "vector",
        "built/java",
        java.util.Map.of("build_id", "seven")
      )
    ) should include("argument 'build_id' must be BIGINT, got a string")
    temporaryViews shouldBe empty
  }

  test(
    "a DataFrame that is not the whole table is refused, and its view dropped"
  ) {
    needsNative()
    val coll = docs()
    failure(
      coll.where(col("id") > 3002L).buildIndex("vector", "built/x")
    ) should
      include("its plan holds Filter")
    failure(coll.limit(3).writeSnapshot("index-1", "built/x")) should include(
      "its plan holds"
    )
    failure(
      coll.join(docs().select(col("id")), "id").buildIndex("vector", "built/x")
    ) should include("its plan holds")
    failure(
      spark.range(3).toDF().buildIndex("vector", "built/x")
    ) should include(
      "does not read a Milvus table"
    )
    // A filter the read takes is no less a filter.
    failure(
      docs(Map(MilvusOption.MilvusFilter -> "id > 3002"))
        .buildIndex("vector", "built/x")
    ) should include("cannot be read with 'milvus.filter'")
    failure(
      spark.sql(
        "CALL milvus.system.build_index(collection => 'local_collection', field => 'vector', " +
          s"output => 'built/x', $optionArguments, `milvus.filter` => 'id > 3002')"
      )
    ) should include("cannot be read with 'milvus.filter'")
    temporaryViews shouldBe empty
  }

  test(
    "a CALL names the table or the collection, and no options with a table"
  ) {
    needsNative()
    docs().createOrReplaceTempView("docs_for_arguments")
    failure(
      spark.sql(
        "CALL milvus.system.build_index(collection => 'c', table => 'docs_for_arguments', " +
          "field => 'vector', output => 'built/x')"
      )
    ) should include("give 'collection' or 'table', not both")
    failure(
      spark.sql(
        "CALL milvus.system.write_snapshot(job => 'index-1', input => 'built/x')"
      )
    ) should include("give 'collection' or 'table'")
    failure(
      spark.sql(
        "CALL milvus.system.build_index(table => 'docs_for_arguments', field => 'vector', " +
          "output => 'built/x', `fs.storage_type` => 'local')"
      )
    ) should include("a call with 'table' takes none; got fs.storage_type")
  }
}
