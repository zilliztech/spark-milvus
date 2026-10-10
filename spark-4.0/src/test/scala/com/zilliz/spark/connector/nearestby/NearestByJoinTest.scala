package com.zilliz.spark.connector.nearestby

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{call_function, col, row_number, udf}
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  LongType,
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
  MilvusNearestByJoinFrameExec,
  MilvusSparkPlugin,
  MilvusSparkSessionExtensions
}
import com.zilliz.spark.connector.implicits._
import com.zilliz.spark.connector.metrics.{NearestByMetrics, SearchMetrics}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.testkit.LocalMilvusCollection
import com.zilliz.spark.connector.testkit.LocalMilvusCollection.{
  Entity,
  Segment
}

/** A nearest-by join over a Milvus table, taken over by the connector, on every
  * Spark line: Spark 4.2's own NEAREST BY, and the connector's `nearestByJoin`
  * and `nearest_by_join` before it (docs/design/architecture/dataframe-api.html
  * sections 2 to 6).
  *
  * Every result is checked against the join over the same rows as a table that
  * is not a Milvus table, which the connector does not take over: Spark's own
  * execution on 4.2, the general plan before it; a brute-force search worked
  * out here checks that baseline. Two segments of eight rows: segment 30 has a
  * deleted id and a NULL category, segment 31 a deleted id; both carry an HNSW
  * index. A second collection, one unindexed segment, holds the vectors
  * Knowhere is not given: NaN, infinity, a zero vector, values too large and
  * too small.
  */
class NearestByJoinTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with AdaptiveSparkPlanHelper {

  private val line =
    org.apache.spark.SPARK_VERSION.split('.').take(2).mkString(".")
  private val sparkHasNearestBy = line == "4.2"

  /** Arrow 12, which Spark 3.5 ships, cannot allocate on JDK 21; the 3.5 line's
    * tests fork a JDK 17 when `SPARK35_TEST_JAVA_HOME` names one.
    */
  private val arrowAllocates =
    line != "3.5" || Runtime.version().feature() < 21

  private var available = true
  private var directory: Path = _
  private var options: Map[String, String] = Map.empty
  private var specialDirectory: Path = _
  private var specialOptions: Map[String, String] = Map.empty
  private var spark: SparkSession = _

  private val segments = Seq(
    Segment(
      30L,
      (0 until 8).map(row =>
        Entity(
          3000L + row,
          Array(row.toFloat, 0f),
          if (row == 1) None else Some(if (row == 0) 0L else 1L)
        )
      ),
      deletedIds = Seq(3002L)
    ),
    Segment(
      31L,
      (0 until 8).map(row =>
        Entity(3100L + row, Array(row + 0.25f, 1f), Some((row % 2).toLong))
      ),
      deletedIds = Seq(3105L)
    )
  )

  /** Rows outside the engine range for every metric, and rows in it, chosen so
    * that no two rows tie at the third place for the queries below.
    */
  private val special = Seq(
    Segment(
      40L,
      Seq(
        Array(1f, 0f),
        Array(Float.NaN, 0f),
        Array(0f, 0f),
        Array(Float.PositiveInfinity, 1f),
        Array(math.pow(2, 40).toFloat, math.pow(2, 39).toFloat),
        Array(math.pow(2, -30).toFloat, math.pow(2, -28).toFloat),
        Array(3f, 5f),
        Array(-2f, 1f)
      ).zipWithIndex.map { case (vector, row) =>
        Entity(4000L + row, vector, Some(1L))
      },
      indexMetric = None
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
      directory = Files.createTempDirectory("nearest-by-takeover-")
      options = LocalMilvusCollection.write(directory, 2, segments)
      specialDirectory = Files.createTempDirectory("nearest-by-special-")
      specialOptions = LocalMilvusCollection.write(specialDirectory, 2, special)
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("nearest-by-takeover")
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
    Seq(directory, specialDirectory).filter(_ != null).foreach { written =>
      val paths = Files.walk(written)
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

  private def specials(read: Map[String, String] = Map.empty): DataFrame =
    spark.read.format("milvus").options(specialOptions ++ read).load()

  /** The same rows, read once and held by Spark: not a Milvus table. */
  private def copyOf(frame: DataFrame): DataFrame =
    spark.createDataFrame(frame.collect().toSeq.asJava, frame.schema)

  private def queries(rows: (String, Array[Float])*): DataFrame =
    spark.createDataFrame(
      rows.map { case (qid, vector) =>
        Row(qid, Option(vector).map(_.toSeq).orNull)
      }.asJava,
      StructType(
        Seq(
          StructField("qid", StringType, nullable = false),
          StructField("qv", ArrayType(FloatType, containsNull = true))
        )
      )
    )

  /** Query vectors of any length, with NULL elements; a null is a NULL vector.
    */
  private def rawQueries(rows: (String, Seq[java.lang.Float])*): DataFrame =
    spark.createDataFrame(
      rows.map { case (qid, vector) => Row(qid, vector) }.asJava,
      StructType(
        Seq(
          StructField("qid", StringType, nullable = false),
          StructField("qv", ArrayType(FloatType, containsNull = true))
        )
      )
    )

  private def boxed(values: Float*): Seq[java.lang.Float] =
    values.map(java.lang.Float.valueOf)

  private def ranking(
      function: String,
      query: DataFrame,
      base: DataFrame
  ): Column = call_function(function, query("qv"), base("vector"))

  private def nearest(
      query: DataFrame,
      base: DataFrame,
      function: String = "vector_l2_distance",
      k: Int = 3,
      mode: String = "exact",
      direction: String = "distance",
      joinType: String = "inner"
  ): DataFrame = query.nearestByJoin(
    base,
    ranking(function, query, base),
    k,
    mode,
    direction,
    joinType
  )

  /** The same join as Spark executes it: the ranking plus zero has the same
    * values and the same failures, and is not a function the connector
    * computes, so it is left to Spark's own execution on 4.2 and to the general
    * plan before it.
    */
  private def bySpark(
      query: DataFrame,
      base: DataFrame,
      function: String = "vector_l2_distance",
      k: Int = 3,
      mode: String = "exact",
      direction: String = "distance",
      joinType: String = "inner"
  ): DataFrame = {
    val joined = query.nearestByJoin(
      base,
      ranking(function, query, base) + 0,
      k,
      mode,
      direction,
      joinType
    )
    assert(!takenOver(joined), "the baseline is taken over")
    joined
  }

  /** The node, also inside an adaptive plan, which wraps a plan that has an
    * exchange: over a Milvus table input or over a DataFrame input.
    */
  private def node(frame: DataFrame): Option[SparkPlan] =
    collectFirst(frame.queryExecution.executedPlan) {
      case exec: MilvusNearestByJoinExec      => exec
      case exec: MilvusNearestByJoinFrameExec => exec
    }

  private def takenOver(frame: DataFrame): Boolean = node(frame).nonEmpty

  /** Which input the connector reads the base as. */
  private def input(frame: DataFrame): String = node(frame) match {
    case Some(_: MilvusNearestByJoinExec) => "Milvus table"
    case Some(_)                          => "DataFrame"
    case None                             => fail("not taken over")
  }

  /** (qid, id) pairs, NULL ids as -1, in a stable order. */
  private def pairs(frame: DataFrame): Seq[(String, Long)] =
    frame
      .select(col("qid"), col("id"))
      .collect()
      .map(row =>
        (row.getString(0), if (row.isNullAt(1)) -1L else row.getLong(1))
      )
      .toSeq
      .sorted

  /** No two base rows tie at the third place for any of these queries, with or
    * without the category filter, so Spark's choice among tied rows never
    * decides a comparison.
    */
  private val standardQueries = Seq(
    "q1" -> Array(0.1f, 0f),
    "q2" -> Array(4.1f, 0.6f),
    "q3" -> Array(7.5f, 0.9f)
  )

  /** Every message in a failure's cause chain. */
  private def messages(failure: Throwable): String =
    Iterator
      .iterate[Throwable](failure)(_.getCause)
      .takeWhile(_ != null)
      .map(e => String.valueOf(e.getMessage))
      .mkString(" ")

  test("EXACT over a Milvus table is taken over and answers as Spark does") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = docs()

    val taken = nearest(query, base)

    takenOver(taken) shouldBe true
    pairs(taken) shouldBe pairs(bySpark(query, base))
    pairs(taken).map(_._2) should contain noneOf (3002L, 3105L)
  }

  test("the output is Spark's: query columns then base columns, all nullable") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = docs()

    val taken = nearest(query, base)
    val spark42 = bySpark(query, base)

    taken.schema.fieldNames.toSeq shouldBe spark42.schema.fieldNames.toSeq
    taken.schema.fields.map(_.nullable).toSeq shouldBe
      spark42.schema.fields.map(_.nullable).toSeq
  }

  test("APPROX searches the indexes and, on this data, finds the exact rows") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = docs(Map(MilvusOption.SearchParams -> "ef=64"))

    val taken = nearest(query, base, mode = "approx")

    takenOver(taken) shouldBe true
    pairs(taken) shouldBe pairs(bySpark(query, base))
  }

  test("explain says which segments an APPROX search takes through an index") {
    needsNative()
    val query = queries(standardQueries: _*)

    def explained(frame: DataFrame): String =
      node(frame).map(_.simpleString(100)).getOrElse(fail("not taken over"))
    explained(nearest(query, docs(), mode = "approx")) should include(
      "2 of 2 segments by index, 0 scanned exactly"
    )
    explained(nearest(query, docs())) should include(
      "2 segments scanned exactly"
    )
    // The two indexes are built for L2.
    explained(
      nearest(
        query,
        docs(),
        function = "vector_inner_product",
        mode = "approx",
        direction = "similarity"
      )
    ) should include(
      "0 of 2 segments by index, 2 scanned exactly (an index of another metric: 2)"
    )
    explained(
      nearest(
        rawQueries(("q1", boxed(1f, 0.5f))),
        specials(),
        mode = "approx"
      )
    ) should include("(no index on the field: 1)")
  }

  test("APPROX scans exactly a segment whose index is for another metric") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = docs()

    val taken = nearest(
      query,
      base,
      function = "vector_inner_product",
      mode = "approx",
      direction = "similarity"
    )
    takenOver(taken) shouldBe true
    pairs(taken) shouldBe pairs(
      bySpark(
        query,
        base,
        function = "vector_inner_product",
        direction = "similarity"
      )
    )
  }

  test("the node's SQL metrics count the query rows, segments and output") {
    needsNative()
    val query = rawQueries(
      ("q1", boxed(0.1f, 0f)),
      ("q2", boxed(4.1f, 0.6f)),
      ("nan", boxed(Float.NaN, 0f)),
      ("none", null)
    )
    val taken =
      nearest(query, docs(), mode = "approx", joinType = "leftouter")
    val rows = taken.collect()

    val metrics = node(taken).getOrElse(fail("not taken over")).metrics
    metrics(NearestByMetrics.SearchedQueries).value shouldBe 2L
    metrics(NearestByMetrics.SparkQueries).value shouldBe 1L
    metrics(NearestByMetrics.UnrankedQueries).value shouldBe 1L
    metrics(NearestByMetrics.IndexSegments).value shouldBe 2L
    metrics(NearestByMetrics.ExactSegments).value shouldBe 0L
    metrics(NearestByMetrics.OutputRows).value shouldBe rows.length.toLong
    // Two searched queries, three each; three for NaN; one row for NULL.
    rows.length shouldBe 10
    metrics(SearchMetrics.SegmentSearches).value shouldBe 2L
    metrics(SearchMetrics.Candidates).value should be > 0L
  }

  test("an option the index does not take fails the search") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = docs(Map(MilvusOption.SearchParams -> "nprobe=8"))

    val failure =
      the[Exception] thrownBy nearest(query, base, mode = "approx").collect()
    messages(failure) should include(
      "HNSW search supports only the ef parameter"
    )
  }

  test("COSINE and inner product rank by similarity, as Spark does") {
    needsNative()
    val query = queries(standardQueries: _*)
    // Segment 30's vectors all point along one axis, so under COSINE they tie,
    // and its zero vector has no cosine at all: COSINE is checked on segment 31.
    Seq(
      "vector_cosine_similarity" -> docs().where(col("id") >= 3100L),
      "vector_inner_product" -> docs()
    ).foreach { case (f, base) =>
      val taken = nearest(query, base, function = f, direction = "similarity")
      takenOver(taken) shouldBe true
      pairs(taken) shouldBe pairs(
        bySpark(query, base, function = f, direction = "similarity")
      )
    }
  }

  test(
    "a NULL query vector ranks nothing: INNER drops it, LEFT OUTER keeps it"
  ) {
    needsNative()
    val query = queries(("q1", Array(0.1f, 0f)), ("none", null))
    val base = docs()

    Seq("inner", "leftouter").foreach { joinType =>
      val taken = nearest(query, base, joinType = joinType)
      takenOver(taken) shouldBe true
      pairs(taken) shouldBe pairs(
        bySpark(query, base, joinType = joinType)
      )
    }
  }

  test("under COSINE a zero query vector ranks nothing") {
    needsNative()
    val query = queries(("q1", Array(0.1f, 0f)), ("zero", Array(0f, 0f)))
    val base = docs().where(col("id") >= 3100L)

    val taken = nearest(
      query,
      base,
      function = "vector_cosine_similarity",
      direction = "similarity",
      joinType = "leftouter"
    )

    takenOver(taken) shouldBe true
    pairs(taken) shouldBe pairs(
      bySpark(
        query,
        base,
        function = "vector_cosine_similarity",
        direction = "similarity",
        joinType = "leftouter"
      )
    )
    pairs(taken).filter(_._1 == "zero") shouldBe Seq("zero" -> -1L)
  }

  private val Rankings = Seq(
    ("vector_l2_distance", "distance"),
    ("vector_inner_product", "similarity"),
    ("vector_cosine_similarity", "similarity")
  )

  test("a query of another length fails as Spark's does, the empty one too") {
    needsNative()
    Seq(
      rawQueries(("wide", boxed(0.1f, 0f, 1f))),
      rawQueries(("empty", boxed())),
      // The length is checked before the elements.
      rawQueries(("wide", Seq[java.lang.Float](1f, null, 0f)))
    ).foreach { query =>
      val ours = the[Exception] thrownBy nearest(query, docs()).collect()
      val sparks =
        the[Exception] thrownBy bySpark(query, docs()).collect()
      messages(ours) should include("VECTOR_DIMENSION_MISMATCH")
      messages(sparks) should include("VECTOR_DIMENSION_MISMATCH")
    }
  }

  test("a query of another length compared with no base row does not fail") {
    needsNative()
    val query =
      rawQueries(("wide", boxed(0.1f, 0f, 1f)), ("q1", boxed(0.1f, 0f)))
    val base = docs().where(col("id") < 0L)

    Seq("inner", "leftouter").foreach { joinType =>
      val taken = nearest(query, base, joinType = joinType)
      takenOver(taken) shouldBe true
      pairs(taken) shouldBe pairs(
        bySpark(query, base, joinType = joinType)
      )
    }
  }

  test("a query of the field's length with a NULL element ranks nothing") {
    needsNative()
    val query = rawQueries(
      ("q1", boxed(0.1f, 0f)),
      ("hole", Seq[java.lang.Float](null, 1f))
    )

    Seq("inner", "leftouter").foreach { joinType =>
      val taken = nearest(query, docs(), joinType = joinType)
      takenOver(taken) shouldBe true
      pairs(taken) shouldBe pairs(
        bySpark(query, docs(), joinType = joinType)
      )
    }
  }

  test("NaN, infinite and too large query values are joined by Spark") {
    needsNative()
    // Seven live rows and k = 10: every row with a value is in the answer,
    // so the comparison does not depend on how Spark orders equal values.
    val base = docs().where(col("id") >= 3100L)
    val query = rawQueries(
      ("nan", boxed(Float.NaN, 0f)),
      ("inf", boxed(Float.PositiveInfinity, 1f)),
      ("large", boxed(math.pow(2, 40).toFloat, 0f)),
      ("searched", boxed(4.1f, 0.6f))
    )

    for {
      (function, direction) <- Rankings
      joinType <- Seq("inner", "leftouter")
    } {
      val taken = nearest(
        query,
        base,
        function = function,
        k = 10,
        direction = direction,
        joinType = joinType
      )
      takenOver(taken) shouldBe true
      pairs(taken) shouldBe pairs(
        bySpark(
          query,
          base,
          function = function,
          k = 10,
          direction = direction,
          joinType = joinType
        )
      )
    }
  }

  test("base rows Knowhere is not given are ranked by Spark's function") {
    needsNative()
    val query =
      rawQueries(("q1", boxed(1f, 0.5f)), ("q2", boxed(-1f, 3f)))

    for {
      (function, direction) <- Rankings
      k <- Seq(3, 8)
      mode <- Seq("exact", "approx")
    } {
      val taken = nearest(
        query,
        specials(),
        function = function,
        k = k,
        mode = mode,
        direction = direction
      )
      withClue(s"$function k=$k $mode: ") {
        takenOver(taken) shouldBe true
        pairs(taken) shouldBe pairs(
          bySpark(
            query,
            specials(),
            function = function,
            k = k,
            direction = direction
          )
        )
      }
    }
  }

  test("a filter in the base applies before the top k, above the join after") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = docs().where(col("category") === 1L)

    val before = nearest(query, base)
    takenOver(before) shouldBe true
    pairs(before) shouldBe pairs(bySpark(query, base))

    val after = nearest(query, docs()).where(col("category") === 1L)
    takenOver(after) shouldBe true
    pairs(after) shouldBe pairs(
      bySpark(query, docs()).where(col("category") === 1L)
    )
  }

  /** One query side three ways: rows of a local relation, which the driver
    * holds; rows Spark computes that fit the broadcast limit, which the driver
    * fetches; and the same with a limit of one byte, which stay on the
    * executors and meet their hits in a join. Each pairs with the base read
    * options it needs.
    */
  private def whereQueriesLive(
      query: DataFrame
  ): Seq[(String, DataFrame, Map[String, String])] = Seq(
    ("local", query, Map.empty[String, String]),
    ("fetched", query.repartition(2), Map.empty[String, String]),
    (
      "distributed",
      query.repartition(2),
      Map(MilvusOption.SearchQueriesMaxBytes -> "1")
    )
  )

  test("the query rows meet their hits the same wherever they live") {
    needsNative()
    val query = rawQueries(
      ("q1", boxed(0.1f, 0f)),
      ("q2", boxed(4.1f, 0.6f)),
      ("q3", boxed(7.5f, 0.9f)),
      ("none", null),
      ("hole", Seq[java.lang.Float](null, 1f)),
      ("nan", boxed(Float.NaN, 0f))
    )
    for {
      (where, side, read) <- whereQueriesLive(query)
      joinType <- Seq("inner", "leftouter")
      mode <- Seq("exact", "approx")
    } withClue(s"$where $joinType $mode: ") {
      val base = docs(read)
      val taken = nearest(side, base, mode = mode, joinType = joinType)
      takenOver(taken) shouldBe true
      pairs(taken) shouldBe pairs(
        bySpark(query, docs(), joinType = joinType)
      )
    }
  }

  test(
    "a window over the join runs, the node's driver side kept out of tasks"
  ) {
    needsNative()
    val query = queries(standardQueries: _*)
    // The window's exchange is read by a whole-stage stage, whose tasks carry
    // the adaptive plan's canonical form of what lies under the exchange: the
    // node with its children.
    def placed(frame: DataFrame): Seq[(String, Long, Int)] =
      frame
        .withColumn(
          "place",
          row_number().over(Window.partitionBy("qid").orderBy(col("id")))
        )
        .select("qid", "id", "place")
        .collect()
        .map(row => (row.getString(0), row.getLong(1), row.getInt(2)))
        .toSeq
        .sorted
    Seq("Milvus table" -> docs(), "DataFrame" -> copyOf(docs())).foreach {
      case (kind, base) =>
        withClue(s"$kind input: ") {
          val taken = nearest(query, base)
          input(taken) shouldBe kind
          placed(taken) shouldBe placed(bySpark(query, base))
        }
    }
  }

  test("a LEFT OUTER join of query rows that all rank nothing keeps them all") {
    needsNative()
    val query = rawQueries(
      ("none", null),
      ("hole", Seq[java.lang.Float](null, 1f))
    )
    for ((where, side, read) <- whereQueriesLive(query))
      withClue(s"$where: ") {
        val taken = nearest(side, docs(read), joinType = "leftouter")
        takenOver(taken) shouldBe true
        pairs(taken) shouldBe Seq("hole" -> -1L, "none" -> -1L)
        nearest(side, docs(read)).count() shouldBe 0L
      }
  }

  /** Each standard query's k nearest live rows of the base, as (qid, id),
    * scored in double as the functions define them; COSINE leaves out a zero
    * vector, whose cosine is NULL. `from` keeps the rows of the base the join
    * reads.
    */
  private def bruteForce(
      metric: String,
      k: Int,
      from: Long => Boolean = _ => true
  ): Seq[(String, Long)] = {
    val live = segments.flatMap(segment =>
      segment.entities.filter(e =>
        !segment.deletedIds.contains(e.id) && from(e.id)
      )
    )
    standardQueries.flatMap { case (qid, query) =>
      val scored = live.flatMap { entity =>
        val pairs = query.zip(entity.vector).map { case (a, b) =>
          (a.toDouble, b.toDouble)
        }
        val dot = pairs.map { case (a, b) => a * b }.sum
        metric match {
          case "L2" =>
            Some(entity.id -> math.sqrt(pairs.map { case (a, b) =>
              (a - b) * (a - b)
            }.sum))
          case "IP" => Some(entity.id -> -dot)
          case _ =>
            val norms = math.sqrt(
              pairs.map(p => p._1 * p._1).sum * pairs.map(p => p._2 * p._2).sum
            )
            if (norms == 0.0) None else Some(entity.id -> -(dot / norms))
        }
      }
      scored.sortBy(_._2).take(k).map(scored => qid -> scored._1)
    }.sorted
  }

  test(
    "the baseline the suite compares with is a brute-force search's answer"
  ) {
    needsNative()
    val query = queries(standardQueries: _*)
    Seq(
      ("vector_l2_distance", "distance", "L2", (_: Long) => true),
      ("vector_inner_product", "similarity", "IP", (_: Long) => true),
      // Segment 30's vectors all point one way, so their cosines tie.
      (
        "vector_cosine_similarity",
        "similarity",
        "COSINE",
        (id: Long) => id >= 3100L
      )
    ).foreach { case (function, direction, metric, from) =>
      withClue(s"$function: ") {
        val base =
          docs().where(col("id") >= (if (metric == "COSINE") 3100L else 0L))
        val expected = bruteForce(metric, 3, from)
        pairs(
          bySpark(query, base, function = function, direction = direction)
        ) shouldBe expected
        pairs(
          nearest(query, base, function = function, direction = direction)
        ) shouldBe expected
      }
    }
  }

  test(
    "SQL writes the same join: NEAREST BY on 4.2, nearest_by_join before it"
  ) {
    needsNative()
    val query = queries(standardQueries: _*)
    query.createOrReplaceTempView("nearest_queries")
    docs().createOrReplaceTempView("nearest_docs")
    val (inner, outer) =
      if (sparkHasNearestBy)
        (
          "SELECT * FROM nearest_queries q JOIN nearest_docs d " +
            "EXACT NEAREST 3 BY DISTANCE vector_l2_distance(q.qv, d.vector)",
          "SELECT * FROM nearest_queries q LEFT OUTER JOIN nearest_docs d " +
            "APPROX NEAREST 2 BY SIMILARITY vector_inner_product(d.vector, q.qv)"
        )
      else
        (
          "SELECT * FROM nearest_by_join(TABLE(nearest_queries), TABLE(nearest_docs), " +
            "'vector_l2_distance(query.qv, base.vector)', 3, 'exact', 'distance')",
          // The output columns carry their side's name, as the ranking does.
          "SELECT query.qid, base.id FROM nearest_by_join(query => TABLE(nearest_queries), " +
            "base => TABLE(nearest_docs), " +
            "ranking => 'vector_inner_product(base.vector, query.qv)', " +
            "num_results => 2, mode => 'approx', direction => 'similarity', " +
            "join_type => 'left_outer')"
        )
    val taken = spark.sql(inner)
    takenOver(taken) shouldBe true
    pairs(taken) shouldBe pairs(nearest(query, docs()))
    val outerTaken = spark.sql(outer)
    takenOver(outerTaken) shouldBe true
    pairs(outerTaken) shouldBe pairs(
      nearest(
        query,
        docs(),
        function = "vector_inner_product",
        k = 2,
        mode = "approx",
        direction = "similarity",
        joinType = "leftouter"
      )
    )
  }

  test("the arguments are checked as Spark 4.2 checks them") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = docs()
    def failure(run: => DataFrame): String =
      messages(the[Exception] thrownBy run)
    failure(nearest(query, base, k = 0)) should include(
      "NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE"
    )
    failure(nearest(query, base, k = 100001)) should include(
      "must be between 1 and 100000"
    )
    failure(nearest(query, base, mode = "fast")) should include(
      "NEAREST_BY_JOIN.UNSUPPORTED_MODE"
    )
    failure(nearest(query, base, direction = "near")) should include(
      "NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION"
    )
    failure(nearest(query, base, joinType = "full")) should include(
      "NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE"
    )
    // Case does not matter, and the join type ignores underscores.
    pairs(
      nearest(
        query,
        base,
        mode = "EXACT",
        direction = "Distance",
        joinType = "LEFT_OUTER"
      )
    ) shouldBe pairs(nearest(query, base, joinType = "leftouter"))
  }

  test("with cross joins disabled the join fails as Spark 4.2's does") {
    needsNative()
    spark.conf.set("spark.sql.crossJoin.enabled", "false")
    try {
      val failure = the[Exception] thrownBy
        nearest(queries(standardQueries: _*), docs()).collect()
      messages(failure) should include("NEAREST_BY_JOIN.CROSS_JOIN_NOT_ENABLED")
    } finally spark.conf.set("spark.sql.crossJoin.enabled", "true")
  }

  test("what the connector does not compute is left to Spark") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = docs()

    // A ranking the connector does not compute.
    val doubled = query.nearestByJoin(
      base,
      ranking("vector_l2_distance", query, base) * 2,
      3,
      "exact",
      "distance"
    )
    takenOver(doubled) shouldBe false
    pairs(doubled) shouldBe bruteForce("L2", 3)
    // A metric asked in the direction it does not rank by: the farthest rows.
    val farthest = nearest(query, base, k = 1, direction = "similarity")
    takenOver(farthest) shouldBe false
    pairs(farthest) shouldBe Seq("q1" -> 3107L, "q2" -> 3000L, "q3" -> 3000L)
  }

  /** A base of `(id, vector)` rows Spark holds: not a Milvus table. */
  private def frameBase(rows: (Long, Seq[java.lang.Float])*): DataFrame =
    spark.createDataFrame(
      rows.map { case (id, vector) => Row(id, vector) }.asJava,
      StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("vector", ArrayType(FloatType, containsNull = true))
        )
      )
    )

  test("EXACT over a base that is not a Milvus table reads it as a DataFrame") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = copyOf(docs())

    val taken = nearest(query, base)
    input(taken) shouldBe "DataFrame"
    node(taken).get.simpleString(100) should include("scanned exactly")
    pairs(taken) shouldBe pairs(bySpark(query, base))
    pairs(taken) shouldBe bruteForce("L2", 3)
    Seq(
      "vector_inner_product" -> base,
      "vector_cosine_similarity" -> base.where(col("id") >= 3100L)
    ).foreach { case (function, from) =>
      withClue(s"$function: ") {
        val similar =
          nearest(query, from, function = function, direction = "similarity")
        input(similar) shouldBe "DataFrame"
        pairs(similar) shouldBe pairs(
          bySpark(query, from, function = function, direction = "similarity")
        )
      }
    }
  }

  test("APPROX over a base without a Milvus table is Spark's on 4.2 only") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = copyOf(docs())

    val approximate = nearest(query, base, mode = "approx")
    if (sparkHasNearestBy) takenOver(approximate) shouldBe false
    else {
      input(approximate) shouldBe "DataFrame"
      node(approximate).get.simpleString(100) should include("computed exactly")
    }
    pairs(approximate) shouldBe bruteForce("L2", 3)
  }

  test("a Milvus base the scan cannot take whole is read as a DataFrame") {
    needsNative()
    val query = queries(standardQueries: _*)
    val small = udf((id: Long) => id < 3104L)
    Seq(
      "a filter the scan cannot take" -> docs().where(small(col("id"))),
      "a computed column" -> docs().select(
        col("id"),
        col("vector"),
        (col("category") + 1).as("category")
      ),
      "a limit" -> docs().limit(100)
    ).foreach { case (shape, base) =>
      withClue(s"$shape: ") {
        val taken = nearest(query, base, mode = "approx")
        input(taken) shouldBe "DataFrame"
        node(taken).get.simpleString(100) should include("computed exactly")
        pairs(taken) shouldBe pairs(bySpark(query, base))
      }
    }
  }

  test(
    "DataFrame base rows: a NULL vector or element ranks nothing, another length fails"
  ) {
    needsNative()
    val query = queries("q1" -> Array(0.1f, 0f))
    val base = frameBase(
      1L -> boxed(0f, 0f),
      2L -> null,
      3L -> Seq[java.lang.Float](null, 1f),
      4L -> boxed(1f, 1f),
      5L -> boxed(3f, 0f)
    )
    val taken = nearest(query, base)
    input(taken) shouldBe "DataFrame"
    pairs(taken) shouldBe Seq("q1" -> 1L, "q1" -> 4L, "q1" -> 5L)
    pairs(taken) shouldBe pairs(bySpark(query, base))

    val longer = frameBase(1L -> boxed(0f, 0f), 2L -> boxed(1f, 2f, 3f))
    val failure =
      messages(the[Exception] thrownBy nearest(query, longer).collect())
    failure should include("VECTOR_DIMENSION_MISMATCH")
    messages(
      the[Exception] thrownBy bySpark(query, longer).collect()
    ) should include("VECTOR_DIMENSION_MISMATCH")
  }

  test(
    "DataFrame query rows of another length or with a NULL element are Spark's"
  ) {
    needsNative()
    val base = frameBase(
      1L -> boxed(0f, 0f),
      2L -> boxed(1f, 0f),
      3L -> boxed(5f, 5f)
    )
    val query = rawQueries(
      ("q1", boxed(0.1f, 0f)),
      ("hole", Seq[java.lang.Float](null, 1f)),
      ("none", null)
    )
    Seq("inner", "leftouter").foreach { joinType =>
      withClue(s"$joinType: ") {
        val taken = nearest(query, base, k = 2, joinType = joinType)
        input(taken) shouldBe "DataFrame"
        pairs(taken) shouldBe pairs(
          bySpark(query, base, k = 2, joinType = joinType)
        )
      }
    }
    val mixed = rawQueries(("q1", boxed(0.1f, 0f)), ("q3", boxed(1f, 0f, 0f)))
    messages(
      the[Exception] thrownBy nearest(mixed, base).collect()
    ) should include("VECTOR_DIMENSION_MISMATCH")
  }

  test(
    "DataFrame base vectors Knowhere is not given are ranked by Spark's function"
  ) {
    needsNative()
    val query = rawQueries(("q1", boxed(1f, 0.5f)), ("q2", boxed(-1f, 3f)))
    val base = copyOf(specials())
    for {
      (function, direction) <- Rankings
      k <- Seq(3, 8)
    } {
      withClue(s"$function k=$k: ") {
        val taken =
          nearest(
            query,
            base,
            function = function,
            k = k,
            direction = direction
          )
        input(taken) shouldBe "DataFrame"
        pairs(taken) shouldBe pairs(
          bySpark(
            query,
            base,
            function = function,
            k = k,
            direction = direction
          )
        )
      }
    }
  }

  test("a base vector the ranking computes is read as a DataFrame") {
    needsNative()
    val query = queries(standardQueries: _*)
    // Short elements, as an Int8Vector field reads, cast to floats.
    val shorts = spark.createDataFrame(
      Seq(
        Row(1L, Seq[Short](0, 0)),
        Row(2L, Seq[Short](4, 1)),
        Row(3L, Seq[Short](8, 1)),
        Row(4L, Seq[Short](2, 0))
      ).asJava,
      StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("v", ArrayType(org.apache.spark.sql.types.ShortType))
        )
      )
    )
    def ranked(plus: Boolean) = {
      val ranking = call_function(
        "vector_l2_distance",
        query("qv"),
        shorts("v").cast(ArrayType(FloatType))
      )
      query.nearestByJoin(
        shorts,
        if (plus) ranking + 0 else ranking,
        2,
        "exact",
        "distance"
      )
    }
    input(ranked(plus = false)) shouldBe "DataFrame"
    pairs(ranked(plus = false)) shouldBe pairs(ranked(plus = true))
  }

  test("a DataFrame input with k above its rows gives every row") {
    needsNative()
    val query = queries(standardQueries: _*)
    val base = frameBase(1L -> boxed(0f, 0f), 2L -> boxed(1f, 0f))
    val taken = nearest(query, base, k = 10)
    input(taken) shouldBe "DataFrame"
    pairs(taken) shouldBe pairs(bySpark(query, base, k = 10))
    pairs(taken).size shouldBe 6
  }
}
