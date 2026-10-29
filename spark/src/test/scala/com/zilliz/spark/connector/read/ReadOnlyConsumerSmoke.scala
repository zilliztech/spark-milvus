package com.zilliz.spark.connector.read

import java.nio.file.{Files, Path}
import java.util.Comparator

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{call_function, col}

import com.zilliz.milvus.storage.io.ReadFixtures
import com.zilliz.spark.connector.extensions.{
  MilvusNearestByJoinExec,
  MilvusReadSparkSessionExtensions,
  MilvusSparkPlugin
}
import com.zilliz.spark.connector.implicits._
import com.zilliz.spark.connector.metrics.{NearestByMetrics, SearchMetrics}
import com.zilliz.spark.connector.options.MilvusOption

/** Consumes frozen public files through a published connector and real native
  * libraries. Preparation only verifies and copies resources: no collection,
  * index or snapshot is produced by the connector under test.
  */
object ReadOnlyConsumerSmoke extends AdaptiveSparkPlanHelper {
  private final case class Entity(
      id: Long,
      vector: Seq[Float],
      category: Option[Long]
  )

  private val entities = (0 until 8).map { row =>
    Entity(
      3000L + row,
      Seq(row.toFloat, 0f),
      if (row == 1) None else Some(if (row == 0) 0L else 1L)
    )
  } ++ (0 until 8).map { row =>
    Entity(3100L + row, Seq(row + 0.25f, 1f), Some((row % 2).toLong))
  }
  private val live = entities.filterNot(row => Set(3002L, 3105L)(row.id))
  private val queries = Seq(
    "q1" -> Seq(0.1f, 0f),
    "q2" -> Seq(4.1f, 0.6f),
    "q3" -> Seq(7.5f, 0.9f)
  )
  private val K = 3
  private val mapper = new ObjectMapper()

  private def passed(name: String): Unit =
    println(s"MILVUS_CHECK\t$name\tpassed")

  private def storage(directory: Path): Map[String, String] =
    Map("fs.storage_type" -> "local", "fs.root_path" -> directory.toString)

  private def read(
      spark: SparkSession,
      options: Map[String, String]
  ): DataFrame =
    spark.read.format("milvus").options(options).load()

  private def l2(left: Seq[Float], right: Seq[Float]): Double =
    math.sqrt(
      left
        .zip(right)
        .map { case (a, b) =>
          val distance = a.toDouble - b.toDouble
          distance * distance
        }
        .sum
    )

  private def pathEntries(spark: SparkSession, directory: Path): Unit = {
    val json =
      mapper.readTree(Files.readAllBytes(directory.resolve("snapshot.json")))
    val options = storage(directory) +
      (MilvusOption.SnapshotPath -> "snapshot.json")
    val frame = read(spark, options).select("id", "category").orderBy("id")
    val wanted = live.sortBy(_.id).map(row => row.id -> row.category)
    def rows = frame
      .collect()
      .toSeq
      .map(row =>
        row.getLong(0) -> Option(row.get(1)).map(_.asInstanceOf[Long])
      )
    assert(rows == wanted, "advanced V3 fields or deletes differ")
    assert(
      read(spark, options + (MilvusOption.ReadApplyDeletes -> "false"))
        .count() == 16L
    )
    passed("v3-advanced")
    assert(
      frame.count() == 14L && rows == wanted,
      "repeated actions changed the fixed relation"
    )
    passed("repeated-actions")
    val empty = read(
      spark,
      storage(directory) ++ Map(
        MilvusOption.SnapshotSchemaJson -> json.toString,
        MilvusOption.SnapshotManifests -> "[]"
      )
    )
    assert(
      empty.schema == read(spark, options).schema,
      "empty V3 input lost its schema"
    )
    assert(empty.count() == 0L && empty.collect().isEmpty)
    passed("v3-empty")
  }

  private def indexedReads(spark: SparkSession, directory: Path): Unit = {
    ReadFixtures.copyTo("collections/nearest", directory)
    val options = storage(directory) ++ Map(
      MilvusOption.SnapshotPath -> "snapshot.json",
      MilvusOption.SearchParams -> "ef=32"
    )
    val rows = read(spark, options).select("id", "vector", "category").collect()
    assert(rows.length == 14, s"V3 snapshot returned ${rows.length} rows")
    val byId = rows.map(row => row.getLong(0) -> row).toMap
    assert(
      byId.keySet == live.map(_.id).toSet,
      "V3 deletes or row identities differ"
    )
    live.foreach { expected =>
      val row = byId(expected.id)
      assert(
        row.getSeq[Float](1).toSeq == expected.vector,
        s"vector ${expected.id}"
      )
      assert(
        Option(row.get(2)).map(_.asInstanceOf[Long]) == expected.category,
        s"nullable category ${expected.id}"
      )
    }
    val nulls = read(spark, options)
      .filter(col("category").isNull)
      .select("id")
      .collect()
      .map(_.getLong(0))
      .toSeq
    assert(nulls == Seq(3001L), s"nullable filter: $nulls")
    passed("v3-snapshot")

    import spark.implicits._
    val query = queries.toDF("qid", "query_vector")
    for (mode <- Seq("exact", "approx"); filtered <- Seq(false, true)) {
      val input = read(spark, options)
      val base = if (filtered) input.filter(col("category") === 1L) else input
      val distance = call_function(
        "vector_l2_distance",
        query("query_vector"),
        base("vector")
      )
      val joined = query.nearestByJoin(base, distance, K, mode, "distance")
      // The join returns both inputs' columns, without an internal score. The
      // oracle checks returned IDs; this projection checks their output vectors.
      val result = joined.select(
        col("qid"),
        col("id"),
        distance.cast("double").as("distance")
      )
      val hits = result.collect().toSeq.groupBy(_.getString(0))
      assert(
        hits.keySet == queries.map(_._1).toSet,
        s"$mode missing query results"
      )
      queries.foreach { case (qid, vector) =>
        val eligible =
          live.filter(row => !filtered || row.category.contains(1L))
        val expected = eligible
          .map(row => row.id -> l2(vector, row.vector))
          .sortBy { case (id, score) => (score, id) }
          .take(K)
        val got = hits(qid)
          .map(row => row.getLong(1) -> row.getDouble(2))
          .sortBy { case (id, score) => (score, id) }
        assert(
          got.map(_._1) == expected.map(_._1),
          s"$mode filtered=$filtered $qid: got $got, expected $expected"
        )
        got.zip(expected).foreach { case ((_, score), (_, wanted)) =>
          assert(
            math.abs(score - wanted) <= 1e-5 * math.max(1d, wanted),
            s"$mode $qid distance $score != $wanted"
          )
        }
      }
      val execution = collectFirst(result.queryExecution.executedPlan) {
        case node: MilvusNearestByJoinExec => node
      }.getOrElse(throw new AssertionError("Milvus search was not taken over"))
      val metrics = execution.metrics
      if (mode == "approx") {
        assert(metrics(NearestByMetrics.IndexSegments).value == 2L)
        assert(metrics(NearestByMetrics.ExactSegments).value == 0L)
        assert(
          metrics(SearchMetrics.IndexBytes).value > 0L,
          "HNSW files were not loaded"
        )
      } else {
        assert(metrics(NearestByMetrics.ExactSegments).value == 2L)
      }
      assert(metrics(SearchMetrics.KnowhereCalls).value > 0L)
      println(
        s"PASS $mode NEAREST BY filtered=$filtered: fixed HNSW input and independent L2 oracle"
      )
      passed(s"$mode-${if (filtered) "filtered" else "unfiltered"}")
    }
    query.createOrReplaceTempView("consumer_queries")
    read(spark, options).createOrReplaceTempView("consumer_docs")
    val sql =
      if (org.apache.spark.SPARK_VERSION.startsWith("4.2."))
        "SELECT q.qid, d.id FROM consumer_queries q JOIN consumer_docs d " +
          "APPROX NEAREST 3 BY DISTANCE vector_l2_distance(q.query_vector, d.vector)"
      else
        "SELECT query.qid, base.id FROM nearest_by_join(TABLE(consumer_queries), " +
          "TABLE(consumer_docs), 'vector_l2_distance(query.query_vector, base.vector)', 3, 'approx', 'distance')"
    val result = spark.sql(sql)
    val actual = result
      .collect()
      .map(row => row.getString(0) -> row.getLong(1))
      .toSeq
      .sorted
    val expected = queries.flatMap { case (qid, vector) =>
      live
        .sortBy(row => (l2(vector, row.vector), row.id))
        .take(K)
        .map(row => qid -> row.id)
    }.sorted
    assert(
      actual == expected,
      s"SQL search returned $actual instead of $expected"
    )
    val execution = collectFirst(result.queryExecution.executedPlan) {
      case node: MilvusNearestByJoinExec => node
    }.getOrElse(throw new AssertionError("SQL search was not taken over"))
    assert(execution.metrics(NearestByMetrics.IndexSegments).value == 2L)
    assert(execution.metrics(NearestByMetrics.ExactSegments).value == 0L)
    assert(execution.metrics(SearchMetrics.IndexBytes).value > 0L)
    assert(execution.metrics(SearchMetrics.KnowhereCalls).value > 0L)
    passed("sql-search")
  }

  def main(args: Array[String]): Unit = {
    val root = Files.createTempDirectory("read-only-consumer-")
    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .master(sys.props.getOrElse("milvus.smoke.master", "local[2]"))
        .appName("read-only-consumer-smoke")
        .config(
          "spark.sql.extensions",
          classOf[MilvusReadSparkSessionExtensions].getName
        )
        .config("spark.plugins", classOf[MilvusSparkPlugin].getName)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      if (spark.sparkContext.master.startsWith("local-cluster[")) {
        val driver = ProcessHandle.current().pid()
        val workers = spark.sparkContext
          .parallelize(Seq(1, 2), 2)
          .mapPartitions(_ => Iterator(ProcessHandle.current().pid()))
          .collect()
        assert(workers.nonEmpty && workers.forall(_ != driver))
        passed("independent-executor")
      }
      indexedReads(spark, root.resolve("nearest"))
      pathEntries(spark, root.resolve("nearest"))
      println("ALL PASS")
    } finally {
      if (spark != null) spark.stop()
      val paths = Files.walk(root)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }
}
