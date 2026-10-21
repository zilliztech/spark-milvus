package com.zilliz.spark.connector.read

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{call_function, col}

import com.zilliz.milvus.storage.io.ReadFixtures
import com.zilliz.spark.connector.extensions.{
  MilvusNearestByJoinExec,
  MilvusSparkPlugin,
  MilvusSparkSessionExtensions
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

  private def optionsFile(path: Path): Map[String, String] = {
    val json = mapper.readTree(Files.readAllBytes(path))
    assert(json.isObject, s"Expected an options object in $path")
    json
      .fields()
      .asScala
      .map { entry =>
        assert(
          entry.getValue.isTextual,
          s"Non-string option ${entry.getKey} in $path"
        )
        entry.getKey -> entry.getValue.asText()
      }
      .toMap
  }

  private def offlineEntries(spark: SparkSession, root: Path): Unit = {
    for (name <- Seq("long-v2", "string-v2")) {
      val directory = root.resolve(name)
      ReadFixtures.copyTo(s"consumer/$name", directory)
      val expected =
        mapper.readTree(Files.readAllBytes(directory.resolve("expected.json")))
      val columns =
        expected.get("columns").elements().asScala.map(_.asText()).toSeq
      val wanted =
        expected.get("rows").elements().asScala.map(_.toString).toSeq.sorted
      val entries = Seq(
        "snapshot" -> Map(MilvusOption.SnapshotPath -> "snapshot.json"),
        "advanced" -> optionsFile(directory.resolve("options.json")),
        "backup" -> optionsFile(directory.resolve("backup-options.json"))
      )
      for ((entry, configured) <- entries) {
        val options = configured ++ storage(directory)
        val frame = read(spark, options).select(columns.map(col): _*)
        val got = frame
          .collect()
          .toSeq
          .map { row =>
            mapper.valueToTree[JsonNode](row.toSeq.asJava).toString
          }
          .sorted
        assert(got == wanted, s"$name $entry: got $got, expected $wanted")
        val raw =
          read(spark, options + (MilvusOption.ReadApplyDeletes -> "false"))
            .count()
        assert(
          raw == expected.get("rawRows").asLong(),
          s"$name $entry raw rows: $raw"
        )
        println(
          s"PASS fixed $name $entry: nullable projection and equal-timestamp deletes"
        )
      }
    }
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
    println("PASS fixed V3 snapshot: projection, nullable fields and deletes")

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
    }
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
          classOf[MilvusSparkSessionExtensions].getName
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
        println("PASS independent executor JVM")
      }
      indexedReads(spark, root.resolve("nearest"))
      offlineEntries(spark, root.resolve("offline"))
      println("ALL PASS")
    } finally {
      if (spark != null) spark.stop()
      val paths = Files.walk(root)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }
}
