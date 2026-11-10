package com.zilliz.spark.connector.read

import java.nio.file.{Files, Path}
import java.util.Comparator

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import com.zilliz.milvus.storage.io.ReadFixtures
import com.zilliz.spark.connector.options.MilvusOption

/** Consumes frozen public files through a published connector and real native
  * libraries. Preparation only verifies and copies resources: no collection,
  * index or snapshot is produced by the connector under test.
  */
object ReadOnlyConsumerSmoke {
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

  private def snapshotReads(spark: SparkSession, directory: Path): Unit = {
    ReadFixtures.copyTo("collections/basic", directory)
    val options = storage(directory) ++ Map(
      MilvusOption.SnapshotPath -> "snapshot.json"
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

  }

  def main(args: Array[String]): Unit = {
    val root = Files.createTempDirectory("read-only-consumer-")
    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .master(sys.props.getOrElse("milvus.smoke.master", "local[2]"))
        .appName("read-only-consumer-smoke")
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
      snapshotReads(spark, root.resolve("basic"))
      pathEntries(spark, root.resolve("basic"))
      println("ALL PASS")
    } finally {
      if (spark != null) spark.stop()
      val paths = Files.walk(root)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }
}
