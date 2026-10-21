package com.zilliz.spark.connector.sources

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import com.zilliz.spark.connector.options.MilvusOption

/** Reads the committed integration/v2 fixture uploaded to object storage.
  * MILVUS_READ_IT_OPTIONS names a JSON file containing its connector storage
  * options and milvus.snapshot.path. This suite never creates a collection or
  * prepares a snapshot through Milvus; see docs/contributing.md.
  */
class MilvusDataSourceTest extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var options: Map[String, String] = Map.empty

  override def beforeAll(): Unit = {
    super.beforeAll()
    val path = sys.env.getOrElse(
      "MILVUS_READ_IT_OPTIONS",
      throw new IllegalArgumentException(
        "Set MILVUS_READ_IT_OPTIONS to the JSON options for the uploaded integration/v2 read fixture"
      )
    )
    val json = new ObjectMapper().readTree(Files.readAllBytes(Paths.get(path)))
    require(json.isObject, "Read integration options must be a JSON object")
    options = json
      .fields()
      .asScala
      .map { entry =>
        require(
          entry.getValue.isTextual,
          "Read integration option values must be strings"
        )
        entry.getKey -> entry.getValue.asText()
      }
      .toMap
    require(
      options.contains(MilvusOption.SnapshotPath),
      "The read fixture requires milvus.snapshot.path"
    )
    spark = SparkSession
      .builder()
      .appName("MilvusDataSourceTest")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) spark.stop()
    } finally super.afterAll()

  private def read(extra: (String, String)*): DataFrame =
    spark.read.format("milvus").options(options ++ extra).load()

  test("Read data using Storage V2 DataSource") {
    val rows = read().orderBy("id").collect()
    assert(rows.length == 30)
    rows.zipWithIndex.foreach { case (row, id) =>
      assert(row.getAs[Long]("id") == id.toLong)
      assert(row.getAs[Long]("int64") == id % 10)
      assert(row.getAs[Float]("float") == id.toFloat / 32)
      assert(row.getAs[String]("varchar") == s"test_string_$id")
      assert(
        row.getSeq[Float](row.fieldIndex("vector")) == Seq(
          id.toFloat,
          id.toFloat + 1
        )
      )
    }
  }

  test("Read specific fields using ReaderFieldIDs") {
    val df = read(MilvusOption.ReaderFieldIDs -> "100,102")
    assert(df.columns.toSet == Set("id", "float"))
    assert(df.count() == 30)
  }

  test("Query data with Spark SQL") {
    read().createOrReplaceTempView("milvus_data")
    assert(
      spark
        .sql("SELECT id, int64, float FROM milvus_data WHERE int64 < 5")
        .count() == 15
    )
    val aggregate = spark
      .sql("SELECT COUNT(*) AS total, AVG(float) AS avg_float FROM milvus_data")
      .head()
    assert(aggregate.getLong(0) == 30)
    assert(aggregate.getDouble(1) == 14.5 / 32)
  }

  test("Column pruning via select") {
    val df = read().select("id", "int64", "float")
    assert(df.columns.toSet == Set("id", "int64", "float"))
    assert(df.count() == 30)
  }

  test("Column pruning via explicit ReaderFieldIDs") {
    val df = read(MilvusOption.ReaderFieldIDs -> "100,101,102")
    assert(df.columns.toSet == Set("id", "int64", "float"))
    assert(df.count() == 30)
  }

  test("Filter pushdown - equality filter on int64") {
    assert(
      read()
        .filter("int64 = 3")
        .select("id")
        .collect()
        .map(_.getLong(0))
        .toSet == Set(3L, 13L, 23L)
    )
  }

  test("Filter pushdown - range filter on int64") {
    assert(read().filter("int64 >= 5 AND int64 < 8").count() == 9)
  }

  test("Filter pushdown - IN filter on int64") {
    assert(read().filter("int64 IN (1, 3, 5)").count() == 9)
  }

  test("Filter pushdown - combined with column pruning") {
    val df = read().select("id", "int64").filter("int64 < 5")
    assert(df.columns.toSet == Set("id", "int64"))
    assert(df.count() == 15)
  }

  test("Filter pushdown - IS NULL and IS NOT NULL") {
    val df = read()
    assert(df.filter("int64 IS NOT NULL").count() == 30)
    assert(df.filter("int64 IS NULL").count() == 0)
  }
}
