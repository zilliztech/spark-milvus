package com.zilliz.spark.connector.sources

import java.nio.file.{Files, Paths}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.TaskContext
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions
import com.zilliz.spark.connector.options.MilvusOption

/** Reads the committed collections/nearest V3 fixture from object storage. The
  * two options files bind distinct read-only identities to the same bucket.
  * Each identity can read its own snapshot and that snapshot's immutable
  * segment files. The suite never creates collections or calls a Milvus
  * service.
  */
class MilvusDataSourceTest extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var first: Map[String, String] = Map.empty
  private var second: Map[String, String] = Map.empty
  private val ids = (3000L to 3007L).toSet - 3002L
  private val otherIds = (3100L to 3107L).toSet - 3105L

  private def options(variable: String): Map[String, String] = {
    val path = sys.env.getOrElse(
      variable,
      throw new IllegalArgumentException(
        s"Set $variable to a V3 read fixture options file"
      )
    )
    val json = new ObjectMapper().readTree(Files.readAllBytes(Paths.get(path)))
    require(json.isObject, "Read integration options must be a JSON object")
    val result = json
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
      result.contains(MilvusOption.SnapshotPath),
      "The read fixture requires milvus.snapshot.path"
    )
    result
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    first = options("MILVUS_READ_IT_OPTIONS")
    second = options("MILVUS_READ_IT_OPTIONS_B")
    require(
      first("fs.address") == second("fs.address") &&
        first("fs.bucket_name") == second("fs.bucket_name"),
      "Identity test requires the same endpoint and bucket"
    )
    require(
      first("fs.access_key_id") != second("fs.access_key_id"),
      "Identity test requires different users"
    )
    spark = SparkSession
      .builder()
      .appName("MilvusDataSourceTest")
      .master("local[2,2]")
      .config(
        "spark.sql.extensions",
        classOf[MilvusReadSparkSessionExtensions].getName
      )
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit =
    try { if (spark != null) spark.stop() }
    finally super.afterAll()

  private def read(binding: Map[String, String] = first): DataFrame =
    spark.read.format("milvus").options(binding).load()

  private def actual(frame: DataFrame): Set[Long] =
    frame.select("id").collect().map(_.getLong(0)).toSet

  test("native S3 byte, size and range reads match both committed objects") {
    Seq(first, second).zipWithIndex.foreach { case (binding, index) =>
      NativeStorageProbe.verifyObject(StorageProperties.from(binding), index)
    }
  }

  test("bucket-scoped Hadoop configuration reaches the native S3 reader") {
    Seq(first, second).zipWithIndex.foreach { case (binding, index) =>
      val properties =
        NativeStorageProbe.hadoopProperties(binding, useEnvironment = false)
      NativeStorageProbe.verifyObject(properties, index)
      NativeStorageProbe.verifyEnvironment(binding, index)
    }
  }

  test("V3 projection, nullable fields and deletes match the fixed oracle") {
    val frame = read()
    assert(actual(frame) == ids)
    assert(
      frame.filter("category IS NULL").select("id").head().getLong(0) == 3001L
    )
    assert(frame.filter("category = 0").count() == 1)
    val vectors = frame.select("id", "vector")
    assert(vectors.columns.toSeq == Seq("id", "vector"))
    vectors.collect().foreach { row =>
      assert(row.getSeq[Float](1) == Seq((row.getLong(0) - 3000L).toFloat, 0f))
    }
    assert(frame.filter("id >= 3001 AND id < 3005").count() == 3)
    assert(
      actual(frame.select("id", "category").filter("category = 0")) == Set(
        3000L
      )
    )
    assert(frame.filter("category IS NOT NULL").count() == 6)
    assert(frame.filter("id IN (3000, 3002, 3105, 3107)").count() == 1)
    val selected = read(first + (MilvusOption.ReaderFieldIDs -> "100,102"))
    assert(selected.columns.toSet == Set("id", "category"))
    assert(actual(selected) == ids)
    frame.createOrReplaceTempView("v3_fixture")
    assert(spark.sql("SELECT count(*) FROM v3_fixture").head().getLong(0) == 7)
    assert(
      spark
        .sql("SELECT avg(category) FROM v3_fixture")
        .head()
        .getDouble(0) == 5.0 / 6.0
    )
  }

  test(
    "two explicit identities remain bound across actions and concurrent reads"
  ) {
    val config = spark.sparkContext.hadoopConfiguration
    val before =
      config.iterator().asScala.map(e => e.getKey -> e.getValue).toMap
    val left = read(first)
    val right = read(second)
    assert(actual(left) == ids)
    assert(actual(right) == otherIds)
    assert(actual(left) == ids)
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val results = Await.result(
      Future.sequence(
        Seq(Future(actual(read(first))), Future(actual(read(second))))
      ),
      2.minutes
    )
    assert(results == Seq(ids, otherIds))
    assert(
      config.iterator().asScala.map(e => e.getKey -> e.getValue).toMap == before
    )
  }

  test("task retries retain each input identity and fixed rows") {
    Seq(first -> ids, second -> otherIds).foreach { case (binding, expected) =>
      val rows = read(binding)
        .select("id")
        .rdd
        .mapPartitions { input =>
          val values = input.map(_.getLong(0)).toVector
          val attempt = TaskContext.get().attemptNumber()
          if (attempt == 0)
            throw new IllegalStateException("intentional V3 read retry")
          values.iterator.map(id => (id, attempt))
        }
        .collect()
      assert(rows.map(_._1).toSet == expected)
      assert(rows.forall(_._2 == 1))
    }
  }

  test(
    "one identity cannot reuse the other identity's snapshot authorization"
  ) {
    // Warm both sessions first, so a filesystem cache keyed only by bucket fails this test.
    assert(actual(read(first)) == ids)
    assert(actual(read(second)) == otherIds)
    Seq(first -> second, second -> first).foreach { case (identity, location) =>
      val error = intercept[Exception] {
        read(
          identity + (MilvusOption.SnapshotPath -> location(
            MilvusOption.SnapshotPath
          ))
        ).collect()
      }
      val messages = Iterator
        .iterate[Throwable](error)(_.getCause)
        .takeWhile(_ != null)
        .map(e => Option(e.getMessage).getOrElse(""))
        .mkString(" ")
        .toLowerCase
      assert(
        messages.contains("accessdenied") || messages.contains(
          "access denied"
        ) ||
          messages.contains("forbidden") || messages.contains("403"),
        messages
      )
    }
    assert(actual(read(first)) == ids)
    assert(actual(read(second)) == otherIds)
  }
}
