package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Integration tests for the Iceberg backfill input reader.
  *
  * Exercises the read mechanism `MilvusBackfill.readIceberg` relies on: an
  * Iceberg table in a user-registered hadoop catalog on MinIO, loaded by
  * catalog-qualified identifier via `spark.read.format("iceberg").load(...)`,
  * including `snapshot-id` time travel. Reads go through Hadoop FS (S3A),
  * matching the `--source-s3-*` per-bucket credential path.
  *
  * Note: Iceberg 1.10 routes raw *path* reads (`load("s3a://...")`) through a
  * default catalog hardcoded to `type=hive`, which needs a Hive metastore;
  * catalog-qualified identifiers are the supported hadoop-catalog read form.
  *
  * Prerequisites: MinIO running at localhost:9000 with a pre-created
  * `a-bucket` and minioadmin/minioadmin credentials.
  */
class IcebergReadMinioIT extends AnyFunSuite with Matchers {

  private val catalog = "iceberg_cat"
  private val warehouse = "s3a://a-bucket/iceberg-warehouse"
  private val table = "backfill_input"
  private val tableIdentifier = s"$catalog.db.$table"

  private def newSpark(): SparkSession =
    SparkSession
      .builder()
      .appName("IcebergReadMinioTest")
      .master("local[*]")
      .config(
        s"spark.sql.catalog.$catalog",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config(s"spark.sql.catalog.$catalog.type", "hadoop")
      .config(s"spark.sql.catalog.$catalog.warehouse", warehouse)
      .config("spark.hadoop.fs.s3a.endpoint", "localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

  private def currentSnapshotId(spark: SparkSession): Long =
    spark
      .sql(
        s"SELECT snapshot_id FROM $catalog.db.$table.snapshots " +
          "ORDER BY committed_at DESC LIMIT 1"
      )
      .head()
      .getLong(0)

  test("read an Iceberg table by catalog identifier from MinIO") {
    val spark = newSpark()
    try {
      import spark.implicits._
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
      spark.sql(
        s"CREATE TABLE $tableIdentifier " +
          "(pk LONG, new_field STRING, embedding ARRAY<DOUBLE>) USING iceberg"
      )
      Seq(
        (1L, "alpha", Seq(0.1, 0.2, 0.3)),
        (2L, "beta", Seq(0.4, 0.5, 0.6))
      ).toDF("pk", "new_field", "embedding")
        .writeTo(tableIdentifier)
        .append()

      val df = spark.read.format("iceberg").load(tableIdentifier)
      df.count() shouldBe 2
      df.schema.fieldNames should contain allOf (
        "pk",
        "new_field",
        "embedding"
      )
      df
        .select("new_field")
        .orderBy("pk")
        .collect()
        .map(_.getString(0)) shouldBe Array("alpha", "beta")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableIdentifier")
      spark.stop()
    }
  }

  test("snapshot-id option time-travels an Iceberg input table") {
    val spark = newSpark()
    try {
      import spark.implicits._
      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
      spark.sql(
        s"CREATE TABLE $tableIdentifier (pk LONG, new_field STRING) USING iceberg"
      )
      Seq((1L, "v1"), (2L, "v2"))
        .toDF("pk", "new_field")
        .writeTo(tableIdentifier)
        .append()
      val firstSnapshot = currentSnapshotId(spark)

      Seq((3L, "v3"), (4L, "v4"))
        .toDF("pk", "new_field")
        .writeTo(tableIdentifier)
        .append()

      spark.read.format("iceberg").load(tableIdentifier).count() shouldBe 4
      spark.read
        .format("iceberg")
        .option("snapshot-id", firstSnapshot.toString)
        .load(tableIdentifier)
        .count() shouldBe 2
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableIdentifier")
      spark.stop()
    }
  }
}
