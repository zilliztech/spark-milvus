package com.zilliz.spark.connector.operations.migration

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import com.zilliz.spark.connector.loon.Properties.FsConfig
import com.zilliz.spark.connector.TencentStorageAuth

class MigrationExportAppTest extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private val mapper = new ObjectMapper()
  private val config = MigrationExportApp.Config(
    "in01-source",
    "default",
    "items",
    9L,
    "backup/meta/full_meta.json",
    "snapshot.json",
    "output",
    Map.empty,
    true
  )

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("MigrationExportAppTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }
  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test(
    "cutoff comes from the exact database/collection metadata, including default database"
  ) {
    val meta = mapper.readTree("""{"collection_backups":[
      {"collection_id":"9","collection_name":"items","db_name":"default","backup_timestamp":"200"},
      {"collection_id":"10","collection_name":"items","db_name":"other","backup_timestamp":"500"}
    ]}""")
    assert(MigrationExportApp.backupTimestamp(meta, config) == 200L)
    assertThrows[IllegalArgumentException] {
      MigrationExportApp.backupTimestamp(
        meta,
        config.copy(databaseName = "other")
      )
    }
    val defaultMeta = mapper.readTree("""{"collection_backups":[
      {"collection_id":9,"collection_name":"items","backup_timestamp":201}]}""")
    assert(MigrationExportApp.backupTimestamp(defaultMeta, config) == 201L)
  }

  test(
    "missing, invalid and ambiguous backup cutoffs fail instead of using a job start time"
  ) {
    Seq("null", "0", "1.5", "\"bad\"", "\"9223372036854775808\"").foreach {
      ts =>
        val meta = mapper.readTree(
          s"""{"collection_backups":[
        {"collection_id":9,"collection_name":"items","backup_timestamp":$ts}]}"""
        )
        assertThrows[IllegalArgumentException](
          MigrationExportApp.backupTimestamp(meta, config)
        )
    }
    val repeated = mapper.readTree("""{"collection_backups":[
      {"collection_id":9,"collection_name":"items","backup_timestamp":200},
      {"collection_id":9,"collection_name":"items","backup_timestamp":300}]}""")
    assertThrows[IllegalArgumentException](
      MigrationExportApp.backupTimestamp(repeated, config)
    )
  }

  test(
    "export selects the final insert/upsert values in (backup, snapshot], preserving payload"
  ) {
    val session = spark
    import session.implicits._
    val input = Seq(
      (1L, 10L, 100L, "unchanged", Seq(1.0f, 2.0f)),
      (2L, 11L, 200L, "at backup", Seq(2.0f, 3.0f)),
      (3L, 12L, 201L, "insert", Seq(3.0f, 4.0f)),
      (4L, 13L, 210L, "old update", Seq(4.0f, 5.0f)),
      (4L, 14L, 300L, "latest update", Seq(6.0f, 7.0f))
    ).toDF("id", "RowID", "Timestamp", "value", "vector")
    val changes = MigrationExportApp.changedRows(input, "id", 200L, 300L)
    assert(
      changes.select("id", "value").as[(Long, String)].collect().toSet ==
        Set((3L, "insert"), (4L, "latest update"))
    )
    val output =
      Files.createTempDirectory("migration-export-").resolve("result")
    val manifest = mapper.readTree("""{"rowCount":2}""")
    MigrationExportApp.writeExport(
      changes,
      "id",
      output.toString,
      manifest,
      spark.sparkContext.hadoopConfiguration
    )
    val rows = spark.read.parquet(output.resolve("rows").toString)
    assert(rows.columns.toSeq == Seq("id", "value", "vector"))
    assert(
      rows.filter("id = 4").select("vector").as[Seq[Float]].head() == Seq(
        6.0f,
        7.0f
      )
    )
    assert(spark.read.parquet(output.resolve("versions").toString).count() == 2)
    assert(Files.exists(output.resolve("manifest.json")))
    assertThrows[IllegalArgumentException] {
      MigrationExportApp.writeExport(
        changes,
        "id",
        output.toString,
        manifest,
        spark.sparkContext.hadoopConfiguration
      )
    }
  }

  test(
    "VarChar keys and nullable fields survive; null keys and future timestamps fail"
  ) {
    val schema = StructType(
      Seq(
        StructField("count", StringType),
        StructField("Timestamp", LongType),
        StructField("value", StringType)
      )
    )
    def input(rows: Row*) =
      spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val result = MigrationExportApp.changedRows(
      input(Row("a", 201L, null)),
      "count",
      200L,
      300L
    )
    assert(result.head().isNullAt(2))
    Seq(Row(null, 201L, "bad"), Row("a", null, "bad"), Row("a", 301L, "future"))
      .foreach { row =>
        assertThrows[IllegalArgumentException] {
          MigrationExportApp.changedRows(input(row), "count", 200L, 300L)
        }
      }
    assertThrows[IllegalArgumentException] {
      MigrationExportApp.changedRows(
        input(Row("a", 201L, "x"), Row("a", 201L, "y")),
        "count",
        200L,
        300L
      )
    }
    assert(
      MigrationExportApp
        .changedRows(input(Row("a", 200L, "old")), "count", 200L, 300L)
        .count() == 0
    )
  }

  test("an unfrozen source fails before opening any source or output files") {
    assertThrows[IllegalArgumentException] {
      MigrationExportApp.run(spark, config.copy(sourceWritesStopped = false))
    }
  }

  test("failed Parquet output does not publish a completion manifest") {
    val session = spark
    import session.implicits._
    val changes = Seq((1L, 201L))
      .toDF("id", "Timestamp")
      .withColumn("unsupported", org.apache.spark.sql.functions.lit(null))
    val output =
      Files.createTempDirectory("migration-failed-").resolve("result")
    assertThrows[Exception] {
      MigrationExportApp.writeExport(
        changes,
        "id",
        output.toString,
        mapper.createObjectNode(),
        spark.sparkContext.hadoopConfiguration
      )
    }
    assert(Files.exists(output.resolve("_EXPORT_STARTED")))
    assert(!Files.exists(output.resolve("manifest.json")))
  }

  test("Tencent native reads use the same runtime data role as Hadoop") {
    val options = Map(
      FsConfig.FsBucketName -> "source",
      FsConfig.FsUseIam -> "true",
      FsConfig.FsCloudProvider -> "tencent"
    )
    val conf = new Configuration(false)
    assertThrows[IllegalArgumentException](
      MigrationExportApp.nativeSourceOptions(options, conf)
    )
    conf.set(
      "fs.s3a.aws.credentials.provider",
      TencentStorageAuth.CredentialsProvider
    )
    conf.set("fs.s3a.assumed.role.arn", "qcs::cam::uin/123:roleName/data-role")
    conf.set("fs.s3a.assumed.role.session.name", "migration")
    val resolved = MigrationExportApp.nativeSourceOptions(options, conf)
    assert(
      resolved(FsConfig.FsRoleArn) == "qcs::cam::uin/123:roleName/data-role"
    )
    assert(resolved(FsConfig.FsSessionName) == "migration")
    assertThrows[IllegalArgumentException] {
      MigrationExportApp.nativeSourceOptions(
        options + (FsConfig.FsRoleArn -> "different"),
        conf
      )
    }
  }

  test(
    "the complete app accepts an empty frozen V2 snapshot and emits a zero-row manifest"
  ) {
    val dir = Files.createTempDirectory("migration-app-")
    val snapshot = """{
      "snapshot-info":{"id":1,"name":"final","collection_id":9,"create_ts":300,"partition_ids":[]},
      "collection":{"schema":{"name":"items","fields":[
        {"fieldID":100,"name":"id","is_primary_key":true,"data_type":5}]}},
      "manifest-list":[]
    }"""
    val backup = """{"collection_backups":[
      {"collection_id":9,"collection_name":"items","backup_timestamp":200}]}"""
    Files.writeString(dir.resolve("snapshot.json"), snapshot)
    Files.writeString(dir.resolve("backup.json"), backup)
    MigrationExportApp.run(
      spark,
      config.copy(
        snapshotPath = dir.resolve("snapshot.json").toString,
        backupMetaPath = dir.resolve("backup.json").toString,
        outputPath = dir.resolve("output").toString,
        sourceOptions = Map(
          FsConfig.FsBucketName -> "unused",
          FsConfig.FsStorageType -> "local"
        )
      )
    )
    assert(
      mapper
        .readTree(dir.resolve("output/manifest.json").toFile)
        .path("rowCount")
        .asLong() == 0L
    )
    assert(
      spark.read
        .parquet(dir.resolve("output/rows").toString)
        .columns
        .toSeq == Seq("id")
    )
  }
}
