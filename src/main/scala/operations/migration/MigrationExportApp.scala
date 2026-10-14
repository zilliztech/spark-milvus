package com.zilliz.spark.connector.operations.migration

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.storage.StorageLevel

import com.zilliz.spark.connector.{MilvusOption, TencentStorageAuth}
import com.zilliz.spark.connector.loon.Properties.FsConfig
import com.zilliz.spark.connector.read.{BackupMetaReader, MilvusSnapshotReader}
import com.zilliz.spark.connector.sources.MilvusScan

/** Export surviving inserts/updates after one collection's backup cutoff.
  * Inputs must refer to a binlog backup taken after stopping and flushing
  * source writes. This is a read-only export, not CDC or a target-side merge.
  */
object MigrationExportApp {
  case class Config(
      sourceInstanceId: String,
      databaseName: String,
      collectionName: String,
      collectionId: Long,
      backupMetaPath: String,
      finalBackupDir: String,
      outputPath: String,
      sourceOptions: Map[String, String],
      sourceWritesStopped: Boolean = false
  )

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val RankColumn = "__migration_version_rank"

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Usage: MigrationExportApp <config.json URI>")
    val spark =
      SparkSession.builder().appName("MilvusMigrationExport").getOrCreate()
    try {
      val conf = spark.sparkContext.hadoopConfiguration
      val config = mapper.treeToValue(readJson(args(0), conf), classOf[Config])
      run(spark, config)
    } finally spark.stop()
  }

  private[migration] def run(spark: SparkSession, config: Config): Unit = {
    require(
      config.sourceWritesStopped,
      "Stop and flush source writes before taking the final backup"
    )
    require(
      Seq(
        config.sourceInstanceId,
        config.databaseName,
        config.collectionName,
        config.backupMetaPath,
        config.finalBackupDir,
        config.outputPath
      )
        .forall(s => s != null && s.trim.nonEmpty),
      "Source identity and paths are required"
    )
    require(config.collectionId > 0, "collectionId must be positive")
    require(
      config.sourceOptions != null && config.sourceOptions.keys
        .forall(_.startsWith("fs.")),
      "sourceOptions accepts only filesystem options; read semantics cannot be overridden"
    )
    val bucket = config.sourceOptions.getOrElse(FsConfig.FsBucketName, "")
    require(bucket.nonEmpty, "sourceOptions.fs.bucket_name is required")
    val backupBucket = new Path(config.finalBackupDir).toUri.getAuthority
    require(
      backupBucket == null || backupBucket == bucket,
      "sourceOptions.fs.bucket_name must match the final backup bucket"
    )
    val conf = MilvusScan.buildHadoopConfForOptions(
      config.sourceOptions,
      config.finalBackupDir,
      spark.sparkContext.hadoopConfiguration
    )
    val finalMeta =
      readJson(BackupMetaReader.metaPath(config.finalBackupDir), conf)
    val backup =
      BackupMetaReader.parse(finalMeta.toString).fold(throw _, identity)
    require(
      backup.format.isEmpty || backup.format == "binlog",
      "The final input must be a Milvus 2.6-compatible binlog backup"
    )
    val cutoff = backupTimestamp(readJson(config.backupMetaPath, conf), config)
    val end = backupTimestamp(finalMeta, config)
    require(end > cutoff, "Final backup must be newer than the baseline backup")
    val collections =
      backup.collectionBackups.filter(_.collectionId == config.collectionId)
    require(
      collections.size == 1,
      "Final backup contains ambiguous collection IDs"
    )
    val collection = collections.head
    val schema = collection.schema.getOrElse {
      throw new IllegalArgumentException(
        "Final backup collection schema is missing"
      )
    }
    val schemaBytes = BackupMetaReader.toProtobufSchemaBytes(schema)
    val pkFields = schema.fields.filter(_.isPrimaryKey)
    require(pkFields.size == 1, "Exactly one primary key is required")
    val userFields = schema.fields.filter(_.fieldId >= 100L)
    require(
      userFields.contains(pkFields.head),
      "Primary key field ID is missing from the backup schema"
    )
    require(
      !userFields.exists(f =>
        Set("RowID", "Timestamp", RankColumn).contains(f.name)
      ),
      "User fields conflict with migration metadata columns"
    )
    val segments = BackupMetaReader
      .toV2Segments(
        backup,
        conf,
        config.finalBackupDir,
        applyDeletes = true,
        collectionId = config.collectionId
      )
      .fold(throw _, identity)
    // SnapshotMode is the connector's offline segment-plan format. The plan
    // below comes entirely from a 2.6 binlog backup, not Milvus Snapshot APIs.
    val options = nativeSourceOptions(config.sourceOptions, conf) ++ Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotV2Segments -> MilvusSnapshotReader
        .serializeV2Segments(segments),
      MilvusOption.SnapshotCollectionId -> config.collectionId.toString,
      MilvusOption.SnapshotPartitionIds -> collection.partitionBackups
        .map(_.partitionId)
        .mkString(","),
      MilvusOption.SnapshotSchemaBytes -> Base64.getEncoder.encodeToString(
        schemaBytes
      ),
      MilvusOption.MilvusCollectionName -> config.collectionName,
      MilvusOption.ReadApplyDeletes -> "true",
      // Explicit IDs avoid positional field-ID inference after Spark pruning.
      MilvusOption.ReaderFieldIDs -> (Seq(0L, 1L) ++ userFields.map(_.fieldId))
        .mkString(",")
    )
    val source = spark.read
      .format("com.zilliz.spark.connector.sources.MilvusDataSource")
      .options(options)
      .load()
      .persist(StorageLevel.DISK_ONLY)
    try {
      val changes = changedRows(source, pkFields.head.name, cutoff, end)
        .persist(StorageLevel.DISK_ONLY)
      try {
        val count = changes.count()
        val manifest = mapper.createObjectNode()
        manifest.put("kind", "surviving-inserts-and-updates")
        manifest.put("sourceInstanceId", config.sourceInstanceId)
        manifest.put("databaseName", config.databaseName)
        manifest.put("collectionName", config.collectionName)
        manifest.put("collectionId", config.collectionId.toString)
        manifest.put("finalBackupDir", config.finalBackupDir)
        manifest.put("backupMetaPath", config.backupMetaPath)
        manifest.put("backupTimestampExclusive", cutoff.toString)
        manifest.put("finalBackupTimestampInclusive", end.toString)
        manifest.put("primaryKey", pkFields.head.name)
        manifest.put("rowCount", count)
        writeExport(
          changes,
          pkFields.head.name,
          config.outputPath,
          manifest,
          conf
        )
      } finally changes.unpersist()
    } finally source.unpersist()
  }

  private[migration] def nativeSourceOptions(
      options: Map[String, String],
      conf: Configuration
  ): Map[String, String] = {
    if (
      options.get(FsConfig.FsCloudProvider).contains("tencent") &&
      options.get(FsConfig.FsUseIam).exists(_.equalsIgnoreCase("true"))
    ) {
      val role = TencentStorageAuth
        .resolve(conf, options(FsConfig.FsBucketName))
        .getOrElse {
          throw new IllegalArgumentException(
            "Tencent export requires the runtime COS data role"
          )
        }
      val nativeRole = Map(FsConfig.FsRoleArn -> role.arn) ++
        role.sessionName.map(FsConfig.FsSessionName -> _) ++
        role.externalId.map(FsConfig.FsExternalId -> _)
      Seq(FsConfig.FsRoleArn, FsConfig.FsSessionName, FsConfig.FsExternalId)
        .foreach { key =>
          require(
            options.get(key).forall(v => nativeRole.get(key).contains(v)),
            "Native and Hadoop Tencent role settings must match"
          )
        }
      options ++ nativeRole
    } else options
  }

  private[migration] def backupTimestamp(
      meta: JsonNode,
      config: Config
  ): Long = {
    val collections = meta.path("collection_backups")
    require(
      collections.isArray,
      "Backup metadata must contain collection_backups"
    )
    val matches = collections
      .elements()
      .asScala
      .filter { c =>
        c.path("collection_id").asText() == config.collectionId.toString &&
        c.path("collection_name").asText() == config.collectionName &&
        (c.path("db_name").asText() match {
          case "" => config.databaseName == "default"
          case db => db == config.databaseName
        })
      }
      .toSeq
    require(
      matches.size == 1,
      "Backup must contain exactly one matching database/collection ID/name"
    )
    val ts = matches.head.path("backup_timestamp")
    require(
      ts.isIntegralNumber || ts.isTextual,
      "Collection backup_timestamp is required"
    )
    val value = ts.asText().toLong
    require(
      value > 0,
      "Collection backup_timestamp must be a positive Milvus logical timestamp"
    )
    value
  }

  private[migration] def changedRows(
      source: DataFrame,
      pk: String,
      cutoff: Long,
      end: Long
  ): DataFrame = {
    require(cutoff > 0 && end > cutoff, "Invalid migration timestamp window")
    require(
      source.schema.fieldNames.distinct.length == source.schema.length,
      "Duplicate source column names"
    )
    require(
      source.schema("Timestamp").dataType == LongType,
      "Timestamp must be Int64"
    )
    require(
      source
        .schema(pk)
        .dataType == LongType || source.schema(pk).dataType == StringType,
      "Primary key must be Int64 or VarChar"
    )
    require(!source.columns.contains(RankColumn), "Reserved migration column")
    def quoted(name: String) = col("`" + name.replace("`", "``") + "`")
    require(
      source
        .filter(
          quoted(pk).isNull || col("Timestamp").isNull ||
            col("Timestamp") <= 0L || col("Timestamp") > end
        )
        .limit(1)
        .count() == 0,
      "Invalid primary key/timestamp or source contains rows newer than the final backup"
    )
    val latest = source
      .withColumn(
        RankColumn,
        dense_rank().over(
          Window.partitionBy(quoted(pk)).orderBy(col("Timestamp").desc)
        )
      )
      .filter(col(RankColumn) === 1)
      .drop(RankColumn)
      .filter(col("Timestamp") > cutoff)
    require(
      latest
        .groupBy(quoted(pk))
        .agg(count(lit(1)).as(RankColumn))
        .filter(col(RankColumn) > 1)
        .limit(1)
        .count() == 0,
      "Ambiguous source: multiple latest rows for the same primary key"
    )
    latest
  }

  private[migration] def writeExport(
      changes: DataFrame,
      pk: String,
      output: String,
      manifest: JsonNode,
      conf: Configuration
  ): Unit = {
    val root = new Path(output)
    val fs = root.getFileSystem(conf)
    require(
      !fs.exists(root),
      "Export path already exists; use a fresh path for every attempt"
    )
    // Never treat a partially written export as complete. The manifest is last.
    val lock = fs.create(new Path(root, "_EXPORT_STARTED"), false)
    lock.close()
    changes
      .drop("RowID", "Timestamp")
      .write
      .mode("errorifexists")
      .parquet(new Path(root, "rows").toString)
    changes
      .select(col("`" + pk.replace("`", "``") + "`"), col("Timestamp"))
      .write
      .mode("errorifexists")
      .parquet(new Path(root, "versions").toString)
    val out = fs.create(new Path(root, "manifest.json"), false)
    try out.write(mapper.writeValueAsString(manifest).getBytes(UTF_8))
    finally out.close()
  }

  private def readJson(path: String, conf: Configuration): JsonNode = {
    val p = new Path(path)
    val in = p.getFileSystem(conf).open(p)
    try mapper.readTree(in)
    finally in.close()
  }
}
