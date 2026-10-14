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
import com.zilliz.spark.connector.read.{MilvusSnapshotReader, V2SegmentLoader}
import com.zilliz.spark.connector.sources.MilvusScan

/** Export surviving inserts/updates after one collection's backup cutoff.
  * Inputs must refer to a snapshot taken after stopping and flushing source
  * writes. This is a read-only export, not CDC or a target-side merge.
  */
object MigrationExportApp {
  case class Config(
      sourceInstanceId: String,
      databaseName: String,
      collectionName: String,
      collectionId: Long,
      backupMetaPath: String,
      snapshotPath: String,
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
      "Stop and flush source writes before taking the snapshot"
    )
    require(
      Seq(
        config.sourceInstanceId,
        config.databaseName,
        config.collectionName,
        config.backupMetaPath,
        config.snapshotPath,
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
    val conf = MilvusScan.buildHadoopConfForOptions(
      config.sourceOptions,
      config.snapshotPath,
      spark.sparkContext.hadoopConfiguration
    )
    val snapshot = MilvusSnapshotReader
      .parseSnapshotMetadata(readJson(config.snapshotPath, conf).toString)
      .fold(throw _, identity)
    require(
      snapshot.snapshotInfo.collectionId == config.collectionId &&
        snapshot.collection.schema.name == config.collectionName,
      "Snapshot collection does not match the requested source"
    )
    require(
      snapshot.storageV2ManifestList.forall(_.isEmpty),
      "Only StorageV2 data is supported"
    )
    val cutoff = backupTimestamp(readJson(config.backupMetaPath, conf), config)
    val end = snapshot.snapshotInfo.createTs
    require(end > cutoff, "Final snapshot must be newer than the backup cutoff")
    val pkFields =
      snapshot.collection.schema.fields.filter(_.isPrimaryKey.contains(true))
    require(pkFields.size == 1, "Exactly one primary key is required")
    val userFields =
      snapshot.collection.schema.fields.filter(_.getFieldIDAsLong >= 100L)
    require(
      userFields.contains(pkFields.head),
      "Primary key field ID is missing from the snapshot schema"
    )
    require(
      !snapshot.collection.schema.enableDynamicField.contains(true) ||
        userFields.exists(_.isDynamic.contains(true)),
      "Dynamic field metadata is missing; refusing to silently omit dynamic values"
    )
    require(
      !userFields.exists(f =>
        Set("RowID", "Timestamp", RankColumn).contains(f.name)
      ),
      "User fields conflict with migration metadata columns"
    )
    val segments = V2SegmentLoader
      .loadV2Segments(
        snapshot.manifestList,
        bucket,
        conf,
        manifestSchemaVersion = snapshot.manifestSchemaVersion,
        applyDeletes = true,
        storageScheme = "s3a",
        requireV2Data = true
      )
      .fold(throw _, identity)
    val options = nativeSourceOptions(config.sourceOptions, conf) ++ Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotV2Segments -> MilvusSnapshotReader
        .serializeV2Segments(segments),
      MilvusOption.SnapshotCollectionId -> config.collectionId.toString,
      MilvusOption.SnapshotPartitionIds -> snapshot.snapshotInfo.partitionIds
        .mkString(","),
      MilvusOption.SnapshotSchemaBytes -> Base64.getEncoder.encodeToString(
        MilvusSnapshotReader.toProtobufSchemaBytes(snapshot.collection.schema)
      ),
      MilvusOption.MilvusCollectionName -> config.collectionName,
      MilvusOption.ReadApplyDeletes -> "true",
      // Explicit IDs avoid positional field-ID inference after Spark pruning.
      MilvusOption.ReaderFieldIDs -> (Seq(0L, 1L) ++ userFields.map(
        _.getFieldIDAsLong
      )).mkString(",")
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
        manifest.put("snapshotPath", config.snapshotPath)
        manifest.put("backupMetaPath", config.backupMetaPath)
        manifest.put("backupTimestampExclusive", cutoff.toString)
        manifest.put("snapshotTimestampInclusive", end.toString)
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
      "Invalid primary key/timestamp or source contains rows newer than the frozen snapshot"
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
