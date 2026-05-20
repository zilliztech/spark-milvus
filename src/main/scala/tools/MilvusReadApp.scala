package com.zilliz.spark.connector.tools

import java.io.FileInputStream
import java.net.URI
import java.util.Base64

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.read.{
  MilvusSnapshotReader,
  SnapshotMetadata,
  V2SegmentInfo,
  V2SegmentLoader
}
import com.zilliz.spark.connector.MilvusOption

object MilvusReadApp {

  private[tools] val DataSourceFqcn =
    "com.zilliz.spark.connector.sources.MilvusDataSource"

  private[tools] case class ReadArgs(
      mode: String = "client",
      milvusUri: String = "",
      milvusToken: String = "",
      database: String = "default",
      collection: String = "",
      partitionName: Option[String] = None,
      snapshot: Option[String] = None,
      s3Endpoint: String = "",
      s3Bucket: String = "",
      s3RootPath: String = "files",
      s3AccessKey: String = "",
      s3SecretKey: String = "",
      s3Region: String = "us-east-1",
      s3UseSSL: Boolean = false,
      useIam: Boolean = false,
      fieldIds: Option[String] = None,
      extraColumns: Option[String] = None,
      show: Option[Int] = None,
      count: Boolean = false,
      printSchema: Boolean = false,
      debugRead: Boolean = false,
      select: Option[String] = None,
      where: Option[String] = None,
      outputParquet: Option[String] = None
  )

  private[tools] val BoolFlags: Set[String] = Set(
    "count",
    "print-schema",
    "debug-read",
    "s3-use-ssl",
    "use-iam"
  )

  private[tools] val KvFlags: Set[String] = Set(
    "mode",
    "milvus-uri",
    "milvus-token",
    "database",
    "collection",
    "partition-name",
    "snapshot",
    "s3-endpoint",
    "s3-bucket",
    "s3-root-path",
    "s3-access-key",
    "s3-secret-key",
    "s3-region",
    "field-ids",
    "extra-columns",
    "show",
    "select",
    "where",
    "output-parquet"
  )

  private[tools] val KnownFlags: Set[String] = BoolFlags ++ KvFlags

  private[tools] def validateModeArgs(args: ReadArgs): Unit = {
    args.mode match {
      case "client" =>
        require(
          args.snapshot.isEmpty,
          "--snapshot is only valid in snapshot mode"
        )
      case "snapshot" =>
        require(
          args.snapshot.nonEmpty,
          "--snapshot is required in snapshot mode"
        )
        val clientOnly = Seq(
          "--milvus-uri" -> args.milvusUri.nonEmpty,
          "--milvus-token" -> args.milvusToken.nonEmpty,
          "--collection" -> args.collection.nonEmpty,
          "--partition-name" -> args.partitionName.nonEmpty
        ).collect { case (flag, true) => flag }
        require(
          clientOnly.isEmpty,
          s"Snapshot mode does not accept client-only option(s): ${clientOnly.mkString(", ")}"
        )
      case other =>
        throw new IllegalArgumentException(s"Unsupported mode: $other")
    }
  }

  private[tools] def parseArgs(args: Array[String]): ReadArgs = {
    val parsed = scala.collection.mutable.Map.empty[String, String]
    var i = 0
    while (i < args.length) {
      val token = args(i)
      if (!token.startsWith("--")) {
        throw new IllegalArgumentException(
          s"Unexpected positional argument: $token"
        )
      }
      val key = token.stripPrefix("--")
      if (!KnownFlags.contains(key)) {
        throw new IllegalArgumentException(s"Unknown option: --$key")
      }
      if (BoolFlags.contains(key)) {
        parsed(key) = "true"
        i += 1
      } else {
        if (i + 1 >= args.length || args(i + 1).startsWith("--")) {
          throw new IllegalArgumentException(s"Missing value for --$key")
        }
        parsed(key) = args(i + 1)
        i += 2
      }
    }

    val mode = parsed.getOrElse("mode", "client")
    if (mode != "client" && mode != "snapshot") {
      throw new IllegalArgumentException(
        "--mode must be either 'client' or 'snapshot'"
      )
    }

    val readArgs = ReadArgs(
      mode = mode,
      milvusUri = parsed.getOrElse("milvus-uri", ""),
      milvusToken = parsed.getOrElse("milvus-token", ""),
      database = parsed.getOrElse("database", "default"),
      collection = parsed.getOrElse("collection", ""),
      partitionName = parsed.get("partition-name"),
      snapshot = parsed.get("snapshot"),
      s3Endpoint = parsed.getOrElse("s3-endpoint", ""),
      s3Bucket = parsed.getOrElse("s3-bucket", ""),
      s3RootPath = parsed.getOrElse("s3-root-path", "files"),
      s3AccessKey = parsed.getOrElse("s3-access-key", ""),
      s3SecretKey = parsed.getOrElse("s3-secret-key", ""),
      s3Region = parsed.getOrElse("s3-region", "us-east-1"),
      s3UseSSL = parsed.contains("s3-use-ssl"),
      useIam = parsed.contains("use-iam"),
      fieldIds = parsed.get("field-ids"),
      extraColumns = parsed.get("extra-columns"),
      show = parsed.get("show").map(_.toInt),
      count = parsed.contains("count"),
      printSchema = parsed.contains("print-schema"),
      debugRead = parsed.contains("debug-read"),
      select = parsed.get("select"),
      where = parsed.get("where"),
      outputParquet = parsed.get("output-parquet")
    )
    validateModeArgs(readArgs)
    readArgs
  }

  private[tools] def buildStorageOptions(
      args: ReadArgs
  ): Map[String, String] = {
    require(args.s3Bucket.nonEmpty, "--s3-bucket is required")

    val opts = scala.collection.mutable.Map[String, String](
      Properties.FsConfig.FsBucketName -> args.s3Bucket,
      Properties.FsConfig.FsRootPath -> args.s3RootPath,
      Properties.FsConfig.FsRegion -> args.s3Region,
      Properties.FsConfig.FsUseSSL -> args.s3UseSSL.toString
    )

    if (args.s3Endpoint.nonEmpty) {
      opts += Properties.FsConfig.FsAddress -> args.s3Endpoint
    }
    if (args.s3AccessKey.nonEmpty) {
      opts += Properties.FsConfig.FsAccessKeyId -> args.s3AccessKey
    }
    if (args.s3SecretKey.nonEmpty) {
      opts += Properties.FsConfig.FsAccessKeyValue -> args.s3SecretKey
    }
    if (args.useIam) {
      opts += Properties.FsConfig.FsUseIam -> "true"
    }

    opts.toMap
  }

  private[tools] def buildClientOptions(args: ReadArgs): Map[String, String] = {
    require(args.milvusUri.nonEmpty, "--milvus-uri is required in client mode")
    require(args.collection.nonEmpty, "--collection is required in client mode")

    val opts = scala.collection.mutable.Map[String, String](
      MilvusOption.MilvusUri -> args.milvusUri,
      MilvusOption.MilvusToken -> args.milvusToken,
      MilvusOption.MilvusDatabaseName -> args.database,
      MilvusOption.MilvusCollectionName -> args.collection
    )

    args.partitionName.foreach(v =>
      opts += MilvusOption.MilvusPartitionName -> v
    )
    args.fieldIds.foreach(v => opts += MilvusOption.ReaderFieldIDs -> v)
    args.extraColumns.foreach(v => opts += MilvusOption.MilvusExtraColumns -> v)
    if (args.debugRead) opts += MilvusOption.ReaderDebug -> "true"

    opts.toMap ++ buildStorageOptions(args)
  }

  private[tools] def buildSnapshotOptionsFromMetadata(
      args: ReadArgs,
      metadata: SnapshotMetadata,
      snapshotJson: String,
      v2Segments: Seq[V2SegmentInfo]
  ): Map[String, String] = {
    require(args.snapshot.nonEmpty, "--snapshot is required in snapshot mode")

    val schemaBytes = MilvusSnapshotReader.toProtobufSchemaBytes(
      metadata.collection.schema
    )

    val opts = scala.collection.mutable.Map[String, String](
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.MilvusDatabaseName -> args.database,
      MilvusOption.MilvusCollectionName -> metadata.collection.schema.name,
      MilvusOption.SnapshotCollectionId -> metadata.snapshotInfo.collectionId.toString,
      MilvusOption.SnapshotPartitionIds -> metadata.snapshotInfo.partitionIds
        .mkString(","),
      MilvusOption.SnapshotSchemaJson -> snapshotJson,
      MilvusOption.SnapshotSchemaBytes -> Base64.getEncoder.encodeToString(
        schemaBytes
      )
    )

    opts += MilvusOption.SnapshotManifests ->
      MilvusSnapshotReader.serializeManifestList(
        metadata.storageV2ManifestList.getOrElse(Seq.empty)
      )

    if (v2Segments.nonEmpty) {
      opts += MilvusOption.SnapshotV2Segments ->
        MilvusSnapshotReader.serializeV2Segments(v2Segments)
    }

    args.fieldIds.foreach(v => opts += MilvusOption.ReaderFieldIDs -> v)
    args.extraColumns.foreach(v => opts += MilvusOption.MilvusExtraColumns -> v)
    if (args.debugRead) opts += MilvusOption.ReaderDebug -> "true"

    opts.toMap ++ buildStorageOptions(args)
  }

  private[tools] def buildSnapshotOptions(
      args: ReadArgs,
      hadoopConf: Configuration
  ): Map[String, String] = {
    val snapshotPath = args.snapshot.getOrElse(
      throw new IllegalArgumentException(
        "--snapshot is required in snapshot mode"
      )
    )
    configureHadoopS3A(hadoopConf, args)

    val snapshotJson = readSnapshotJson(snapshotPath, hadoopConf)
    val metadata =
      MilvusSnapshotReader.parseSnapshotMetadata(snapshotJson) match {
        case Right(value) => value
        case Left(err) =>
          throw new IllegalArgumentException(
            s"Failed to parse snapshot metadata: ${err.getMessage}",
            err
          )
      }

    val v2Segments = if (metadata.manifestList.nonEmpty) {
      V2SegmentLoader.loadV2Segments(
        metadata.manifestList,
        args.s3Bucket,
        hadoopConf
      ) match {
        case Right(segs) => segs
        case Left(err) =>
          throw new IllegalStateException(
            s"Failed to load StorageV2 segments from snapshot: ${err.getMessage}",
            err
          )
      }
    } else {
      Seq.empty
    }

    buildSnapshotOptionsFromMetadata(args, metadata, snapshotJson, v2Segments)
  }

  private[tools] def normalizeSnapshotPath(path: String): String = {
    if (path.startsWith("s3://")) "s3a://" + path.stripPrefix("s3://")
    else path
  }

  private[tools] def readLocalSnapshotJson(path: String): String = {
    val in = new FileInputStream(path)
    try MilvusSnapshotReader.readUtf8WithLimit(in, path)
    finally in.close()
  }

  private[tools] def readSnapshotJson(
      path: String,
      hadoopConf: Configuration
  ): String = {
    val normalized = normalizeSnapshotPath(path)
    if (!normalized.contains("://")) {
      readLocalSnapshotJson(normalized)
    } else {
      val uri = new URI(normalized)
      val fs = FileSystem.get(uri, hadoopConf)
      val in = fs.open(new Path(uri))
      try MilvusSnapshotReader.readUtf8WithLimit(in, normalized)
      finally in.close()
    }
  }

  private[tools] def configureHadoopS3A(
      conf: Configuration,
      args: ReadArgs
  ): Unit = {
    if (args.s3Bucket.isEmpty) return

    MilvusOption.configureHadoopS3A(
      conf,
      buildStorageOptions(args),
      args.s3Bucket
    )
  }

  private[tools] def applyTransformations(
      df: DataFrame,
      args: ReadArgs
  ): DataFrame = {
    val filtered = args.where match {
      case Some(expr) if expr.trim.nonEmpty => df.where(expr)
      case _                                => df
    }

    args.select match {
      case Some(raw) if raw.trim.nonEmpty =>
        val cols = raw.split(",").map(_.trim).filter(_.nonEmpty)
        require(cols.nonEmpty, "--select must contain at least one column")
        filtered.select(cols.head, cols.tail: _*)
      case _ => filtered
    }
  }

  private[tools] def readDataFrame(
      spark: SparkSession,
      args: ReadArgs
  ): DataFrame = {
    val options = args.mode match {
      case "client" => buildClientOptions(args)
      case "snapshot" =>
        buildSnapshotOptions(args, spark.sparkContext.hadoopConfiguration)
      case other =>
        throw new IllegalArgumentException(s"Unsupported mode: $other")
    }

    val df = spark.read
      .format(DataSourceFqcn)
      .options(options)
      .load()

    applyTransformations(df, args)
  }

  private[tools] def runActions(df: DataFrame, args: ReadArgs): Unit = {
    val dataActionCount = Seq(
      args.show.map(_ => 1).getOrElse(0),
      if (args.count) 1 else 0,
      if (args.outputParquet.nonEmpty) 1 else 0
    ).sum
    val shouldCache = dataActionCount > 1
    val actionDf =
      if (shouldCache) df.persist(StorageLevel.MEMORY_AND_DISK) else df
    var materializedCount = Option.empty[Long]

    try {
      if (shouldCache) {
        materializedCount = Some(actionDf.count())
      }

      if (args.printSchema) {
        println("\n=== Schema ===")
        actionDf.printSchema()
      }

      args.show.foreach { n =>
        println(s"\n=== Showing $n row(s) ===")
        actionDf.show(n, truncate = false)
      }

      if (args.count) {
        println("\n=== Count ===")
        println(materializedCount.getOrElse(actionDf.count()))
      }

      args.outputParquet.foreach { path =>
        println(s"\n=== Writing parquet output to $path ===")
        actionDf.write.mode("overwrite").parquet(path)
        println(s"Wrote parquet output to $path")
      }
    } finally {
      if (shouldCache) actionDf.unpersist()
    }
  }

  def main(rawArgs: Array[String]): Unit = {
    val args = parseArgs(rawArgs)
    val spark = SparkSession.builder
      .appName("MilvusReadApp")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      println(s"MilvusReadApp mode=${args.mode}")
      val df = readDataFrame(spark, args)
      val hasAction = args.printSchema || args.show.nonEmpty || args.count ||
        args.outputParquet.nonEmpty
      if (hasAction) {
        runActions(df, args)
      } else {
        println(
          "No action selected; defaulting to --print-schema and --show 20"
        )
        df.printSchema()
        df.show(20, truncate = false)
      }
    } finally {
      spark.stop()
    }
  }
}
