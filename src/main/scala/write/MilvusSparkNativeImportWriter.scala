package com.zilliz.spark.connector.sources

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.{MilvusClient, MilvusOption, MilvusS3Option}

class MilvusSparkNativeImportWriter(
    uri: String,
    token: String,
    collectionName: String,
    options: Map[String, String]
) extends Logging {

  private val optionsMap = mutable.Map[String, String]()
  options.foreach { case (key, value) =>
    optionsMap.put(key, value)
  }
  optionsMap.put(MilvusOption.MilvusUri, uri)
  optionsMap.put(MilvusOption.MilvusToken, token)
  optionsMap.put(MilvusOption.MilvusCollectionName, collectionName)

  private val milvusOption = MilvusOption(
    new CaseInsensitiveStringMap(optionsMap.asJava)
  )

  private val s3Option = MilvusS3Option(
    new CaseInsensitiveStringMap(optionsMap.asJava)
  )

  def write(df: DataFrame): Try[Seq[Long]] = {
    val spark = df.sparkSession

    try {
      val s3Path = generateS3Path()
      logInfo(s"Writing data to S3 path: $s3Path")

      writeToS3WithSpark(df, s3Path) match {
        case Success(_) =>
          logInfo("Successfully wrote data to S3 using Spark native writer")

          collectS3FilesAndImport(s3Path)

        case Failure(e) =>
          logError(s"Failed to write data to S3: ${e.getMessage}")
          Failure(e)
      }
    } catch {
      case e: Exception =>
        logError(s"Import write failed: ${e.getMessage}")
        Failure(e)
    }
  }

  private def writeToS3WithSpark(df: DataFrame, s3Path: String): Try[Unit] = {
    try {
      val fullS3Path =
        s"s3a://${s3Option.s3BucketName}/${s3Option.s3RootPath}/${s3Path}"

      val spark = df.sparkSession
      configureSparkS3Settings(spark)

      df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(fullS3Path)

      logInfo(s"Successfully wrote ${df.count()} rows to $fullS3Path")
      Success(())
    } catch {
      case e: Exception =>
        logError(s"Failed to write to S3 with Spark: ${e.getMessage}")
        Failure(e)
    }
  }

  private def configureSparkS3Settings(spark: SparkSession): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration

    // Basic S3 connection settings
    conf.set("fs.s3a.endpoint", s3Option.s3Endpoint)
    conf.set("fs.s3a.access.key", s3Option.s3AccessKey)
    conf.set("fs.s3a.secret.key", s3Option.s3SecretKey)
    conf.set("fs.s3a.path.style.access", s3Option.s3PathStyleAccess.toString)
    conf.set("fs.s3a.connection.ssl.enabled", s3Option.s3UseSSL.toString)

    // Performance optimization settings
    // Multipart upload size: 128MB (134217728 bytes) - larger parts reduce overhead for big files
    conf.set("fs.s3a.multipart.size", "134217728")
    conf.set("fs.s3a.connection.maximum", "32")
    conf.set("fs.s3a.connection.timeout", "30000")
    conf.set("fs.s3a.socket.timeout", "30000")
    conf.set("fs.s3a.retry.limit", "3")
    // Fast upload mode: enabled - uses multiple threads and buffering for better performance
    conf.set("fs.s3a.fast.upload", "true")
    // Maximum upload threads: 32 - parallel uploads for better throughput
    conf.set("fs.s3a.threads.max", "32")
    // Core upload threads: 16 - minimum threads kept alive for consistent performance
    conf.set("fs.s3a.threads.core", "16")
    // Upload buffer type: disk - uses local disk for buffering to avoid memory pressure
    conf.set("fs.s3a.fast.upload.buffer", "disk")
    // Active upload blocks: 8 - number of concurrent multipart uploads per file
    conf.set("fs.s3a.fast.upload.active.blocks", "8")

    logInfo("Configured Spark S3 settings for optimal performance")
  }

  private def collectS3FilesAndImport(s3Path: String): Try[Seq[Long]] = {
    try {
      val conf = s3Option.getConf()
      val fullS3Path =
        s"s3a://${s3Option.s3BucketName}/${s3Option.s3RootPath}/$s3Path"
      val path = new Path(fullS3Path)
      val fs = path.getFileSystem(conf)

      val parquetFiles = collectParquetFiles(fs, path)
      logInfo(
        s"Found ${parquetFiles.size} parquet files: ${parquetFiles.mkString(", ")}"
      )

      if (parquetFiles.isEmpty) {
        logWarning("No parquet files found for import")
        Success(Seq.empty)
      } else {
        callMilvusImport(parquetFiles)
      }
    } catch {
      case e: Exception =>
        logError(s"Failed to collect S3 files and import: ${e.getMessage}")
        Failure(e)
    }
  }

  private def collectParquetFiles(fs: FileSystem, path: Path): Seq[String] = {
    val files = fs.listStatus(path)
    val parquetFiles = scala.collection.mutable.ArrayBuffer[String]()

    files.foreach { fileStatus =>
      if (fileStatus.isDirectory) {
        parquetFiles ++= collectParquetFiles(fs, fileStatus.getPath)
      } else if (fileStatus.getPath.getName.endsWith(".parquet")) {
        val s3Path = fileStatus.getPath.toString.replace(
          s"s3a://${s3Option.s3BucketName}",
          ""
        )
        parquetFiles += s3Path
      }
    }

    parquetFiles.toSeq
  }

  private def callMilvusImport(filePaths: Seq[String]): Try[Seq[Long]] = {
    val client = MilvusClient(milvusOption)
    try {
      val result = client.importData(
        dbName = milvusOption.databaseName,
        collectionName = milvusOption.collectionName,
        partitionName =
          if (milvusOption.partitionName.nonEmpty)
            Some(milvusOption.partitionName)
          else None,
        files = filePaths,
        rowBased = false
      )

      result match {
        case Success(jobIds) =>
          logInfo(
            s"Import started successfully with job IDs: ${jobIds.mkString(", ")}"
          )
          Success(jobIds)
        case Failure(e) =>
          logError(s"Import failed: ${e.getMessage}")
          Failure(e)
      }
    } finally {
      client.close()
    }
  }

  private def generateS3Path(): String = {
    s"spark-import/${milvusOption.collectionName}/${System.currentTimeMillis()}"
  }
}

object MilvusSparkNativeImportWriter {

  // TODO: auto id
  // TODO: dynamic field
  // TODO: complicate data type, include array, sparse/f16/bf vector
  def writeDataFrame(
      df: DataFrame,
      uri: String,
      token: String,
      collectionName: String,
      options: Map[String, String]
  ): Try[Seq[Long]] = {
    val writer =
      new MilvusSparkNativeImportWriter(uri, token, collectionName, options)
    writer.write(df)
  }
}
