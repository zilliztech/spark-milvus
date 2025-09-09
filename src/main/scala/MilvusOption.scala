package com.zilliz.spark.connector

import java.net.URI
import scala.collection.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.binlog.Constants
import com.zilliz.spark.connector.MilvusConnectionException

case class MilvusOption(
    uri: String,
    token: String = "",
    serverPemPath: String = "",
    clientKeyPath: String = "",
    clientPemPath: String = "",
    caPemPath: String = "",
    databaseName: String = "",
    collectionName: String = "",
    partitionName: String = "",
    collectionPKType: String = "",
    insertMaxBatchSize: Int = 0,
    retryCount: Int = 3,
    retryInterval: Int = 1000,
    collectionID: String = "",
    partitionID: String = "",
    segmentID: String = "",
    fieldID: String = "",
    fieldIDs: String = "",
    extraColumns: Seq[String] = Seq.empty
)

object MilvusOption {
  // Constants for map keys
  val MilvusUri = "milvus.uri"
  val MilvusToken = "milvus.token"
  val MilvusServerPemPath = "milvus.server.pem"
  val MilvusClientKeyPath = "milvus.client.key"
  val MilvusClientPemPath = "milvus.client.pem"
  val MilvusCaPemPath = "milvus.ca.pem"
  val MilvusDatabaseName = "milvus.database.name"
  val MilvusCollectionName = "milvus.collection.name"
  val MilvusPartitionName = "milvus.partition.name"
  val MilvusCollectionPKType = "milvus.collection.pkType"
  val MilvusCollectionID = "milvus.collection.id"
  val MilvusPartitionID = "milvus.partition.id"
  val MilvusSegmentID = "milvus.segment.id"
  val MilvusFieldID = "milvus.field.id"
  val MilvusInsertMaxBatchSize = "milvus.insertMaxBatchSize"
  val MilvusRetryCount = "milvus.retry.count"
  val MilvusRetryInterval = "milvus.retry.interval"

  val MilvusExtraColumns = "milvus.extra.columns"
  val MilvusExtraColumnPartition = "partition"

  // reader config
  val ReaderPath = Constants.LogReaderPathParamName
  val ReaderType = Constants.LogReaderTypeParamName
  val ReaderFieldIDs = Constants.LogReaderFieldIDs

  // s3 config
  val S3FileSystemTypeName = Constants.S3FileSystemTypeName
  val S3Endpoint = Constants.S3Endpoint
  val S3BucketName = Constants.S3BucketName
  val S3RootPath = Constants.S3RootPath
  val S3AccessKey = Constants.S3AccessKey
  val S3SecretKey = Constants.S3SecretKey
  val S3UseSSL = Constants.S3UseSSL
  val S3PathStyleAccess = Constants.S3PathStyleAccess

  // Create MilvusOption from a map
  def apply(options: CaseInsensitiveStringMap): MilvusOption = {
    val uri = options.getOrDefault(MilvusUri, "")
    val token = options.getOrDefault(MilvusToken, "")
    val serverPemPath = options.getOrDefault(MilvusServerPemPath, "")
    val clientKeyPath = options.getOrDefault(MilvusClientKeyPath, "")
    val clientPemPath = options.getOrDefault(MilvusClientPemPath, "")
    val caPemPath = options.getOrDefault(MilvusCaPemPath, "")

    val databaseName = options.getOrDefault(MilvusDatabaseName, "")
    val collectionName = options.getOrDefault(MilvusCollectionName, "")
    val partitionName = options.getOrDefault(MilvusPartitionName, "")
    val collectionPKType = options.getOrDefault(MilvusCollectionPKType, "")
    val collectionID = options.getOrDefault(MilvusCollectionID, "")
    val partitionID = options.getOrDefault(MilvusPartitionID, "")
    val segmentID = options.getOrDefault(MilvusSegmentID, "")
    val fieldID = options.getOrDefault(MilvusFieldID, "")
    val insertMaxBatchSize =
      options.getOrDefault(MilvusInsertMaxBatchSize, "5000").toInt
    val retryCount = options.getOrDefault(MilvusRetryCount, "3").toInt
    val retryInterval =
      options.getOrDefault(MilvusRetryInterval, "1000").toInt
    val fieldIDs = options.getOrDefault(ReaderFieldIDs, "")
    val extraColumns = options
      .getOrDefault(MilvusExtraColumns, "")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSeq

    MilvusOption(
      uri,
      token,
      serverPemPath,
      clientKeyPath,
      clientPemPath,
      caPemPath,
      databaseName,
      collectionName,
      partitionName,
      collectionPKType,
      insertMaxBatchSize,
      retryCount,
      retryInterval,
      collectionID,
      partitionID,
      segmentID,
      fieldID,
      fieldIDs,
      extraColumns
    )
  }

  def isInt64PK(milvusPKType: String): Boolean = {
    milvusPKType.toLowerCase() == "int64"
  }
}

case class MilvusS3Option(
    readerType: String,
    s3FileSystemType: String,
    s3BucketName: String,
    s3RootPath: String,
    s3Endpoint: String,
    s3AccessKey: String,
    s3SecretKey: String,
    s3UseSSL: Boolean,
    s3PathStyleAccess: Boolean,
    milvusPKType: String,
    s3MaxConnections: Int,
    s3PreloadPoolSize: Int
) extends Serializable {
  def notEmpty(str: String): Boolean = str != null && str.trim.nonEmpty

  def getConf(): Configuration = {
    val conf = new Configuration()
    if (notEmpty(s3FileSystemType)) {
      // Basic S3 configuration
      conf.set("fs.s3a.endpoint", s3Endpoint)
      conf.set("fs.s3a.path.style.access", s3PathStyleAccess.toString)
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )
      conf.set("fs.s3a.access.key", s3AccessKey)
      conf.set("fs.s3a.secret.key", s3SecretKey)
      conf.set("fs.s3a.connection.ssl.enabled", s3UseSSL.toString)

      // Performance optimization settings
      conf.set("fs.s3a.block.size", "134217728") // 128MB
      conf.set("fs.s3a.threads.max", s3MaxConnections.toString)
      conf.set("fs.s3a.threads.core", (s3MaxConnections / 2).toString)
      conf.set("fs.s3a.connection.maximum", (s3MaxConnections + 32).toString)
      conf.set("fs.s3a.connection.timeout", "30000")
      conf.set("fs.s3a.socket.timeout", "30000")
      conf.set("fs.s3a.retry.limit", "3")
    }
    conf
  }

  def getFileSystem(path: Path): FileSystem = {
    if (notEmpty(s3FileSystemType)) {
      val conf = getConf()
      val fileSystem = new S3AFileSystem()
      try {
        fileSystem.initialize(
          new URI(
            s"s3a://${s3BucketName}/"
          ),
          conf
        )
        fileSystem
      } catch {
        case e: Exception =>
          // Close the filesystem if initialization failed
          try {
            fileSystem.close()
          } catch {
            case _: Exception => // Ignore close errors
          }
          throw new RuntimeException(
            s"Failed to initialize S3 FileSystem for bucket $s3BucketName: ${e.getMessage}",
            e
          )
      }
    } else {
      val conf = getConf()
      path.getFileSystem(conf)
    }
  }

  def getFilePath(path: String): Path = {
    if (notEmpty(s3FileSystemType)) {
      if (path.startsWith("s3a://")) {
        new Path(path)
      } else {
        val finalPath = s"s3a://${s3BucketName}/${s3RootPath}/${path}"
        new Path(new URI(finalPath))
      }
    } else {
      new Path(path)
    }
  }
}

object MilvusS3Option {
  def apply(options: CaseInsensitiveStringMap): MilvusS3Option = {
    new MilvusS3Option(
      options.get(Constants.LogReaderTypeParamName),
      options.get(Constants.S3FileSystemTypeName),
      options.getOrDefault(Constants.S3BucketName, "a-bucket"),
      options.getOrDefault(Constants.S3RootPath, "files"),
      options.getOrDefault(Constants.S3Endpoint, "localhost:9000"),
      options.getOrDefault(Constants.S3AccessKey, "minioadmin"),
      options.getOrDefault(Constants.S3SecretKey, "minioadmin"),
      options.getOrDefault(Constants.S3UseSSL, "false").toBoolean,
      options.getOrDefault(Constants.S3PathStyleAccess, "true").toBoolean,
      options.getOrDefault(MilvusOption.MilvusCollectionPKType, ""),
      options.getOrDefault(Constants.S3MaxConnections, "32").toInt,
      options.getOrDefault(Constants.S3PreloadPoolSize, "4").toInt
    )
  }
}
