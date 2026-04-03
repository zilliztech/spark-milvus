package com.zilliz.spark.connector

import java.net.URI
import scala.collection.Map

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.MilvusConnectionException

/**
 * Vector search configuration for Milvus Storage V2
 */
case class VectorSearchConfig(
    queryVector: Array[Float],
    topK: Int,
    metricType: String,
    vectorColumn: String
)

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
    extraColumns: Seq[String] = Seq.empty,
    options: Map[String, String] = Map.empty,
    vectorSearchConfig: Option[VectorSearchConfig] = None
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
  val MilvusExtraColumnSegmentID = "segment_id"
  val MilvusExtraColumnRowOffset = "row_offset"

  // reader config
  val ReaderPath = "path"
  val ReaderType = "type"
  val ReaderFieldIDs = "fieldIDs"

  // vector search config
  val VectorSearchQueryVector = "vector.search.query"
  val VectorSearchTopK = "vector.search.topK"
  val VectorSearchMetric = "vector.search.metric"
  val VectorSearchVectorColumn = "vector.search.column"
  val VectorSearchIdColumn = "vector.search.idColumn"

  // s3 config
  val S3FileSystemTypeName = "s3.fs"
  val S3Endpoint = "s3.endpoint"
  val S3BucketName = "s3.bucketName"
  val S3RootPath = "s3.rootPath"
  val S3AccessKey = "s3.accessKey"
  val S3SecretKey = "s3.secretKey"
  val S3UseSSL = "s3.useSSL"
  val S3PathStyleAccess = "s3.pathStyleAccess"
  val S3MaxConnections = "s3.maxConnections"
  val S3PreloadPoolSize = "s3.preloadPoolSize"

  // FFI (Storage V2) filesystem property keys
  val FsAddress = "fs.address"
  val FsBucketName = "fs.bucketName"
  val FsAccessKeyId = "fs.accessKeyId"
  val FsAccessKeyValue = "fs.accessKeyValue"
  val FsRootPath = "fs.rootPath"
  val FsStorageType = "fs.storageType"
  val FsCloudProvider = "fs.cloudProvider"
  val FsIamEndpoint = "fs.iamEndpoint"
  val FsLogLevel = "fs.logLevel"
  val FsRegion = "fs.region"
  val FsUseSSL = "fs.useSSL"
  val FsSslCaCert = "fs.sslCaCert"
  val FsUseIam = "fs.useIam"
  val FsUseVirtualHost = "fs.useVirtualHost"
  val FsRequestTimeoutMs = "fs.requestTimeoutMs"
  val FsGcpNativeWithoutAuth = "fs.gcpNativeWithoutAuth"
  val FsGcpCredentialJson = "fs.gcpCredentialJson"
  val FsUseCustomPartUpload = "fs.useCustomPartUpload"

  // Writer config
  val WriterCustomPath = "milvus.writer.customPath"
  val WriterCommitType = "milvus.writer.commitType"  // "addfield" for backfill, default "addfiles"
  val WriterFieldIds = "milvus.writer.fieldIds"      // JSON map of field name -> field ID (e.g., "new_field:104,other_field:105")

  // Snapshot-based reading options (for offline/client-free mode)
  val SnapshotMode = "milvus.snapshot.mode"                 // "true" to enable snapshot mode
  val SnapshotManifests = "milvus.snapshot.manifests"       // JSON array of StorageV2ManifestItem
  val SnapshotCollectionId = "milvus.snapshot.collection.id"
  val SnapshotPartitionIds = "milvus.snapshot.partition.ids"
  val SnapshotSchemaJson = "milvus.snapshot.schema.json"    // Optional: raw schema JSON for building MilvusCollectionInfo
  val SnapshotSchemaBytes = "milvus.snapshot.schema.bytes"  // Base64 encoded protobuf CollectionSchema bytes

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

    // Convert CaseInsensitiveStringMap to regular Map for storage
    import scala.collection.JavaConverters._
    val optionsMap = options.asScala.toMap

    // Parse vector search configuration
    val vectorSearchConfig = parseVectorSearchConfig(options)

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
      extraColumns,
      optionsMap,
      vectorSearchConfig
    )
  }

  /**
   * Parse vector search configuration from options
   */
  private def parseVectorSearchConfig(
      options: CaseInsensitiveStringMap
  ): Option[VectorSearchConfig] = {
    val queryVectorStr = Option(options.get(VectorSearchQueryVector))
    val topKStr = Option(options.get(VectorSearchTopK))

    if (queryVectorStr.isEmpty || topKStr.isEmpty) {
      return None
    }

    try {
      val queryVector = parseQueryVector(queryVectorStr.get)
      val topK = topKStr.get.toInt
      val metricType = Option(options.get(VectorSearchMetric))
        .getOrElse("L2")
        .toUpperCase
      val vectorColumn = Option(options.get(VectorSearchVectorColumn))
        .getOrElse("vector")

      Some(VectorSearchConfig(queryVector, topK, metricType, vectorColumn))
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Parse query vector from JSON string format
   * Expected format: "[0.1, 0.2, 0.3, ...]"
   */
  private def parseQueryVector(jsonStr: String): Array[Float] = {
    jsonStr.trim
      .stripPrefix("[")
      .stripSuffix("]")
      .split(",")
      .map(_.trim.toFloat)
  }

  def isInt64PK(milvusPKType: String): Boolean = {
    milvusPKType.toLowerCase() == "int64"
  }

  /**
   * Generate vector dimension configuration key for a given field name
   * Format: vector.{fieldName}.dim
   */
  def vectorDimKey(fieldName: String): String = s"vector.$fieldName.dim"

  /**
   * Helper method to convert Map to CaseInsensitiveStringMap and create MilvusOption
   */
  def apply(options: Map[String, String]): MilvusOption = {
    import scala.collection.JavaConverters._
    apply(new CaseInsensitiveStringMap(options.asJava))
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
      options.get(MilvusOption.ReaderType),
      options.get(MilvusOption.S3FileSystemTypeName),
      options.getOrDefault(MilvusOption.S3BucketName, "a-bucket"),
      options.getOrDefault(MilvusOption.S3RootPath, "files"),
      options.getOrDefault(MilvusOption.S3Endpoint, "localhost:9000"),
      options.getOrDefault(MilvusOption.S3AccessKey, "minioadmin"),
      options.getOrDefault(MilvusOption.S3SecretKey, "minioadmin"),
      options.getOrDefault(MilvusOption.S3UseSSL, "false").toBoolean,
      options.getOrDefault(MilvusOption.S3PathStyleAccess, "true").toBoolean,
      options.getOrDefault(MilvusOption.MilvusCollectionPKType, ""),
      options.getOrDefault(MilvusOption.S3MaxConnections, "32").toInt,
      options.getOrDefault(MilvusOption.S3PreloadPoolSize, "4").toInt
    )
  }
}
