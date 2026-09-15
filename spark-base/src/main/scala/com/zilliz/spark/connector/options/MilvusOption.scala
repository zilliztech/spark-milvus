package com.zilliz.spark.connector.options

import java.net.URI
import scala.collection.Map

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.client.api.MilvusConnectionParams

/** Vector search configuration for Milvus Storage V2
  */
case class VectorSearch(
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
    vectorSearch: Option[VectorSearch] = None
) {

  /** Just the fields needed to connect to Milvus. The client does not know
    * about MilvusOption: options are layer 3, the client module is layer 2, and
    * dependencies only point downward.
    */
  def connectionParams: MilvusConnectionParams =
    MilvusConnectionParams(
      uri,
      token,
      databaseName,
      serverPemPath,
      clientPemPath,
      clientKeyPath,
      caPemPath
    )
}

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
  val MilvusExtraColumnSegmentID = "$segment_id"
  val MilvusExtraColumnRowOffset = "$row_offset"
  private[connector] val MilvusExtraColumnSegmentIDAlias = "segment_id"
  private[connector] val MilvusExtraColumnRowOffsetAlias = "row_offset"

  private[connector] def normalizeExtraColumnName(name: String): String =
    name match {
      case MilvusExtraColumnSegmentIDAlias => MilvusExtraColumnSegmentID
      case MilvusExtraColumnRowOffsetAlias => MilvusExtraColumnRowOffset
      case other                           => other
    }

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
  val WriterCommitType =
    "milvus.writer.commitType" // "addfield" for backfill, default "addfiles"
  val WriterFieldIds =
    "milvus.writer.fieldIds" // JSON map of field name -> field ID (e.g., "new_field:104,other_field:105")
  val WriterVariableWidthBytesPerValue =
    "milvus.writer.variableWidthBytesPerValue"

  // Backfill merge mode.
  //   replace   — file is source of truth for target columns: matched rows
  //               take file values (null included); unmatched source rows get
  //               null target columns.
  //   coalesce  — fill-if-null: matched rows pick coalesce(src, file) per
  //               field (source wins when non-null); unmatched source rows
  //               keep their original values.
  //   overwrite — file overrides matched rows only (null included); unmatched
  //               source rows keep their original values.
  val BackfillMode = "milvus.backfill.mode"
  val BackfillModeReplace = "replace"
  val BackfillModeCoalesce = "coalesce"
  val BackfillModeOverwrite = "overwrite"

  // Snapshot-based reading options (for offline/client-free mode)
  // Backup-based reading options (offline/client-free). `milvus.backup.dir`
  // points at a binlog-format backup produced by milvus-backup (the directory
  // whose `meta/full_meta.json` holds the collection schema + segment layout).
  val BackupDir = "milvus.backup.dir"

  val SnapshotMode = "milvus.snapshot.mode" // "true" to enable snapshot mode
  // JSON array of ManifestItemJson. Despite the "V2" in the class name,
  // these are StorageV3 loon manifests (segment-info storage_version = 3).
  // The class name + JSON wire key are historical: milvus-storage's library
  // calls its own manifest format "format v2", which collides with the
  // server's segment-info enum where V2 means non-manifest packed parquet.
  val SnapshotManifests = "milvus.snapshot.manifests"
  // JSON array of com.zilliz.milvus.storage.snapshot.V2SegmentInfo — true
  // StorageV2 (segment-info storage_version = 2, non-manifest packed parquet).
  // Populated by backfill after decoding the per-segment AVROs + parquet
  // footers; consumed by MilvusDataSource's snapshot planner to create
  // MilvusV2InputPartition instances.
  val SnapshotV2Segments = "milvus.snapshot.v2.segments"

  /** A snapshot JSON in the snapshot directory, as an `s3a://` URI or a key
    * relative to `fs.bucket_name`. Snapshot mode without a Milvus service: the
    * schema, the partitions and the segments all come from that file.
    */
  val SnapshotPath = "milvus.snapshot.path"
  val SnapshotCollectionId = "milvus.snapshot.collection.id"
  val SnapshotPartitionIds = "milvus.snapshot.partition.ids"
  val SnapshotSchemaJson =
    "milvus.snapshot.schema.json" // Optional: raw schema JSON for building MilvusCollectionInfo
  val SnapshotSchemaBytes =
    "milvus.snapshot.schema.bytes" // Base64 encoded protobuf CollectionSchema bytes
  val SnapshotMaxJsonBytes = "milvus.snapshot.max.json.bytes"
  val ReadApplyDeletes = "milvus.read.apply.deletes"
  val ReadVectorRaw = "milvus.read.vector.raw"
  val ReadColumnar = "milvus.read.columnar"

  /** Client mode: read the snapshot of this name from the snapshot directory
    * instead of the latest one.
    */
  val ClientSnapshotName = "milvus.client.snapshot.name"

  private def nonEmptyOption(
      getOption: String => Option[String],
      key: String
  ): Boolean =
    getOption(key).exists(_.trim.nonEmpty)

  private def isSnapshotModeFrom(
      getOption: String => Option[String]
  ): Boolean = {
    getOption(SnapshotMode)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.equalsIgnoreCase("true"))
      .getOrElse {
        // Only a non-empty hint enables snapshot mode: config templates that
        // keep optional keys with empty values (e.g. milvus.snapshot.manifests="")
        // must not trip the snapshot/backup mutual-exclusion check.
        nonEmptyOption(getOption, SnapshotManifests) ||
        nonEmptyOption(getOption, SnapshotV2Segments) ||
        nonEmptyOption(getOption, SnapshotPath)
      }
  }

  private def validateSnapshotModeOptionsFrom(
      getOption: String => Option[String]
  ): Unit = {
    val explicitSnapshotMode = getOption(SnapshotMode)
      .exists(_.trim.equalsIgnoreCase("true"))
    val hasSnapshotLists = nonEmptyOption(getOption, SnapshotManifests) ||
      nonEmptyOption(getOption, SnapshotV2Segments)
    val hasSnapshotPath = nonEmptyOption(getOption, SnapshotPath)
    if (explicitSnapshotMode && !hasSnapshotLists && !hasSnapshotPath) {
      throw new IllegalArgumentException(
        s"$SnapshotMode=true requires $SnapshotPath, $SnapshotManifests or $SnapshotV2Segments"
      )
    }
    if (hasSnapshotPath && hasSnapshotLists) {
      throw new IllegalArgumentException(
        s"$SnapshotPath and $SnapshotManifests / $SnapshotV2Segments are two sources for the same read; give one"
      )
    }
  }

  def isSnapshotMode(options: Map[String, String]): Boolean = {
    isSnapshotModeFrom { key =>
      options.collectFirst {
        case (optionKey, value) if optionKey.equalsIgnoreCase(key) =>
          value
      }
    }
  }

  def isSnapshotMode(options: CaseInsensitiveStringMap): Boolean = {
    isSnapshotModeFrom(key => Option(options.get(key)))
  }

  def validateSnapshotModeOptions(options: Map[String, String]): Unit = {
    validateSnapshotModeOptionsFrom { key =>
      options.collectFirst {
        case (optionKey, value) if optionKey.equalsIgnoreCase(key) =>
          value
      }
    }
  }

  def validateSnapshotModeOptions(options: CaseInsensitiveStringMap): Unit = {
    validateSnapshotModeOptionsFrom(key => Option(options.get(key)))
  }

  private def backupDirFrom(
      getOption: String => Option[String]
  ): Option[String] =
    getOption(BackupDir)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(normalizeBackupScheme)

  /** Normalize the `s3://` alias to `s3a://` at entry so Hadoop (which drops
    * the `s3://` provider) and the connector's path reconstruction agree.
    */
  private def normalizeBackupScheme(dir: String): String =
    if (dir.startsWith("s3://")) "s3a://" + dir.stripPrefix("s3://") else dir

  def backupDir(options: Map[String, String]): Option[String] = {
    backupDirFrom { key =>
      options.collectFirst {
        case (optionKey, value) if optionKey.equalsIgnoreCase(key) =>
          value
      }
    }
  }

  def backupDir(options: CaseInsensitiveStringMap): Option[String] = {
    backupDirFrom(key => Option(options.get(key)))
  }

  def isBackupMode(options: Map[String, String]): Boolean =
    backupDir(options).isDefined

  def isBackupMode(options: CaseInsensitiveStringMap): Boolean =
    backupDir(options).isDefined

  /** Where the read takes its segment list from, decided once from the options.
    * Snapshot options win over `milvus.backup.dir`; the two together are
    * rejected by `validateBackupModeOptions`.
    */
  def readMode(options: CaseInsensitiveStringMap): ReadMode =
    if (isSnapshotMode(options)) ReadMode.Snapshot
    else if (isBackupMode(options)) ReadMode.Backup
    else ReadMode.Client

  def readMode(options: Map[String, String]): ReadMode =
    if (isSnapshotMode(options)) ReadMode.Snapshot
    else if (isBackupMode(options)) ReadMode.Backup
    else ReadMode.Client

  /** Backup mode reads a milvus-backup binlog-format export offline. It is
    * mutually exclusive with snapshot mode: pick one source of truth for the
    * segment layout.
    */
  def validateBackupModeOptions(options: Map[String, String]): Unit = {
    if (isBackupMode(options) && isSnapshotMode(options)) {
      throw new IllegalArgumentException(
        s"$BackupDir and snapshot mode ($SnapshotMode/$SnapshotManifests/$SnapshotV2Segments) are mutually exclusive"
      )
    }
  }

  def validateBackupModeOptions(options: CaseInsensitiveStringMap): Unit = {
    if (isBackupMode(options) && isSnapshotMode(options)) {
      throw new IllegalArgumentException(
        s"$BackupDir and snapshot mode ($SnapshotMode/$SnapshotManifests/$SnapshotV2Segments) are mutually exclusive"
      )
    }
  }

  private def readApplyDeletesFrom(
      getOption: String => Option[String]
  ): Boolean = {
    getOption(ReadApplyDeletes)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.equalsIgnoreCase("true"))
      .getOrElse(true)
  }

  def readApplyDeletes(options: Map[String, String]): Boolean = {
    readApplyDeletesFrom { key =>
      options.collectFirst {
        case (optionKey, value) if optionKey.equalsIgnoreCase(key) =>
          value
      }
    }
  }

  def readApplyDeletes(options: CaseInsensitiveStringMap): Boolean = {
    readApplyDeletesFrom(key => Option(options.get(key)))
  }

  /** Whether vector columns come out as the bytes Milvus stored.
    *
    * Default false: vectors are decoded into Spark's own types (decision 6).
    * True hands the stored bytes over as BinaryType, which is what a job that
    * feeds them straight to a native library wants — converting to Array[Float]
    * and back again is pure waste there.
    */
  private def readVectorRawFrom(
      getOption: String => Option[String]
  ): Boolean = {
    getOption(ReadVectorRaw)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.equalsIgnoreCase("true"))
      .getOrElse(false)
  }

  def readVectorRaw(options: Map[String, String]): Boolean = {
    readVectorRawFrom { key =>
      options.collectFirst {
        case (optionKey, value) if optionKey.equalsIgnoreCase(key) => value
      }
    }
  }

  def readVectorRaw(options: CaseInsensitiveStringMap): Boolean = {
    readVectorRawFrom(key => Option(options.get(key)))
  }

  /** Whether the scan hands Spark whole batches instead of rows.
    *
    * Default false. The row path is what every existing job runs, and the two
    * have to be shown to agree on real data before the default moves.
    */
  private def readColumnarFrom(
      getOption: String => Option[String]
  ): Boolean = {
    getOption(ReadColumnar)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.equalsIgnoreCase("true"))
      .getOrElse(false)
  }

  def readColumnar(options: Map[String, String]): Boolean = {
    readColumnarFrom { key =>
      options.collectFirst {
        case (optionKey, value) if optionKey.equalsIgnoreCase(key) => value
      }
    }
  }

  def readColumnar(options: CaseInsensitiveStringMap): Boolean = {
    readColumnarFrom(key => Option(options.get(key)))
  }

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
      .map(normalizeExtraColumnName)
      .toSeq

    // Convert CaseInsensitiveStringMap to regular Map for storage
    import scala.collection.JavaConverters._
    val optionsMap = options.asScala.toMap

    // Parse vector search configuration
    val vectorSearch = parseVectorSearch(options)

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
      vectorSearch
    )
  }

  /** Parse vector search configuration from options
    */
  private def parseVectorSearch(
      options: CaseInsensitiveStringMap
  ): Option[VectorSearch] = {
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

      Some(VectorSearch(queryVector, topK, metricType, vectorColumn))
    } catch {
      case _: Exception => None
    }
  }

  /** Parse query vector from JSON string format Expected format: "[0.1, 0.2,
    * 0.3, ...]"
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

  /** Generate vector dimension configuration key for a given field name Format:
    * vector.{fieldName}.dim
    */
  def vectorDimKey(fieldName: String): String = s"vector.$fieldName.dim"

  /** Helper method to convert Map to CaseInsensitiveStringMap and create
    * MilvusOption
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

/** The three sources a read can take its segment list from. */
sealed trait ReadMode

object ReadMode {

  /** The `milvus.snapshot.*` options carry the manifest and segment lists. */
  case object Snapshot extends ReadMode

  /** `milvus.backup.dir` points at a milvus-backup export. */
  case object Backup extends ReadMode

  /** A live Milvus service: the client snapshot fast path, or the legacy
    * segment listing when a selector rules the fast path out.
    */
  case object Client extends ReadMode
}
