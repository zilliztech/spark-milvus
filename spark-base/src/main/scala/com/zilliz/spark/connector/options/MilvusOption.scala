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
  val MilvusPartitions = "milvus.partitions"
  val MilvusSegments = "milvus.segments"
  val MilvusFieldID = "milvus.field.id"
  val MilvusInsertMaxBatchSize = "milvus.insertMaxBatchSize"
  val MilvusRetryCount = "milvus.retry.count"
  val MilvusRetryInterval = "milvus.retry.interval"

  val MilvusExtraColumns = "milvus.extra.columns"
  // Kept as a source-compatibility constant. The partition name is not part of
  // the metadata-column contract and is rejected by extraColumnsFrom.
  val MilvusExtraColumnPartition = "partition"
  val MilvusExtraColumnSegmentID = "_segment_id"
  val MilvusExtraColumnRowOffset = "_row_offset"
  val MilvusExtraColumnTimestamp = "_timestamp"
  private[connector] val MilvusExtraColumnSegmentIDAlias = "$segment_id"
  private[connector] val MilvusExtraColumnRowOffsetAlias = "$row_offset"
  private val MilvusExtraColumnSegmentIDBareAlias = "segment_id"
  private val MilvusExtraColumnRowOffsetBareAlias = "row_offset"

  private val SupportedExtraColumns = Set(
    MilvusExtraColumnSegmentID,
    MilvusExtraColumnRowOffset,
    MilvusExtraColumnTimestamp
  )

  private[connector] def normalizeExtraColumnName(name: String): String =
    name match {
      case MilvusExtraColumnSegmentIDAlias |
          MilvusExtraColumnSegmentIDBareAlias =>
        MilvusExtraColumnSegmentID
      case MilvusExtraColumnRowOffsetAlias |
          MilvusExtraColumnRowOffsetBareAlias =>
        MilvusExtraColumnRowOffset
      case other => other
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
  // JSON array of V2 segments (SegmentListJson) — true
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

  private def booleanOption(
      getOption: String => Option[String],
      key: String,
      defaultValue: => Boolean
  ): Boolean =
    getOption(key).map(_.trim) match {
      case None                                       => defaultValue
      case Some(raw) if raw.equalsIgnoreCase("true")  => true
      case Some(raw) if raw.equalsIgnoreCase("false") => false
      case Some(raw) =>
        throw new IllegalArgumentException(
          s"Option '$key' must be 'true' or 'false', got '$raw'"
        )
    }

  private def isSnapshotModeFrom(
      getOption: String => Option[String]
  ): Boolean =
    booleanOption(
      getOption,
      SnapshotMode,
      defaultValue = {
        // Only a non-empty hint enables snapshot mode: config templates that
        // keep optional keys with empty values (e.g. milvus.snapshot.manifests="")
        // must not trip the snapshot/backup mutual-exclusion check.
        nonEmptyOption(getOption, SnapshotManifests) ||
        nonEmptyOption(getOption, SnapshotV2Segments) ||
        nonEmptyOption(getOption, SnapshotPath)
      }
    )

  private def validateSnapshotModeOptionsFrom(
      getOption: String => Option[String]
  ): Unit = {
    val explicitSnapshotMode = booleanOption(
      getOption,
      SnapshotMode,
      defaultValue = false
    )
    val hasSnapshotLists = nonEmptyOption(getOption, SnapshotManifests) ||
      nonEmptyOption(getOption, SnapshotV2Segments)
    val hasSnapshotPath = nonEmptyOption(getOption, SnapshotPath)
    // A schema alone is a snapshot with no segments: nothing to read, but
    // everything a write needs.
    val hasSchema = nonEmptyOption(getOption, SnapshotSchemaBytes) ||
      nonEmptyOption(getOption, SnapshotSchemaJson)
    if (
      explicitSnapshotMode && !hasSnapshotLists && !hasSnapshotPath && !hasSchema
    ) {
      throw new IllegalArgumentException(
        s"$SnapshotMode=true requires $SnapshotPath, $SnapshotManifests, $SnapshotV2Segments or $SnapshotSchemaBytes"
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
  ): Boolean =
    booleanOption(getOption, ReadApplyDeletes, defaultValue = true)

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
  ): Boolean =
    booleanOption(getOption, ReadVectorRaw, defaultValue = false)

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
    * Default true since 2026-09-16: the columnar outlet is what the 2.0 read
    * path exists for (P0 in the design README), and the two outlets were shown
    * to agree on UAT (V2 and V3 segments, the three delete states, every
    * supported type, value by value). `false` takes the row path; a read with
    * vector search takes it regardless, because that stage scores rows.
    */
  private def readColumnarFrom(
      getOption: String => Option[String]
  ): Boolean =
    booleanOption(getOption, ReadColumnar, defaultValue = true)

  private def selectedIdsFrom(
      getOption: String => Option[String],
      key: String
  ): Seq[Long] =
    getOption(key).map(_.trim) match {
      case None => Seq.empty
      case Some(raw) =>
        val values = raw.split(",", -1).toSeq.map(_.trim)
        if (values.exists(_.isEmpty)) {
          throw new IllegalArgumentException(
            s"Option '$key' must be a comma-separated list of numeric ids without empty entries, got '$raw'"
          )
        }
        values.map { value =>
          val id =
            try value.toLong
            catch {
              case _: NumberFormatException =>
                throw new IllegalArgumentException(
                  s"Option '$key' must contain numeric ids, got '$value' in '$raw'"
                )
            }
          if (id < 0L) {
            throw new IllegalArgumentException(
              s"Option '$key' must contain non-negative ids, got '$value' in '$raw'"
            )
          }
          id
        }.distinct
    }

  private def extraColumnsFrom(
      getOption: String => Option[String]
  ): Seq[String] = {
    val raw = getOption(MilvusExtraColumns).map(_.trim).getOrElse("")
    if (raw.isEmpty) return Seq.empty
    val values = raw.split(",", -1).toSeq.map(_.trim)
    if (values.exists(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"Option '$MilvusExtraColumns' must be a comma-separated list without empty entries, got '$raw'"
      )
    }
    val normalized = values.map(normalizeExtraColumnName)
    val unsupported = normalized.filterNot(SupportedExtraColumns).distinct
    if (unsupported.nonEmpty) {
      throw new IllegalArgumentException(
        s"Option '$MilvusExtraColumns' contains unsupported column(s) ${unsupported
            .mkString(", ")}; " +
          s"supported columns are ${SupportedExtraColumns.toSeq.sorted.mkString(", ")}"
      )
    }
    normalized.distinct
  }

  def extraColumns(options: Map[String, String]): Seq[String] =
    extraColumnsFrom { key =>
      options.collectFirst {
        case (optionKey, value) if optionKey.equalsIgnoreCase(key) => value
      }
    }

  def extraColumns(options: CaseInsensitiveStringMap): Seq[String] =
    extraColumnsFrom(key => Option(options.get(key)))

  def selectedPartitionIds(options: Map[String, String]): Seq[Long] =
    selectedIdsFrom(
      key =>
        options.collectFirst {
          case (optionKey, value) if optionKey.equalsIgnoreCase(key) => value
        },
      MilvusPartitions
    )

  def selectedPartitionIds(
      options: CaseInsensitiveStringMap
  ): Seq[Long] =
    selectedIdsFrom(key => Option(options.get(key)), MilvusPartitions)

  def selectedSegmentIds(options: Map[String, String]): Seq[Long] =
    selectedIdsFrom(
      key =>
        options.collectFirst {
          case (optionKey, value) if optionKey.equalsIgnoreCase(key) => value
        },
      MilvusSegments
    )

  def selectedSegmentIds(options: CaseInsensitiveStringMap): Seq[Long] =
    selectedIdsFrom(key => Option(options.get(key)), MilvusSegments)

  def readerFieldIds(options: Map[String, String]): Seq[Long] =
    selectedIdsFrom(
      key =>
        options.collectFirst {
          case (optionKey, value) if optionKey.equalsIgnoreCase(key) => value
        },
      ReaderFieldIDs
    )

  def readerFieldIds(options: CaseInsensitiveStringMap): Seq[Long] =
    selectedIdsFrom(key => Option(options.get(key)), ReaderFieldIDs)

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
    val extraColumns = MilvusOption.extraColumns(options)

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
    def value(key: String): Option[String] =
      Option(options.get(key)).map { value =>
        val trimmed = value.trim
        if (trimmed.isEmpty) {
          throw new IllegalArgumentException(
            s"Option '$key' must not be empty"
          )
        }
        trimmed
      }

    val queryVectorStr = value(VectorSearchQueryVector)
    val topKStr = value(VectorSearchTopK)
    val metricTypeStr = value(VectorSearchMetric)
    val vectorColumnStr = value(VectorSearchVectorColumn)

    if (
      Seq(queryVectorStr, topKStr, metricTypeStr, vectorColumnStr).forall(
        _.isEmpty
      )
    ) {
      return None
    }
    if (queryVectorStr.isEmpty) {
      throw new IllegalArgumentException(
        s"Options '$VectorSearchQueryVector' and '$VectorSearchTopK' must be set together"
      )
    }
    if (topKStr.isEmpty) {
      throw new IllegalArgumentException(
        s"Options '$VectorSearchQueryVector' and '$VectorSearchTopK' must be set together"
      )
    }

    try {
      val queryVector = parseQueryVector(queryVectorStr.get)
      val topK =
        try topKStr.get.toInt
        catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Option '$VectorSearchTopK' must be a positive integer, got '${topKStr.get}'"
            )
        }
      if (queryVector.isEmpty) {
        throw new IllegalArgumentException(
          s"Option '$VectorSearchQueryVector' must contain at least one number"
        )
      }
      if (topK <= 0) {
        throw new IllegalArgumentException(
          s"Option '$VectorSearchTopK' must be positive, got '$topK'"
        )
      }
      val metricType = metricTypeStr
        .getOrElse("L2")
        .toUpperCase
      if (!Set("L2", "IP", "COSINE").contains(metricType)) {
        throw new IllegalArgumentException(
          s"Option '$VectorSearchMetric' must be one of L2, IP or COSINE, got '$metricType'"
        )
      }
      val vectorColumn = vectorColumnStr.getOrElse("vector")

      Some(VectorSearch(queryVector, topK, metricType, vectorColumn))
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Invalid vector search options: ${e.getMessage}",
          e
        )
    }
  }

  /** Parse query vector from JSON string format Expected format: "[0.1, 0.2,
    * 0.3, ...]"
    */
  private def parseQueryVector(jsonStr: String): Array[Float] = {
    val trimmed = jsonStr.trim
    if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
      throw new IllegalArgumentException(
        s"Option '$VectorSearchQueryVector' must be a JSON-style numeric array, got '$jsonStr'"
      )
    }
    val body = trimmed.substring(1, trimmed.length - 1).trim
    if (body.isEmpty) return Array.empty[Float]
    body
      .split(",", -1)
      .map { value =>
        val number = value.trim
        if (number.isEmpty) {
          throw new IllegalArgumentException(
            s"Option '$VectorSearchQueryVector' contains an empty element in '$jsonStr'"
          )
        }
        val parsed =
          try number.toFloat
          catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(
                s"Option '$VectorSearchQueryVector' contains a non-numeric value '$number'"
              )
          }
        if (!java.lang.Float.isFinite(parsed)) {
          throw new IllegalArgumentException(
            s"Option '$VectorSearchQueryVector' contains a non-finite value '$number'"
          )
        }
        parsed
      }
  }

  def isInt64PK(milvusPKType: String): Boolean = {
    milvusPKType.toLowerCase() == "int64"
  }

  /** Generate vector dimension configuration key for a given field name Format:
    * vector.{fieldName}.dim
    */

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

  /** A live Milvus service names the collection; the snapshot itself comes from
    * the snapshot directory. DataSource loads use `milvus.client.snapshot.name`
    * or latest; Catalog loads select explicitly.
    */
  case object Client extends ReadMode
}
