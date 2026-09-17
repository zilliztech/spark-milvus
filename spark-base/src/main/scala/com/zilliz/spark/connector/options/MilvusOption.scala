package com.zilliz.spark.connector.options

import java.lang.{Float => JavaFloat}
import java.net.URI
import java.util.Locale
import scala.collection.Map

import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.client.api.MilvusConnectionParams
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.expr.{Expr, PlanParser}
import com.zilliz.milvus.storage.read.plan.ReadLimits

/** Vector search configuration for Milvus Storage V2
  */
case class VectorSearch(
    queryVector: Array[Float],
    topK: Int,
    metricType: String,
    vectorColumn: String,
    mode: String = "brute_force",
    searchParameters: Map[String, String] = Map.empty,
    filter: Option[String] = None,
    allowUnindexed: Boolean = false
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
    vectorSearch: Option[VectorSearch] = None,
    readLimits: ReadLimits = ReadLimits.Default,
    writeFileRollingBytes: Long = MilvusOption.DefaultWriteFileRollingBytes,
    milvusFilter: Option[Expr] = None
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
  val ReadBatchMaxRows = "milvus.read.batch.max.rows"
  val ReadBatchMaxBytes = "milvus.read.batch.max.bytes"
  val ReadArrowMaxBytes = "milvus.read.arrow.max.bytes"
  val WriteFileRollingBytes = "milvus.write.file.rolling.bytes"
  val MilvusFilter = "milvus.filter"

  val DefaultWriteFileRollingBytes: Long = 2L * 1024L * 1024L * 1024L
  private[connector] val NativeWriterFileRollingSize =
    "writer.file_rolling.size"

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
  val VectorSearchMode = "vector.search.mode"
  val VectorSearchParameters = "vector.search.parameters"
  val VectorSearchFilter = "vector.search.filter"
  val VectorSearchAllowUnindexed = "vector.search.allowUnindexed"
  val VectorSearchScore = "_score"

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
  ): Boolean = OptionParsing.boolean(getOption, key, defaultValue)

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
    * to agree on UAT (V2 and V3 segments, the three delete states, the
    * all-types collection value by value). Arrays of every element type were
    * compared in unit tests and in the review 749178e UAT run. `false` takes
    * the row path; a read with vector search takes it regardless, because that
    * stage scores rows.
    */
  private def readColumnarFrom(
      getOption: String => Option[String]
  ): Boolean =
    booleanOption(getOption, ReadColumnar, defaultValue = true)

  private def selectedIdsFrom(
      getOption: String => Option[String],
      key: String
  ): Seq[Long] = OptionParsing.nonNegativeLongList(getOption, key)

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
    val getOption = (key: String) => OptionParsing.value(options, key)
    val insertMaxBatchSize = OptionParsing.positiveInt(
      getOption,
      MilvusInsertMaxBatchSize,
      5000
    )
    val retryCount =
      OptionParsing.positiveInt(getOption, MilvusRetryCount, 3)
    val retryInterval =
      OptionParsing.positiveInt(getOption, MilvusRetryInterval, 1000)
    val fieldIDs = options.getOrDefault(ReaderFieldIDs, "")
    val extraColumns = MilvusOption.extraColumns(options)
    val readLimits = ReadLimits(
      OptionParsing.positiveInt(
        getOption,
        ReadBatchMaxRows,
        ReadLimits.DefaultBatchMaxRows
      ),
      OptionParsing.positiveLong(
        getOption,
        ReadBatchMaxBytes,
        ReadLimits.DefaultBatchMaxBytes,
        ReadLimits.MaxBatchBytes
      ),
      OptionParsing.positiveLong(
        getOption,
        ReadArrowMaxBytes,
        ReadLimits.DefaultArrowMaxBytes
      )
    )
    val writeFileRollingBytes = OptionParsing.positiveLong(
      getOption,
      WriteFileRollingBytes,
      DefaultWriteFileRollingBytes
    )

    // Convert CaseInsensitiveStringMap to regular Map for storage
    import scala.collection.JavaConverters._
    val optionsMap = options.asScala.toMap

    val milvusFilter = parseMilvusFilter(options)

    // Parse vector search configuration
    val vectorSearch = parseVectorSearch(options)

    MilvusOption(
      uri = uri,
      token = token,
      serverPemPath = serverPemPath,
      clientKeyPath = clientKeyPath,
      clientPemPath = clientPemPath,
      caPemPath = caPemPath,
      databaseName = databaseName,
      collectionName = collectionName,
      partitionName = partitionName,
      collectionPKType = collectionPKType,
      insertMaxBatchSize = insertMaxBatchSize,
      retryCount = retryCount,
      retryInterval = retryInterval,
      collectionID = collectionID,
      partitionID = partitionID,
      segmentID = segmentID,
      fieldID = fieldID,
      fieldIDs = fieldIDs,
      extraColumns = extraColumns,
      options = optionsMap,
      vectorSearch = vectorSearch,
      readLimits = readLimits,
      writeFileRollingBytes = writeFileRollingBytes,
      milvusFilter = milvusFilter
    )
  }

  private def parseMilvusFilter(
      options: CaseInsensitiveStringMap
  ): Option[Expr] = {
    if (!options.containsKey(MilvusFilter)) return None

    import scala.collection.JavaConverters._
    val vectorSearchOption = options
      .keySet()
      .asScala
      .find(_.toLowerCase(Locale.ROOT).startsWith("vector.search."))
    vectorSearchOption.foreach { key =>
      throw new IllegalArgumentException(
        s"Options '$MilvusFilter' and '$key' cannot be combined; use " +
          s"'$VectorSearchFilter' for vector search"
      )
    }

    val text = Option(options.get(MilvusFilter)).map(_.trim).getOrElse("")
    if (text.isEmpty) {
      throw new IllegalArgumentException(
        s"Option '$MilvusFilter' must not be empty"
      )
    }

    try Some(PlanParser.parse(text))
    catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Option '$MilvusFilter' is invalid: ${e.getMessage}",
          e
        )
    }
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

    val searchKeys = Seq(
      VectorSearchQueryVector,
      VectorSearchTopK,
      VectorSearchMetric,
      VectorSearchVectorColumn,
      VectorSearchMode,
      VectorSearchParameters,
      VectorSearchFilter,
      VectorSearchAllowUnindexed
    )
    if (!searchKeys.exists(options.containsKey)) return None
    val queryVectorStr = value(VectorSearchQueryVector)
    val topKStr = value(VectorSearchTopK)
    if (queryVectorStr.isEmpty || topKStr.isEmpty) {
      throw new IllegalArgumentException(
        s"Options '$VectorSearchQueryVector' and '$VectorSearchTopK' must be set together"
      )
    }
    val queryVector = parseQueryVector(queryVectorStr.get)
    val topK = OptionParsing.positiveInt(
      key => if (key == VectorSearchTopK) topKStr else None,
      VectorSearchTopK,
      1
    )
    val metricType = value(VectorSearchMetric)
      .getOrElse("L2")
      .toUpperCase(Locale.ROOT)
    val vectorColumn = value(VectorSearchVectorColumn).getOrElse("vector")
    require(
      Set("L2", "IP", "COSINE").contains(metricType),
      s"Option '$VectorSearchMetric' must be one of L2, IP or COSINE, got '$metricType'"
    )
    val mode = value(VectorSearchMode).getOrElse("brute_force")
    require(
      Set("index", "brute_force").contains(mode),
      s"Unknown '$VectorSearchMode': '$mode'"
    )
    val filter =
      Option(options.get(VectorSearchFilter)).map(_.trim).filter(_.nonEmpty)
    filter.foreach(PlanParser.parse)
    val parameters = value(VectorSearchParameters)
      .map { json =>
        import scala.jdk.CollectionConverters._
        val node =
          try new ObjectMapper().enable(FAIL_ON_TRAILING_TOKENS).readTree(json)
          catch {
            case e: Exception =>
              throw new IllegalArgumentException(
                s"Option '$VectorSearchParameters' must be a JSON object",
                e
              )
          }
        require(
          node != null && node.isObject,
          s"Option '$VectorSearchParameters' must be a JSON object"
        )
        node
          .fields()
          .asScala
          .map { e =>
            require(
              e.getValue.isValueNode && !e.getValue.isNull,
              s"Option '$VectorSearchParameters' must contain scalar values"
            )
            e.getKey -> e.getValue.asText()
          }
          .toMap
      }
      .getOrElse(Map.empty[String, String])
    val allowUnindexed = OptionParsing.boolean(
      key => Option(options.get(key)),
      VectorSearchAllowUnindexed,
      defaultValue = false
    )
    require(
      mode == "index" || (filter.isEmpty && parameters.isEmpty && !allowUnindexed),
      "Filter, search parameters and unindexed fallback require vector.search.mode=index"
    )
    Some(
      VectorSearch(
        queryVector,
        topK,
        metricType,
        vectorColumn,
        mode,
        parameters,
        filter,
        allowUnindexed
      )
    )
  }

  /** Parse the nonempty JSON numeric array used as the search vector. */
  private def parseQueryVector(jsonStr: String): Array[Float] = {
    val node =
      try new ObjectMapper().enable(FAIL_ON_TRAILING_TOKENS).readTree(jsonStr)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"Option '$VectorSearchQueryVector' must be a JSON numeric array",
            e
          )
      }
    require(
      node != null && node.isArray && node.size() > 0,
      s"Option '$VectorSearchQueryVector' must be a nonempty JSON array"
    )
    Array.tabulate(node.size()) { index =>
      val value = node.get(index)
      require(
        value.isNumber,
        s"Option '$VectorSearchQueryVector' elements must be numbers"
      )
      val parsed = value.floatValue()
      require(
        JavaFloat.isFinite(parsed),
        s"Option '$VectorSearchQueryVector' contains a non-finite value"
      )
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

  /** Native writer properties derived from validated connector options. */
  private[connector] def writerProperties(
      option: MilvusOption
  ): scala.collection.immutable.Map[String, String] =
    writerProperties(option, StorageProperties.from(option.options))

  /** Add validated writer limits to the storage properties already resolved on
    * the driver. This keeps storage credentials and endpoint selection
    * identical for the task writer, manifest commit, and job committer.
    */
  private[connector] def writerProperties(
      option: MilvusOption,
      storage: scala.collection.immutable.Map[String, String]
  ): scala.collection.immutable.Map[String, String] =
    storage +
      (NativeWriterFileRollingSize -> option.writeFileRollingBytes.toString)
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
    val getOption = (key: String) => OptionParsing.value(options, key)
    new MilvusS3Option(
      options.get(MilvusOption.ReaderType),
      options.get(MilvusOption.S3FileSystemTypeName),
      options.getOrDefault(MilvusOption.S3BucketName, "a-bucket"),
      options.getOrDefault(MilvusOption.S3RootPath, "files"),
      options.getOrDefault(MilvusOption.S3Endpoint, "localhost:9000"),
      options.getOrDefault(MilvusOption.S3AccessKey, "minioadmin"),
      options.getOrDefault(MilvusOption.S3SecretKey, "minioadmin"),
      OptionParsing.boolean(
        getOption,
        MilvusOption.S3UseSSL,
        defaultValue = false
      ),
      OptionParsing.boolean(
        getOption,
        MilvusOption.S3PathStyleAccess,
        defaultValue = true
      ),
      options.getOrDefault(MilvusOption.MilvusCollectionPKType, ""),
      OptionParsing.positiveInt(
        getOption,
        MilvusOption.S3MaxConnections,
        32
      ),
      OptionParsing.positiveInt(
        getOption,
        MilvusOption.S3PreloadPoolSize,
        4
      )
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
