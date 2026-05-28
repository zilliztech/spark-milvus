package com.zilliz.spark.connector.sources

import java.{util => ju}
import java.net.URI
import java.util.{Base64, HashMap, Map => JMap, UUID}
import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReaderFactory,
  Scan,
  ScanBuilder,
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
// Spark does not expose a public SQL-execution-end hook; validate when upgrading Spark.
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{
  DataTypes => SparkDataTypes,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import com.zilliz.spark.connector.{
  DataTypeUtil,
  MilvusClient,
  MilvusCollectionInfo,
  MilvusOption,
  VectorSearchConfig
}
import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.read.{
  MilvusPackedV2InputPartition,
  MilvusPartitionReaderFactory,
  MilvusSnapshotReader,
  MilvusStorageV3InputPartition,
  SnapshotMetadata,
  StorageV2ManifestItem,
  V2SegmentInfo,
  V2SegmentLoader
}
import com.zilliz.spark.connector.write.{MilvusWrite, MilvusWriteBuilder}
import io.milvus.grpc.schema.CollectionSchema

// 1. DataSourceRegister and TableProvider
case class MilvusDataSource() extends TableProvider with DataSourceRegister {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val milvusOption = MilvusOption(options)
    MilvusOption.validateSnapshotModeOptions(options)
    val isSnapshotMode = MilvusOption.isSnapshotMode(options)
    if (milvusOption.uri.isEmpty && !isSnapshotMode) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.MilvusUri}' is required for reading milvus data."
      )
    }
    MilvusTable(
      milvusOption,
      Some(schema)
    )
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val milvusOption = MilvusOption(options)

    // Check for snapshot mode - use snapshot schema if provided
    MilvusOption.validateSnapshotModeOptions(options)
    val isSnapshotMode = MilvusOption.isSnapshotMode(options)

    if (isSnapshotMode) {
      // Try to get schema from snapshot JSON
      Option(options.get(MilvusOption.SnapshotSchemaJson))
        .flatMap { json =>
          import com.zilliz.spark.connector.read.MilvusSnapshotReader
          MilvusSnapshotReader.parseSnapshotMetadata(json) match {
            case Right(metadata) =>
              Some(
                MilvusSnapshotReader.toSparkSchema(
                  metadata.collection.schema,
                  includeSystemFields = true
                )
              )
            case Left(_) => None
          }
        }
        .getOrElse {
          // If no snapshot schema provided, return empty schema
          // The actual schema should be provided via .schema() call
          StructType(Seq.empty)
        }
    } else {
      // Client-based mode (existing behavior)
      if (milvusOption.collectionName.isEmpty) {
        throw new IllegalArgumentException("collectionName cannot be empty")
      }
      val client = MilvusClient(milvusOption)
      try {
        val result = client.getCollectionSchema(
          milvusOption.databaseName,
          milvusOption.collectionName
        )
        val schema = result.getOrElse(
          throw new Exception(
            s"Failed to get collection schema: ${result.failed.get.getMessage}"
          )
        )
        StructType(
          schema.fields.map(field =>
            StructField(
              field.name,
              DataTypeUtil.toDataType(field),
              field.nullable,
              DataTypeUtil.metadata(field)
            )
          )
        )
      } finally {
        client.close()
      }
    }
  }
  override def supportsExternalMetadata = true

  override def shortName() = "milvus"
}

// 2. Table
case class MilvusTable(
    milvusOption: MilvusOption,
    sparkSchema: Option[StructType]
) extends Table
    with SupportsWrite
    with SupportsRead
    with Logging {
  var milvusCollection: MilvusCollectionInfo = _
  var partitionID: Long = 0L
  initInfo()
  var fieldIDs =
    if (milvusOption.fieldIDs.nonEmpty) {
      milvusOption.fieldIDs.split(",").toSeq
    } else {
      Seq[String]()
    }
  logInfo(s"MilvusTable fieldIDs: $fieldIDs")

  /** Check if snapshot mode is enabled (data comes from snapshot, not client).
    *
    * Either the legacy V3 (manifest-based) hint
    * [[MilvusOption.SnapshotManifests]] or the new V2 (packed parquet) hint
    * [[MilvusOption.SnapshotV2Segments]] is sufficient to take the
    * snapshot-only path and skip all Milvus client calls.
    */
  private def isSnapshotMode: Boolean =
    MilvusOption.isSnapshotMode(milvusOption.options)

  def initInfo(): Unit = {
    // Check for snapshot mode first - skip client calls if snapshot data is provided
    if (isSnapshotMode) {
      logInfo(
        "Snapshot mode enabled - skipping Milvus client connection for collection info"
      )
      initFromSnapshot()
    } else {
      // Client-based mode (existing behavior)
      initFromClient()
    }
  }

  /** Initialize collection info from snapshot metadata (no client connection)
    */
  private def initFromSnapshot(): Unit = {
    import com.zilliz.spark.connector.read.MilvusSnapshotReader

    // Get collection ID from options
    val collectionId = milvusOption.options
      .get(MilvusOption.SnapshotCollectionId)
      .map(_.toLong)
      .getOrElse(0L)

    // Get partition IDs from options
    val partitionIds = milvusOption.options
      .get(MilvusOption.SnapshotPartitionIds)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).map(_.toLong).toSeq)
      .getOrElse(Seq.empty[Long])

    // Use first partition ID if available
    partitionID = partitionIds.headOption.getOrElse(0L)

    // Try to build schema from snapshot JSON if provided
    val schemaJson = milvusOption.options.get(MilvusOption.SnapshotSchemaJson)
    val snapshotSchema = schemaJson.flatMap { json =>
      MilvusSnapshotReader.parseSnapshotMetadata(json) match {
        case Right(metadata) => Some(metadata.collection.schema)
        case Left(_)         => None
      }
    }

    // Create a minimal MilvusCollectionInfo
    // For snapshot mode, we use the passed-in sparkSchema for actual schema operations
    milvusCollection = MilvusCollectionInfo(
      dbName = milvusOption.databaseName,
      collectionName = milvusOption.collectionName,
      collectionID = collectionId,
      schema = createMinimalCollectionSchema(snapshotSchema)
    )

    logInfo(
      s"Initialized from snapshot: collectionID=$collectionId, partitionID=$partitionID"
    )
  }

  /** Create a minimal CollectionSchema for snapshot mode This is used when we
    * have snapshot data but need a protobuf schema structure
    */
  private def createMinimalCollectionSchema(
      snapshotSchema: Option[com.zilliz.spark.connector.read.CollectionSchema]
  ): CollectionSchema = {
    import io.milvus.grpc.schema.{CollectionSchema => ProtoCollectionSchema}

    snapshotSchema match {
      case Some(schema) =>
        ProtoCollectionSchema.parseFrom(
          MilvusSnapshotReader.toProtobufSchemaBytes(schema)
        )

      case None =>
        // If no schema provided, create empty schema
        // The actual schema will come from sparkSchema passed to the table
        ProtoCollectionSchema(
          name = milvusOption.collectionName,
          description = "",
          fields = Seq.empty
        )
    }
  }

  /** Initialize collection info from Milvus client (existing behavior)
    */
  private def initFromClient(): Unit = {
    val client = MilvusClient(milvusOption)
    try {
      milvusCollection = client
        .getCollectionInfo(
          milvusOption.databaseName,
          milvusOption.collectionName
        )
        .getOrElse(
          throw new Exception(
            s"Collection ${milvusOption.collectionName} not found"
          )
        )
      if (milvusOption.partitionName.nonEmpty) {
        partitionID = client
          .getPartitionID(
            milvusOption.databaseName,
            milvusOption.collectionName,
            milvusOption.partitionName
          )
          .getOrElse(
            throw new Exception(
              s"Partition ${milvusOption.partitionName} not found"
            )
          )
      }
    } finally {
      client.close()
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    MilvusWriteBuilder(milvusOption, info)
  }

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    // Merge table properties with scan options. Scan options take precedence.
    val mergedOptions: JMap[String, String] = new HashMap[String, String]()
    mergedOptions.putAll(properties)
    mergedOptions.putAll(options)
    if (mergedOptions.get(MilvusOption.MilvusCollectionID) == null) {
      mergedOptions.put(
        MilvusOption.MilvusCollectionID,
        milvusCollection.collectionID.toString
      )
    }
    if (partitionID != 0L) {
      mergedOptions.put(
        MilvusOption.MilvusPartitionID,
        partitionID.toString
      )
    }

    val allOptions = new CaseInsensitiveStringMap(mergedOptions)
    new MilvusScanBuilder(schema(), allOptions)
  }

  override def name(): String = milvusOption.collectionName

  override def schema(): StructType = {
    // In snapshot mode with provided sparkSchema, use it directly
    // This avoids the need to parse milvusCollection.schema which may be incomplete
    if (isSnapshotMode && sparkSchema.isDefined && sparkSchema.get.nonEmpty) {
      logInfo(
        s"Using provided sparkSchema in snapshot mode: ${sparkSchema.get.fieldNames.mkString(", ")}"
      )
      return sparkSchema.get
    }

    // Client-based mode or snapshot mode without provided schema: compute from milvusCollection
    var fields = Seq[StructField]()
    var fieldName2ID = mutable.Map[String, Long]()
    milvusCollection.schema.fields.zipWithIndex.foreach { case (field, index) =>
      fieldName2ID(field.name) = if (field.fieldID == 0) {
        index + 100
      } else {
        field.fieldID
      }
    }
    if (fieldIDs.isEmpty || fieldIDs.contains("0")) {
      fields = fields :+ StructField("row_id", LongType, false)
    }
    if (fieldIDs.isEmpty || fieldIDs.contains("1")) {
      fields = fields :+ StructField("timestamp", LongType, false)
    }
    val filteredFields = milvusCollection.schema.fields
      .filter(field =>
        fieldIDs.isEmpty || fieldIDs.contains(fieldName2ID(field.name).toString)
      )
    fields = fields ++ filteredFields.map(field =>
      StructField(
        field.name,
        DataTypeUtil.toDataType(field),
        field.nullable,
        DataTypeUtil.metadata(field)
      )
    )
    // Safely get maxFieldID, default to 100 if empty
    val maxFieldID =
      if (fieldName2ID.values.nonEmpty) fieldName2ID.values.max else 100L
    if (
      milvusCollection.schema.enableDynamicField &&
      (fieldIDs.isEmpty || fieldIDs.contains((maxFieldID + 1).toString))
    ) {
      fields = fields :+ StructField("$meta", StringType, true)
    }
    if (
      milvusOption.extraColumns.contains(
        MilvusOption.MilvusExtraColumnPartition
      )
    ) {
      fields = fields :+ StructField("partition", StringType, true)
    }
    if (
      milvusOption.extraColumns.contains(
        MilvusOption.MilvusExtraColumnSegmentID
      )
    ) {
      fields = fields :+ StructField("segment_id", LongType, false)
    }
    if (
      milvusOption.extraColumns.contains(
        MilvusOption.MilvusExtraColumnRowOffset
      )
    ) {
      fields = fields :+ StructField("row_offset", LongType, false)
    }
    StructType(fields)
  }

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_WRITE,
      TableCapability.BATCH_READ
    ).asJava
  }
}

class MilvusScanBuilder(
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with Logging {
  private var currentSchema = schema
  private var currentOptions = options
  private val extraColumns = options
    .getOrDefault(MilvusOption.MilvusExtraColumns, "")
    .split(",")
    .map(_.trim)
    .filter(_.nonEmpty)
    .toSeq

  // Store the filters that can be pushed down
  private var pushedFilterArray: Array[Filter] = Array.empty[Filter]

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (currentOptions.getOrDefault(MilvusOption.ReaderFieldIDs, "").nonEmpty) {
      return
    }
    val fieldName2ID = mutable.Map[String, Long]()
    schema.fields
      .filterNot(f => extraColumns.contains(f.name))
      .zipWithIndex
      .foreach { case (field, index) =>
        if (index < 2) {
          fieldName2ID(field.name) = index
        } else {
          fieldName2ID(field.name) = index + 98
        }
      }
    var fieldNames = Seq[String]()
    requiredSchema.fields.foreach(field => {
      if (fieldName2ID.contains(field.name)) {
        fieldNames = fieldNames :+ field.name
      }
    })

    // Add fields referenced in pushed filters to ensure they are not pruned
    pushedFilterArray.foreach { filter =>
      val filterColumns = extractFilterColumns(filter)
      filterColumns.foreach { colName =>
        if (fieldName2ID.contains(colName) && !fieldNames.contains(colName)) {
          fieldNames = fieldNames :+ colName
        }
      }
    }

    // Add vector column if vector search is enabled
    val vectorColumn = Option(
      options.get(MilvusOption.VectorSearchVectorColumn)
    ).getOrElse("vector")
    val hasVectorSearch = Option(
      options.get(MilvusOption.VectorSearchQueryVector)
    ).isDefined
    if (
      hasVectorSearch && fieldName2ID.contains(vectorColumn) && !fieldNames
        .contains(vectorColumn)
    ) {
      fieldNames = fieldNames :+ vectorColumn
    }

    fieldNames = fieldNames.sortBy(fieldName => fieldName2ID(fieldName))
    logInfo(s"fieldNames after sort: $fieldNames")
    if (fieldNames.isEmpty) {
      fieldNames = fieldNames :+ "row_id"
      logInfo(s"fieldNames after add row_id: $fieldNames")
    }

    val tmpMap = new HashMap[String, String]()
    options.asScala.foreach { case (key, value) =>
      tmpMap.put(key, value)
    }
    // Only set ReaderFieldIDs if fieldNames is not empty
    if (fieldNames.nonEmpty) {
      val readerFieldIDsStr = fieldNames
        .map(fieldName => fieldName2ID(fieldName).toString)
        .mkString(",")
      tmpMap.put(
        MilvusOption.ReaderFieldIDs,
        readerFieldIDsStr
      )
    }
    if (
      extraColumns.contains(MilvusOption.MilvusExtraColumnPartition) &&
      !fieldNames.contains("partition")
    ) {
      fieldNames = fieldNames :+ "partition"
    }
    if (
      extraColumns.contains(MilvusOption.MilvusExtraColumnSegmentID) &&
      !fieldNames.contains("segment_id")
    ) {
      fieldNames = fieldNames :+ "segment_id"
    }
    if (
      extraColumns.contains(MilvusOption.MilvusExtraColumnRowOffset) &&
      !fieldNames.contains("row_offset")
    ) {
      fieldNames = fieldNames :+ "row_offset"
    }

    currentOptions = new CaseInsensitiveStringMap(tmpMap)
    currentSchema = StructType(
      fieldNames
        .map(fieldName => {
          schema.fields.find(field => field.name == fieldName).get
        })
        .toSeq
    )
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // V2 packed reader does not apply filters server-side yet — return all
    // as unsupported so Spark applies them post-read.
    // TODO: implement filter pushdown for V2 packed reader in a separate PR.
    val isPackedV2 = Option(options.get(MilvusOption.SnapshotV2Segments))
      .exists(_.nonEmpty)
    if (isPackedV2) {
      pushedFilterArray = Array.empty
      return filters
    }
    val (supportedFilters, unsupportedFilters) =
      filters.partition(isSupportedFilter)
    pushedFilterArray = supportedFilters
    unsupportedFilters
  }

  override def pushedFilters(): Array[Filter] = pushedFilterArray

  private def isSupportedFilter(filter: Filter): Boolean = {
    import org.apache.spark.sql.sources._
    filter match {
      // Support equality filters on string and numeric columns
      case EqualTo(attr, _) => isStringOrNumericColumn(attr)
      // Support numeric comparison filters only on numeric columns
      case GreaterThan(attr, _)        => isNumericColumn(attr)
      case GreaterThanOrEqual(attr, _) => isNumericColumn(attr)
      case LessThan(attr, _)           => isNumericColumn(attr)
      case LessThanOrEqual(attr, _)    => isNumericColumn(attr)
      // Support IN filters on string and numeric columns
      case In(attr, _)     => isStringOrNumericColumn(attr)
      case IsNull(attr)    => isStringOrNumericColumn(attr)
      case IsNotNull(attr) => isStringOrNumericColumn(attr)
      // Support AND combinations of supported filters
      case And(left, right) =>
        isSupportedFilter(left) && isSupportedFilter(right)
      // Support OR combinations of supported filters
      case Or(left, right) =>
        isSupportedFilter(left) && isSupportedFilter(right)
      case _ => false
    }
  }

  private def isStringOrNumericColumn(columnName: String): Boolean = {
    schema.fields.find(_.name == columnName) match {
      case Some(field) =>
        field.dataType match {
          case StringType | LongType | SparkDataTypes.IntegerType |
              SparkDataTypes.DoubleType | SparkDataTypes.FloatType |
              SparkDataTypes.BooleanType =>
            true
          case _ => false
        }
      case None => false
    }
  }

  private def isNumericColumn(columnName: String): Boolean = {
    schema.fields.find(_.name == columnName) match {
      case Some(field) =>
        field.dataType match {
          case LongType | SparkDataTypes.IntegerType |
              SparkDataTypes.DoubleType | SparkDataTypes.FloatType =>
            true
          case _ => false
        }
      case None => false
    }
  }

  /** Extract all column names referenced in a filter
    */
  private def extractFilterColumns(filter: Filter): Seq[String] = {
    import org.apache.spark.sql.sources._
    filter match {
      case EqualTo(attr, _)            => Seq(attr)
      case GreaterThan(attr, _)        => Seq(attr)
      case GreaterThanOrEqual(attr, _) => Seq(attr)
      case LessThan(attr, _)           => Seq(attr)
      case LessThanOrEqual(attr, _)    => Seq(attr)
      case In(attr, _)                 => Seq(attr)
      case IsNull(attr)                => Seq(attr)
      case IsNotNull(attr)             => Seq(attr)
      case And(left, right) =>
        extractFilterColumns(left) ++ extractFilterColumns(right)
      case Or(left, right) =>
        extractFilterColumns(left) ++ extractFilterColumns(right)
      case _ => Seq.empty
    }
  }

  override def build(): Scan = {
    new MilvusScan(currentSchema, currentOptions, pushedFilterArray)
  }
}

object MilvusScan extends Logging {
  private[sources] case class SnapshotCleanupRegistration(
      session: SparkSession,
      executionId: Long
  )

  private val SnapshotCleanupDrainTimeout = 2.seconds
  private val InitialDropRetryDelayMillis = 200L
  private val DropRetryMaxAttempts = 7
  private val DefaultClientSnapshotCompactionProtectionSeconds = 86400L
  private val MaxClientSnapshotCompactionProtectionSeconds =
    7L * 24L * 60L * 60L
  private val MaxGeneratedSnapshotNameLength = 255
  private val DefaultAwsCredentialsProvider =
    "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
  private val SimpleAwsCredentialsProvider =
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

  private def daemonThreadFactory(namePrefix: String): ThreadFactory =
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, namePrefix)
        thread.setDaemon(true)
        thread
      }
    }

  private val CleanupRpcExecutor = Executors.newFixedThreadPool(
    2,
    daemonThreadFactory("milvus-snapshot-cleanup-rpc")
  )

  private implicit val CleanupExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(CleanupRpcExecutor)

  private val PendingCleanupFutures =
    ConcurrentHashMap.newKeySet[Future[Unit]]()
  private val CleanupDraining = new AtomicBoolean(false)
  private val CleanupSubmissionLock = new Object

  private[sources] def drainPendingCleanupFutures(
      timeout: FiniteDuration = SnapshotCleanupDrainTimeout
  ): Unit = {
    CleanupSubmissionLock.synchronized {
      CleanupDraining.set(true)
    }
    val deadline = timeout.fromNow
    var keepDraining = true
    while (keepDraining && deadline.timeLeft.length > 0) {
      val snapshot = PendingCleanupFutures.asScala.toSeq
      if (snapshot.isEmpty) {
        keepDraining = false
      } else {
        snapshot.foreach { future =>
          val remaining = deadline.timeLeft
          if (remaining.length > 0) {
            try Await.ready(future, remaining)
            catch {
              case NonFatal(e) =>
                logWarning(
                  "Timed out waiting for client snapshot cleanup",
                  e
                )
            }
          }
        }
      }
    }
  }

  Try(
    Runtime.getRuntime.addShutdownHook(
      new Thread(
        new Runnable {
          override def run(): Unit = drainPendingCleanupFutures()
        },
        "milvus-snapshot-cleanup-shutdown-drain"
      )
    )
  ).failed.foreach { e =>
    logWarning("Failed to register client snapshot cleanup shutdown hook", e)
  }

  private val SnapshotOptionKeys = Seq(
    MilvusOption.SnapshotMode,
    MilvusOption.SnapshotManifests,
    MilvusOption.SnapshotV2Segments,
    MilvusOption.SnapshotCollectionId,
    MilvusOption.SnapshotPartitionIds,
    MilvusOption.SnapshotSchemaJson,
    MilvusOption.SnapshotSchemaBytes
  )

  private[sources] def resolveClientSnapshotLocation(
      location: String,
      bucket: String
  ): String = {
    val trimmed = Option(location).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      throw new IllegalArgumentException("snapshot s3_location is empty")
    }

    val scheme = Option(new URI(trimmed).getScheme).map(_.toLowerCase)
    scheme match {
      case Some("s3a") => trimmed
      case Some("s3") =>
        s"s3a://${trimmed.substring(trimmed.indexOf("://") + 3)}"
      case Some(other) =>
        throw new IllegalArgumentException(
          s"Unsupported snapshot s3_location scheme '$other': $trimmed"
        )
      case None if bucket.trim.nonEmpty =>
        s"s3a://${bucket.trim}/${trimmed.stripPrefix("/")}"
      case None =>
        throw new IllegalArgumentException(
          "bucket-relative snapshot s3_location requires connector S3 bucket"
        )
    }
  }

  private[sources] def snapshotBucket(location: String): Option[String] = {
    val trimmed = Option(location).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      None
    } else {
      val uri = new URI(trimmed)
      Option(uri.getScheme).map(_.toLowerCase) match {
        case Some("s3a") | Some("s3") =>
          Option(uri.getHost).orElse {
            Option(uri.getAuthority)
              .map(_.takeWhile(_ != '@'))
              .map(_.split(":").head)
              .filter(_.nonEmpty)
          }
        case Some(other) =>
          throw new IllegalArgumentException(
            s"Unsupported snapshot s3_location scheme '$other': $trimmed"
          )
        case None => None
      }
    }
  }

  private[sources] def snapshotBucketsToConfigure(
      snapshotPath: String,
      connectorBucket: String
  ): Seq[String] = {
    (Seq(connectorBucket).filter(_.nonEmpty) ++ snapshotBucket(
      snapshotPath
    )).distinct
  }

  private[sources] def optionValue(
      options: scala.collection.Map[String, String],
      key: String
  ): Option[String] = {
    options.collectFirst {
      case (optionKey, value) if optionKey.equalsIgnoreCase(key) => value
    }
  }

  private[sources] def connectorS3BucketOption(
      options: scala.collection.Map[String, String]
  ): Option[String] = {
    optionValue(options, Properties.FsConfig.FsBucketName)
      .map(_.trim)
      .filter(_.nonEmpty)
  }

  private[sources] def resolveConnectorS3Bucket(
      options: scala.collection.Map[String, String]
  ): String = {
    connectorS3BucketOption(options).getOrElse {
      throw new IllegalArgumentException(
        s"${Properties.FsConfig.FsBucketName} is required for client snapshot reads"
      )
    }
  }

  private[sources] def snapshotS3BucketForRelativePaths(
      snapshotPath: String,
      options: scala.collection.Map[String, String]
  ): Option[String] = {
    snapshotBucket(snapshotPath).orElse(connectorS3BucketOption(options))
  }

  private[sources] def storageV2ManifestBasePath(
      item: StorageV2ManifestItem
  ): String = {
    MilvusSnapshotReader.parseManifestContent(item.manifest) match {
      case Right(content) => content.basePath
      case Left(_)        => item.manifest
    }
  }

  private[sources] def validateSnapshotBucketForRelativeDataPaths(
      snapshotPath: String,
      connectorBucket: Option[String],
      storageV2ManifestList: Seq[StorageV2ManifestItem],
      v2Segments: Seq[V2SegmentInfo]
  ): Unit = {
    snapshotBucket(snapshotPath).foreach { snapshot =>
      val relativeStorageV3Paths = storageV2ManifestList
        .map(storageV2ManifestBasePath)
        .filter(isBucketRelativeSnapshotLocation)
      val relativeStorageV2Paths = v2Segments
        .flatMap(_.columnGroups.flatMap(_.filePaths))
        .filter(isBucketRelativeSnapshotLocation)
      val relativePaths = relativeStorageV3Paths ++ relativeStorageV2Paths
      if (relativePaths.nonEmpty && !connectorBucket.contains(snapshot)) {
        val connectorDescription = connectorBucket.getOrElse("<unset>")
        throw new IllegalArgumentException(
          s"Client-created snapshot metadata is in bucket '$snapshot' but " +
            s"${Properties.FsConfig.FsBucketName} is '$connectorDescription' and snapshot data paths are bucket-relative. " +
            "Refusing to guess which bucket native executors should use; set the connector bucket to the data bucket or use fully-qualified data paths. " +
            s"Example relative path: ${relativePaths.head}"
        )
      }
    }
  }

  private[sources] def ensureClientSnapshotHasPackedSegments(
      storageV2ManifestList: Seq[StorageV2ManifestItem],
      v2Segments: Seq[V2SegmentInfo],
      collectionName: String
  ): Unit = {
    if (storageV2ManifestList.isEmpty && v2Segments.isEmpty) {
      throw new IllegalArgumentException(
        s"No packed-parquet segments (StorageV2/V3) found in client-created snapshot for collection " +
          s"$collectionName. This connector requires Milvus 2.6+ with Storage V2 or V3. " +
          "Please ensure the collection has been flushed and contains data."
      )
    }
  }

  private[sources] def isBucketRelativeSnapshotLocation(
      location: String
  ): Boolean = {
    val trimmed = Option(location).map(_.trim).getOrElse("")
    trimmed.nonEmpty && Option(new URI(trimmed).getScheme).isEmpty
  }

  private[sources] def canUseClientSnapshotFastPath(
      milvusOption: MilvusOption
  ): Boolean = {
    milvusOption.partitionName.isEmpty &&
    milvusOption.partitionID.isEmpty &&
    milvusOption.segmentID.isEmpty
  }

  private[sources] def generatedClientSnapshotName(
      collectionName: String,
      currentTimeMillis: Long = System.currentTimeMillis(),
      uuid: String = UUID.randomUUID().toString.replace("-", "")
  ): String = {
    val suffix = s"_${currentTimeMillis}_$uuid"
    val prefix = "spark_read_"
    val maxCollectionNameLength =
      MaxGeneratedSnapshotNameLength - prefix.length - suffix.length
    val sanitizedCollectionName =
      collectionName.replaceAll("[^A-Za-z0-9_]", "_")
    val safeCollectionName = sanitizedCollectionName.take(
      maxCollectionNameLength.max(0)
    )
    s"$prefix$safeCollectionName$suffix"
  }

  private[sources] def parsePositiveLongOption(
      options: CaseInsensitiveStringMap,
      key: String,
      defaultValue: Long
  ): Long = {
    val value = Option(options.get(key))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { raw =>
        try raw.toLong
        catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Option '$key' must be a positive long, got '$raw'"
            )
        }
      }
      .getOrElse(defaultValue)
    if (value <= 0) {
      throw new IllegalArgumentException(
        s"Option '$key' must be positive, got $value"
      )
    }
    value
  }

  private[sources] def parseClientSnapshotCompactionProtectionSeconds(
      options: CaseInsensitiveStringMap
  ): Long = {
    val value = parsePositiveLongOption(
      options,
      MilvusOption.ClientSnapshotCompactionProtectionSeconds,
      DefaultClientSnapshotCompactionProtectionSeconds
    )
    if (value > MaxClientSnapshotCompactionProtectionSeconds) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.ClientSnapshotCompactionProtectionSeconds}' must be <= " +
          s"$MaxClientSnapshotCompactionProtectionSeconds seconds, got $value"
      )
    }
    if (value > DefaultClientSnapshotCompactionProtectionSeconds) {
      logWarning(
        s"Client snapshot compaction protection is set to $value seconds; " +
          "long protection windows can delay Milvus compaction."
      )
    }
    value
  }

  private[sources] def activeCleanupRegistration()
      : Option[SnapshotCleanupRegistration] = {
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession) match {
      case Some(session) =>
        Option(
          session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        ).flatMap { raw =>
          try {
            val executionId = raw.trim.toLong
            Some(SnapshotCleanupRegistration(session, executionId))
          } catch {
            case _: NumberFormatException =>
              logWarning(
                s"Ignoring non-numeric ${SQLExecution.EXECUTION_ID_KEY}: $raw"
              )
              None
          }
        }
      case None => None
    }
  }

  private[sources] def dropClientReadSnapshot(
      client: MilvusClient,
      databaseName: String,
      collectionName: String,
      snapshotName: String,
      reason: String,
      maxAttempts: Int = DropRetryMaxAttempts
  ): Try[Unit] = {
    var lastFailure = Option.empty[Throwable]
    (1 to maxAttempts).foreach { attempt =>
      client.dropSnapshot(databaseName, collectionName, snapshotName) match {
        case Success(_) =>
          logInfo(s"Dropped client read snapshot $snapshotName after $reason")
          return Success(())
        case Failure(e) if MilvusClient.isSnapshotAlreadyDropped(e) =>
          logInfo(
            s"Client read snapshot $snapshotName was already dropped after $reason"
          )
          return Success(())
        case Failure(e) if MilvusClient.isTerminalSnapshotDropError(e) =>
          logWarning(
            s"Not retrying terminal failure while dropping client read snapshot $snapshotName after $reason",
            e
          )
          return Failure(e)
        case Failure(e) =>
          lastFailure = Some(e)
          logWarning(
            s"Failed to drop client read snapshot $snapshotName after $reason " +
              s"(attempt $attempt/$maxAttempts)",
            e
          )
          if (attempt < maxAttempts) {
            val delayMillis = InitialDropRetryDelayMillis << (attempt - 1)
            Thread.sleep(delayMillis)
          }
      }
    }
    Failure(
      lastFailure.getOrElse(
        new RuntimeException(
          s"Failed to drop client read snapshot $snapshotName after $reason"
        )
      )
    )
  }

  private[sources] def preserveResultWhenCloseFails(
      result: Try[Unit],
      close: => Unit,
      closeDescription: String
  ): Try[Unit] = {
    Try(close).failed.foreach { e =>
      logWarning(s"Failed to close $closeDescription", e)
    }
    result
  }

  private def dropClientReadSnapshotWithNewClient(
      baseOptions: Map[String, String],
      databaseName: String,
      collectionName: String,
      snapshotName: String,
      reason: String
  ): Try[Unit] = {
    Try(MilvusClient(MilvusOption(baseOptions))).flatMap { client =>
      val dropResult = dropClientReadSnapshot(
        client,
        databaseName,
        collectionName,
        snapshotName,
        reason
      )
      preserveResultWhenCloseFails(
        dropResult,
        client.close(),
        s"Milvus client after dropping client read snapshot $snapshotName"
      )
    }
  }

  private def submitClientSnapshotCleanup(
      baseOptions: Map[String, String],
      databaseName: String,
      collectionName: String,
      snapshotName: String,
      reason: String
  ): Unit = {
    CleanupSubmissionLock.synchronized {
      if (CleanupDraining.get()) {
        logWarning(
          s"Skipping client read snapshot cleanup submission for $snapshotName after $reason because shutdown drain has started"
        )
        return
      }
      val cleanupFuture = Future {
        dropClientReadSnapshotWithNewClient(
          baseOptions,
          databaseName,
          collectionName,
          snapshotName,
          reason
        ) match {
          case Success(_) =>
          case Failure(e) =>
            logError(
              s"Failed to drop client read snapshot $snapshotName after $reason",
              e
            )
        }
      }
      PendingCleanupFutures.add(cleanupFuture)
      cleanupFuture.onComplete(_ => PendingCleanupFutures.remove(cleanupFuture))
    }
  }

  private[sources] def registerClientSnapshotCleanup(
      registration: SnapshotCleanupRegistration,
      baseOptions: Map[String, String],
      databaseName: String,
      collectionName: String,
      snapshotName: String
  ): Boolean = {
    val cleanupTriggered = new AtomicBoolean(false)
    val session = registration.session
    val executionId = registration.executionId

    def submitCleanup(reason: String): Unit = {
      submitClientSnapshotCleanup(
        baseOptions,
        databaseName,
        collectionName,
        snapshotName,
        reason
      )
    }

    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case e: SparkListenerSQLExecutionEnd
              if e.executionId == executionId && cleanupTriggered
                .compareAndSet(false, true) =>
            try submitCleanup(s"Spark SQL execution $executionId ended")
            finally session.sparkContext.removeSparkListener(this)
          case _ =>
        }
      }
    }

    Try(session.sparkContext.addSparkListener(listener)) match {
      case Success(_) => true
      case Failure(e) =>
        logError(
          s"Failed to register cleanup listener for client read snapshot $snapshotName",
          e
        )
        false
    }
  }

  private[sources] def buildClientSnapshotOptions(
      baseOptions: Map[String, String],
      collectionName: String,
      collectionId: Long,
      partitionIds: Seq[Long],
      schemaBytesBase64: String,
      manifestList: Seq[StorageV2ManifestItem],
      v2Segments: Seq[V2SegmentInfo],
      snapshotBucketForRelativePaths: Option[String] = None
  ): Map[String, String] = {
    var out = baseOptions.filterNot { case (key, _) =>
      SnapshotOptionKeys.exists(_.equalsIgnoreCase(key))
    }
    out = out ++ Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.MilvusCollectionName -> collectionName,
      MilvusOption.SnapshotCollectionId -> collectionId.toString,
      MilvusOption.SnapshotPartitionIds -> partitionIds.mkString(","),
      MilvusOption.SnapshotSchemaBytes -> schemaBytesBase64,
      MilvusOption.SnapshotManifests ->
        MilvusSnapshotReader.serializeManifestList(manifestList)
    )
    if (v2Segments.nonEmpty) {
      out += MilvusOption.SnapshotV2Segments ->
        MilvusSnapshotReader.serializeV2Segments(v2Segments)
    }
    snapshotBucketForRelativePaths.foreach { bucket =>
      out = out.filterNot { case (key, _) =>
        key.equalsIgnoreCase(Properties.FsConfig.FsBucketName)
      }
      out += Properties.FsConfig.FsBucketName -> bucket
    }
    out
  }

  private[sources] def validateClientSnapshotMetadata(
      metadata: SnapshotMetadata,
      snapshotPath: String
  ): SnapshotMetadata = {
    if (metadata == null) {
      throw new IllegalArgumentException(
        s"Client-created snapshot metadata at $snapshotPath is missing metadata"
      )
    }
    if (metadata.snapshotInfo == null) {
      throw new IllegalArgumentException(
        s"Client-created snapshot metadata at $snapshotPath is missing snapshot_info"
      )
    }
    if (metadata.collection == null) {
      throw new IllegalArgumentException(
        s"Client-created snapshot metadata at $snapshotPath is missing collection"
      )
    }
    if (metadata.collection.schema == null) {
      throw new IllegalArgumentException(
        s"Client-created snapshot metadata at $snapshotPath is missing collection.schema"
      )
    }
    if (
      metadata.manifestList.isEmpty &&
      metadata.storageV2ManifestList.forall(_.isEmpty)
    ) {
      throw new IllegalArgumentException(
        s"Invalid client-created snapshot metadata at $snapshotPath: client snapshot is empty: no manifests and no V2 segments"
      )
    }
    metadata
  }
}

class MilvusScan(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends Scan
    with Batch
    with Logging {
  private val milvusOption = MilvusOption(options)

  // Get vector search configuration from MilvusOption
  private val vectorSearchConfig = milvusOption.vectorSearchConfig

  // Log vector search configuration if enabled
  vectorSearchConfig.foreach { config =>
    logInfo(
      s"Vector search enabled: topK=${config.topK}, metric=${config.metricType}, column=${config.vectorColumn}"
    )
  }

  override def readSchema(): StructType = {
    schema
  }

  override def toBatch: Batch = this

  private lazy val plannedSnapshotPartitions: Array[InputPartition] =
    computeInputPartitions()

  override def planInputPartitions(): Array[InputPartition] = {
    if (shouldCacheInputPartitions) plannedSnapshotPartitions
    else computeInputPartitions()
  }

  private[sources] def shouldCacheInputPartitions: Boolean =
    MilvusOption.isSnapshotMode(options)

  private def computeInputPartitions(): Array[InputPartition] = {
    if (MilvusOption.isSnapshotMode(options)) {
      MilvusOption.validateSnapshotModeOptions(options)
      val snapshotManifests = Option(
        options.get(MilvusOption.SnapshotManifests)
      )
      return planInputPartitionsFromSnapshot(snapshotManifests.getOrElse(""))
    }

    if (milvusOption.collectionName.isEmpty) {
      throw new IllegalArgumentException("collectionName cannot be empty")
    }

    val client = MilvusClient(milvusOption)
    try {
      val clientSnapshotPartitions =
        if (MilvusScan.canUseClientSnapshotFastPath(milvusOption)) {
          planInputPartitionsFromClientSnapshot(client)
        } else {
          logInfo(
            "client snapshot fast path disabled because partition/segment selector is set"
          )
          None
        }

      clientSnapshotPartitions.getOrElse {
        val collectionInfo = client
          .getCollectionInfo(
            milvusOption.databaseName,
            milvusOption.collectionName
          )
          .getOrElse(
            throw new Exception(
              s"Collection ${milvusOption.collectionName} not found"
            )
          )
        planInputPartitionsFromLegacyClient(client, collectionInfo)
      }
    } finally {
      client.close()
    }
  }

  private def planInputPartitionsFromClientSnapshot(
      client: MilvusClient
  ): Option[Array[InputPartition]] = {
    val snapshotName = Option(options.get(MilvusOption.ClientSnapshotName))
      .filter(_.trim.nonEmpty)
      .getOrElse(
        MilvusScan.generatedClientSnapshotName(milvusOption.collectionName)
      )
    val description = Option(
      options.get(MilvusOption.ClientSnapshotDescription)
    ).filter(_.trim.nonEmpty).getOrElse("spark connector client read snapshot")
    val protectionSeconds =
      MilvusScan.parseClientSnapshotCompactionProtectionSeconds(options)

    val cleanupRegistration = MilvusScan.activeCleanupRegistration()
    if (cleanupRegistration.isEmpty) {
      logWarning(
        "Skipping client snapshot fast path because Spark SQL execution cleanup cannot be registered; " +
          "falling back to legacy GetPersistentSegmentInfo read path"
      )
      return None
    }

    client.createSnapshotForRead(
      milvusOption.databaseName,
      milvusOption.collectionName,
      snapshotName,
      description,
      protectionSeconds
    ) match {
      case scala.util.Success(snapshot) =>
        val connectorBucket = MilvusScan.connectorS3BucketOption(
          milvusOption.options
        )
        if (
          connectorBucket.isEmpty && MilvusScan
            .isBucketRelativeSnapshotLocation(snapshot.s3Location)
        ) {
          logWarning(
            s"Skipping client snapshot fast path because ${Properties.FsConfig.FsBucketName} is missing " +
              "and the snapshot location is bucket-relative; falling back to legacy GetPersistentSegmentInfo read path"
          )
          MilvusScan.submitClientSnapshotCleanup(
            options.asScala.toMap,
            milvusOption.databaseName,
            milvusOption.collectionName,
            snapshot.name,
            s"missing ${Properties.FsConfig.FsBucketName} for bucket-relative snapshot location"
          )
          None
        } else if (
          MilvusScan.registerClientSnapshotCleanup(
            cleanupRegistration.get,
            options.asScala.toMap,
            milvusOption.databaseName,
            milvusOption.collectionName,
            snapshot.name
          )
        ) {
          logWarning(
            s"Client read snapshot ${snapshot.name} will be dropped when the Spark SQL execution ends; " +
              "an unclean driver exit can leave it behind and require manual cleanup."
          )
          val snapshotPath = MilvusScan.resolveClientSnapshotLocation(
            snapshot.s3Location,
            connectorBucket.getOrElse("")
          )
          Some(planInputPartitionsFromClientSnapshotPath(snapshotPath))
        } else {
          MilvusScan.submitClientSnapshotCleanup(
            options.asScala.toMap,
            milvusOption.databaseName,
            milvusOption.collectionName,
            snapshot.name,
            "cleanup registration failure"
          )
          None
        }

      case scala.util.Failure(e) if MilvusClient.isServiceNotImplemented(e) =>
        logWarning(
          "CreateSnapshot/DescribeSnapshot is not implemented by this Milvus service; " +
            "falling back to legacy GetPersistentSegmentInfo read path"
        )
        None

      case scala.util.Failure(e) =>
        throw new RuntimeException(
          s"Failed to create client read snapshot for collection ${milvusOption.collectionName}: ${e.getMessage}",
          e
        )
    }
  }

  private def planInputPartitionsFromClientSnapshotPath(
      snapshotPath: String
  ): Array[InputPartition] = {
    val hadoopConf = buildSnapshotHadoopConf(snapshotPath)
    val snapshotJson = readAllBytes(hadoopConf, snapshotPath)
    val metadata =
      MilvusScan.validateClientSnapshotMetadata(
        MilvusSnapshotReader.parseSnapshotMetadata(snapshotJson) match {
          case Right(value) => value
          case Left(err) =>
            throw new IllegalArgumentException(
              s"Failed to parse client-created snapshot metadata: ${err.getMessage}",
              err
            )
        },
        snapshotPath
      )

    val snapshotBucket = MilvusScan.snapshotS3BucketForRelativePaths(
      snapshotPath,
      milvusOption.options
    )
    val v2Segments =
      if (metadata.manifestList.nonEmpty) {
        V2SegmentLoader.loadV2Segments(
          metadata.manifestList,
          snapshotBucket.getOrElse(""),
          hadoopConf
        ) match {
          case Right(segs) => segs
          case Left(err) =>
            throw new IllegalStateException(
              s"Failed to load StorageV2 segments from client-created snapshot: ${err.getMessage}",
              err
            )
        }
      } else Seq.empty
    val storageV2ManifestList =
      metadata.storageV2ManifestList.getOrElse(Seq.empty)
    MilvusScan.ensureClientSnapshotHasPackedSegments(
      storageV2ManifestList,
      v2Segments,
      metadata.collection.schema.name
    )
    MilvusScan.validateSnapshotBucketForRelativeDataPaths(
      snapshotPath,
      MilvusScan.connectorS3BucketOption(milvusOption.options),
      storageV2ManifestList,
      v2Segments
    )

    val schemaBytes = MilvusSnapshotReader.toProtobufSchemaBytes(
      metadata.collection.schema
    )
    val snapshotOptions = MilvusScan.buildClientSnapshotOptions(
      options.asScala.toMap,
      collectionName = metadata.collection.schema.name,
      collectionId = metadata.snapshotInfo.collectionId,
      partitionIds = metadata.snapshotInfo.partitionIds,
      schemaBytesBase64 = Base64.getEncoder.encodeToString(schemaBytes),
      manifestList = storageV2ManifestList,
      v2Segments = v2Segments,
      snapshotBucketForRelativePaths = snapshotBucket
    )
    require(
      MilvusOption.isSnapshotMode(snapshotOptions),
      "Client-created snapshot options must enable snapshot mode"
    )

    new MilvusScan(
      schema,
      new CaseInsensitiveStringMap(snapshotOptions.asJava),
      pushedFilters
    ).planInputPartitions()
  }

  private def planInputPartitionsFromLegacyClient(
      client: MilvusClient,
      collectionInfo: MilvusCollectionInfo
  ): Array[InputPartition] = {
    val collection = milvusOption.collectionID
    val partition = milvusOption.partitionID
    val segment = milvusOption.segmentID
    val s3RootPath =
      milvusOption.options.getOrElse(Properties.FsConfig.FsRootPath, "files")

    def createPartition(
        segmentID: String,
        partitionID: String
    ): InputPartition = {
      val segmentPath =
        s"$s3RootPath/insert_log/$collection/$partitionID/$segmentID"
      logInfo(
        s"Creating V3 partition: segmentID=$segmentID, segmentPath=$segmentPath"
      )

      val segmentIDLong =
        try { segmentID.toLong }
        catch { case _: NumberFormatException => -1L }
      MilvusStorageV3InputPartition(
        segmentPath,
        collectionInfo.schema.toByteArray,
        partitionID,
        milvusOption,
        vectorSearchConfig.map(_.topK),
        vectorSearchConfig.map(_.queryVector),
        vectorSearchConfig.map(_.metricType),
        vectorSearchConfig.map(_.vectorColumn),
        segmentIDLong
      )
    }

    val allPackedSegments = client
      .getSegments(
        milvusOption.databaseName,
        milvusOption.collectionName
      )
      .getOrElse(
        throw new Exception("Failed to get segments")
      )
      .filter(_.storageVersion >= 2)

    if (allPackedSegments.isEmpty) {
      throw new IllegalArgumentException(
        s"No packed-parquet segments (StorageV2/V3) found in collection " +
          s"${milvusOption.collectionName}. This connector requires Milvus " +
          "2.6+ with Storage V2 or V3. Please ensure the collection has " +
          "been flushed and contains data."
      )
    }

    val partitions =
      if (partition.nonEmpty && segment.nonEmpty) {
        val segmentInfo =
          allPackedSegments.find(_.segmentID.toString == segment)
        segmentInfo match {
          case Some(seg) =>
            if (seg.partitionID.toString != partition) {
              throw new IllegalArgumentException(
                s"Segment $segment belongs to partition ${seg.partitionID}, not $partition"
              )
            }
            Array(createPartition(segment, partition))
          case None =>
            throw new IllegalArgumentException(
              s"Segment $segment not found or has storage_version < 2 " +
                "(StorageV2/V3 packed parquet required)"
            )
        }
      } else if (partition.nonEmpty) {
        allPackedSegments
          .filter(_.partitionID.toString == partition)
          .map(seg => createPartition(seg.segmentID.toString, partition))
          .toArray
      } else {
        allPackedSegments.map { seg =>
          createPartition(seg.segmentID.toString, seg.partitionID.toString)
        }.toArray
      }

    logInfo(s"Created ${partitions.length} partitions via legacy client path")
    partitions
  }

  private[sources] def buildSnapshotHadoopConf(
      snapshotPath: String
  ): Configuration = {
    val conf = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    val rawOptions = milvusOption.options
    val endpoint =
      MilvusScan.optionValue(rawOptions, Properties.FsConfig.FsAddress)
    val accessKey =
      MilvusScan.optionValue(rawOptions, Properties.FsConfig.FsAccessKeyId)
    val secretKey =
      MilvusScan.optionValue(rawOptions, Properties.FsConfig.FsAccessKeyValue)
    val useSsl =
      MilvusScan.optionValue(rawOptions, Properties.FsConfig.FsUseSSL)
    val region =
      MilvusScan.optionValue(rawOptions, Properties.FsConfig.FsRegion)
    val useIam = MilvusScan
      .optionValue(rawOptions, Properties.FsConfig.FsUseIam)
      .exists(_.trim.equalsIgnoreCase("true"))
    val useVirtualHost = MilvusScan
      .optionValue(rawOptions, Properties.FsConfig.FsUseVirtualHost)
      .filter(_.trim.nonEmpty)
    val pathStyle = MilvusScan
      .optionValue(rawOptions, "fs.s3a.path.style.access")
      .orElse(
        useVirtualHost.map(v => (!v.trim.equalsIgnoreCase("true")).toString)
      )

    def setIfDefined(key: String, value: Option[String]): Unit = {
      value.map(_.trim).filter(_.nonEmpty).foreach(conf.set(key, _))
    }

    def configureS3A(prefix: String): Unit = {
      setIfDefined(s"$prefix.endpoint", endpoint)
      setIfDefined(s"$prefix.connection.ssl.enabled", useSsl)
      setIfDefined(s"$prefix.path.style.access", pathStyle)
      setIfDefined(s"$prefix.endpoint.region", region)
      setIfDefined(s"$prefix.region", region)
      if (useIam) {
        conf.unset(s"$prefix.access.key")
        conf.unset(s"$prefix.secret.key")
        conf.set(
          s"$prefix.aws.credentials.provider",
          MilvusScan.DefaultAwsCredentialsProvider
        )
      } else {
        setIfDefined(s"$prefix.access.key", accessKey)
        setIfDefined(s"$prefix.secret.key", secretKey)
        if (
          accessKey.exists(_.trim.nonEmpty) && secretKey.exists(_.trim.nonEmpty)
        ) {
          conf.set(
            s"$prefix.aws.credentials.provider",
            MilvusScan.SimpleAwsCredentialsProvider
          )
        }
      }
    }

    if (conf.get("fs.s3a.impl") == null) {
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    }
    conf.set("fs.s3a.impl.disable.cache", "true")
    if (
      !useIam && (accessKey
        .forall(_.trim.isEmpty) || secretKey.forall(_.trim.isEmpty))
    ) {
      logWarning(
        "Snapshot S3 credentials were not provided; Hadoop S3A will use the default AWS credential provider chain."
      )
    }
    configureS3A("fs.s3a")

    MilvusScan
      .snapshotBucketsToConfigure(
        snapshotPath,
        MilvusScan.connectorS3BucketOption(rawOptions).getOrElse("")
      )
      .foreach(bucket => configureS3A(s"fs.s3a.bucket.$bucket"))
    conf
  }

  private[sources] def readAllBytes(
      conf: Configuration,
      path: String
  ): String = {
    val maxBytes = MilvusScan.parsePositiveLongOption(
      options,
      MilvusOption.SnapshotMaxJsonBytes,
      MilvusSnapshotReader.MaxSnapshotJsonBytes
    )
    val uri = new URI(path)
    val fs = FileSystem.get(uri, conf)
    try {
      val in = fs.open(new Path(uri))
      try MilvusSnapshotReader.readUtf8WithLimit(in, path, maxBytes)
      finally in.close()
    } finally {
      Option(uri.getScheme).foreach { scheme =>
        if (conf.getBoolean(s"fs.$scheme.impl.disable.cache", false)) {
          fs.close()
        }
      }
    }
  }

  /** Plan input partitions from snapshot manifests (offline mode - no client
    * connection) This enables reading Milvus data purely from snapshot metadata
    * without any client calls.
    *
    * The caller can pass either:
    *   - `manifestsJson` (the legacy `SnapshotManifests` option) for
    *     manifest-based segments (segment-info `storage_version = 3`), or
    *   - a non-empty `SnapshotV2Segments` option carrying a materialized list
    *     of [[com.zilliz.spark.connector.read.V2SegmentInfo]] for non-manifest
    *     packed-parquet segments (segment-info `storage_version = 2`).
    *
    * When both are present the planner emits partitions from both sources
    * (mixed-version snapshot).
    */
  private def planInputPartitionsFromSnapshot(
      manifestsJson: String
  ): Array[InputPartition] = {
    import com.zilliz.spark.connector.read.{
      MilvusSnapshotReader,
      StorageV2ManifestItem
    }

    logInfo(
      "Using snapshot mode for partition planning (no Milvus client connection)"
    )

    // Parse manifest list from JSON. Empty or null means no V3 manifests
    // were supplied (pure V2-packed snapshot) — fall through to the
    // packed-V2 branch below without erroring out.
    val manifestList: Seq[StorageV2ManifestItem] =
      if (manifestsJson == null || manifestsJson.isEmpty) Seq.empty
      else
        MilvusSnapshotReader.deserializeManifestList(manifestsJson) match {
          case Right(list) => list
          case Left(e) =>
            throw new Exception(
              s"Failed to parse snapshot manifests: ${e.getMessage}",
              e
            )
        }

    // Get partition IDs from options (comma-separated)
    val partitionIds = Option(options.get(MilvusOption.SnapshotPartitionIds))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty))
      .getOrElse(Array.empty[String])

    // Use first partition ID as default, or "0" if none provided
    val defaultPartitionId = partitionIds.headOption.getOrElse("0")

    // Get schema bytes from options (Base64 encoded)
    val schemaBytes = Option(options.get(MilvusOption.SnapshotSchemaBytes))
      .map(base64 => java.util.Base64.getDecoder.decode(base64))
      .getOrElse {
        logWarning(
          "No schema bytes provided in snapshot mode, using empty schema"
        )
        Array.empty[Byte]
      }

    logInfo(
      s"Using schema bytes (${schemaBytes.length} bytes) for V2 partitions"
    )

    // Create V2 input partitions from snapshot manifests
    val v2Partitions = manifestList.map { item =>
      // Try to parse manifest as JSON to extract basePath and ver (version)
      // If parsing fails, treat the manifest string as a plain basePath (backward compatible)
      val (basePath, readVersion) =
        MilvusSnapshotReader.parseManifestContent(item.manifest) match {
          case Right(content) => (content.basePath, content.ver.toLong)
          case Left(_) =>
            (
              item.manifest,
              -1L
            ) // Backward compatible: plain basePath, latest version
        }

      // Extract segmentID from manifest path if item.segmentID is 0
      // Path format: files/insert_log/{collectionID}/{partitionID}/{segmentID}
      val segmentID = if (item.segmentID != 0L) {
        item.segmentID
      } else {
        // Try to extract from basePath
        val pathParts = basePath.split("/")
        if (pathParts.length >= 1) {
          try {
            pathParts.last.toLong
          } catch {
            case _: NumberFormatException => 0L
          }
        } else 0L
      }
      logInfo(
        s"Creating partition with manifestPath=$basePath, segmentID=$segmentID, readVersion=$readVersion"
      )
      MilvusStorageV3InputPartition(
        basePath, // The basePath extracted from manifest JSON
        schemaBytes, // Protobuf CollectionSchema bytes from snapshot
        defaultPartitionId, // Partition name/ID
        milvusOption,
        vectorSearchConfig.map(_.topK),
        vectorSearchConfig.map(_.queryVector),
        vectorSearchConfig.map(_.metricType),
        vectorSearchConfig.map(_.vectorColumn),
        segmentID, // Segment ID extracted from path or from item
        readVersion // Manifest version from snapshot (-1 = latest)
      ): InputPartition
    }

    logInfo(
      s"Created ${v2Partitions.size} V2 partitions from snapshot manifests"
    )

    // Additional partitions from pre-materialized packed-V2 segments (if any).
    val packedV2Partitions: Seq[InputPartition] =
      Option(options.get(MilvusOption.SnapshotV2Segments))
        .filter(_.nonEmpty)
        .map { json =>
          MilvusSnapshotReader.deserializeV2Segments(json) match {
            case Right(segs) if segs.nonEmpty =>
              logInfo(
                s"Creating ${segs.size} packed-V2 partition(s) from " +
                  s"SnapshotV2Segments option"
              )
              segs.map { seg =>
                MilvusPackedV2InputPartition(
                  segmentID = seg.segmentId,
                  partitionID = seg.partitionId,
                  columnGroups = seg.columnGroups,
                  milvusSchemaBytes = schemaBytes,
                  milvusOption = milvusOption,
                  neededColumnFieldIds = Seq.empty
                ): InputPartition
              }
            case Right(_) => Seq.empty[InputPartition]
            case Left(e) =>
              throw new Exception(
                s"Failed to parse SnapshotV2Segments: ${e.getMessage}",
                e
              )
          }
        }
        .getOrElse(Seq.empty[InputPartition])

    (v2Partitions ++ packedV2Partitions).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Convert CaseInsensitiveStringMap to regular Map for serialization
    val optionsMap = options.asScala.toMap
    new MilvusPartitionReaderFactory(schema, optionsMap, pushedFilters)
  }
}
