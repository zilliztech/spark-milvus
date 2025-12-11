package com.zilliz.spark.connector.sources

import com.zilliz.spark.connector.loon.{ManifestBuilder, Properties}
import com.zilliz.spark.connector.read.{MilvusInputPartition, MilvusPartitionReaderFactory, MilvusStorageV2InputPartition}
import com.zilliz.spark.connector.write.MilvusWriteBuilder
import com.zilliz.spark.connector._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, DataTypes => SparkDataTypes}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.io.FileNotFoundException
import java.util.concurrent.ConcurrentHashMap
import java.util.{HashMap, Map => JMap}
import java.{util => ju}
import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._
import scala.jdk.CollectionConverters._

// 1. DataSourceRegister and TableProvider
case class MilvusDataSource() extends TableProvider with DataSourceRegister {
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val milvusOption = MilvusOption(options)
    if (milvusOption.uri.isEmpty) {
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
            field.nullable
          )
        )
      )
    } finally {
      client.close()
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

  def initInfo(): Unit = {
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
        field.nullable
      )
    )
    val maxFieldID = fieldName2ID.values.max
    if (milvusCollection.schema.enableDynamicField &&
      (fieldIDs.isEmpty || fieldIDs.contains((maxFieldID + 1).toString))) {
      fields = fields :+ StructField("$meta", StringType, true)
    }
    if (milvusOption.extraColumns.contains(MilvusOption.MilvusExtraColumnPartition)) {
      fields = fields :+ StructField("partition", StringType, true)
    }
    if (milvusOption.extraColumns.contains(MilvusOption.MilvusExtraColumnSegmentID)) {
      fields = fields :+ StructField("segment_id", LongType, false)
    }
    if (milvusOption.extraColumns.contains(MilvusOption.MilvusExtraColumnRowOffset)) {
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
    val vectorColumn = Option(options.get(MilvusOption.VectorSearchVectorColumn)).getOrElse("vector")
    val hasVectorSearch = Option(options.get(MilvusOption.VectorSearchQueryVector)).isDefined
    if (hasVectorSearch && fieldName2ID.contains(vectorColumn) && !fieldNames.contains(vectorColumn)) {
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

  /**
   * Extract all column names referenced in a filter
   */
  private def extractFilterColumns(filter: Filter): Seq[String] = {
    import org.apache.spark.sql.sources._
    filter match {
      case EqualTo(attr, _) => Seq(attr)
      case GreaterThan(attr, _) => Seq(attr)
      case GreaterThanOrEqual(attr, _) => Seq(attr)
      case LessThan(attr, _) => Seq(attr)
      case LessThanOrEqual(attr, _) => Seq(attr)
      case In(attr, _) => Seq(attr)
      case IsNull(attr) => Seq(attr)
      case IsNotNull(attr) => Seq(attr)
      case And(left, right) => extractFilterColumns(left) ++ extractFilterColumns(right)
      case Or(left, right) => extractFilterColumns(left) ++ extractFilterColumns(right)
      case _ => Seq.empty
    }
  }

  override def build(): Scan = {
    new MilvusScan(currentSchema, currentOptions, pushedFilterArray)
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
  private val readerOption = MilvusS3Option(options)
  private val pathOption: String = getPathOption()
  if (pathOption == null) {
    throw new IllegalArgumentException(
      "Option 'path' is required for mybinlog files."
    )
  }
  private val fieldIDs =
    if (options.get(MilvusOption.ReaderFieldIDs) != null) {
      options.get(MilvusOption.ReaderFieldIDs)
        .split(",")
        .toSeq
        .filter(_.nonEmpty)
    } else {
      Seq[String]()
    }

  // Get vector search configuration from MilvusOption
  private val vectorSearchConfig = milvusOption.vectorSearchConfig

  // Log vector search configuration if enabled
  vectorSearchConfig.foreach { config =>
    logInfo(s"Vector search enabled: topK=${config.topK}, metric=${config.metricType}, column=${config.vectorColumn}")
  }

  def getPathOption(): String = {
    if (options.get(MilvusOption.ReaderPath) != null) {
      return options.get(MilvusOption.ReaderPath)
    }
    val collection = milvusOption.collectionID
    val partition = milvusOption.partitionID
    val segment = milvusOption.segmentID
    val firstPath = "insert_log"
    if (collection.isEmpty) {
      throw new IllegalArgumentException(
        "Option 'collection' is required for reading milvus data."
      )
    }
    if (partition.isEmpty) {
      return s"${firstPath}/${collection}"
    }
    if (segment.isEmpty) {
      return s"${firstPath}/${collection}/${partition}"
    }
    return s"${firstPath}/${collection}/${partition}/${segment}"
  }

  override def readSchema(): StructType = {
    schema
  }

  def getCollectionOrPartitionStatuses(
      fs: FileSystem,
      dirPath: Path
  ): Seq[FileStatus] = {
    try {
      if (!fs.getFileStatus(dirPath).isDirectory) {
        throw new IllegalArgumentException(
          s"Path $dirPath is not a directory."
        )
      }
      fs.listStatus(dirPath)
        .filter(_.isDirectory())
        .filterNot(_.getPath.getName.startsWith("_"))
        .filterNot(_.getPath.getName.startsWith("."))
        .toSeq
    } catch {
      case e: FileNotFoundException =>
        logWarning(s"Path $dirPath not found")
        Seq[FileStatus]()
    }
  }

  /**
   * Build field map from pre-fetched segment log info.
   */
  def getSegmentFieldMapWithLogInfo(
      rootPath: Path,
      segmentLogInfo: MilvusSegmentLogInfo
  ): Seq[Map[String, String]] = {
    buildFieldMapFromLogInfo(rootPath, segmentLogInfo)
  }

  /**
   * Internal method to build field map from segment log info.
   */
  private def buildFieldMapFromLogInfo(
      rootPath: Path,
      segmentLogInfo: MilvusSegmentLogInfo
  ): Seq[Map[String, String]] = {
    val insertLogIDs = segmentLogInfo.insertLogIDs
    val segmentID = segmentLogInfo.segmentID

    if (insertLogIDs.isEmpty) {
      logWarning(s"No insert logs found for segment $segmentID")
      return Seq.empty
    }

    // Build file path map directly from API response (no S3 traversal needed)
    // insertLogIDs format: "fieldID/logID"
    var filePathMap = mutable.Map[String, Seq[String]]()
    insertLogIDs.foreach { logID =>
      val parts = logID.split("/")
      if (parts.length == 2) {
        val fieldID = parts(0)
        val fileName = parts(1)
        if (filePathMap.contains(fieldID)) {
          filePathMap(fieldID) = filePathMap(fieldID) :+ fileName
        } else {
          filePathMap(fieldID) = Seq(fileName)
        }
      }
    }

    if (fieldIDs.nonEmpty) {
      logInfo(
        s"Filtering filePathMap with fieldIDs: $fieldIDs, available fields: ${filePathMap.keys.mkString(", ")}"
      )
      filePathMap = filePathMap.filter(entry => fieldIDs.contains(entry._1))
      logInfo(s"After filtering: ${filePathMap.keys.mkString(", ")}")
    }

    if (filePathMap.isEmpty) {
      logWarning(s"No matching fields found for segment $segmentID")
      return Seq.empty
    }

    // Sort the file names in ascending order for each field ID
    filePathMap.foreach { case (fieldId, fileNames) =>
      filePathMap(fieldId) = fileNames.sorted
    }

    val fieldMaps = filePathMap.head._2.indices.map { i =>
      filePathMap.map { case (fieldId, fileNames) =>
        val fullPath = s"${rootPath.toString()}/${fieldId}/${fileNames(i)}"
        fieldId -> fullPath
      }.toMap
    }.toList
    return fieldMaps
  }

  def getValidSegments(client: MilvusClient): (Seq[String], Seq[String]) = {
    val result = client.getSegments(
      milvusOption.databaseName,
      milvusOption.collectionName
    )
    val allSegments = result
      .getOrElse(
        throw new Exception(
          s"Failed to get segment info: ${result.failed.get.getMessage}"
        )
      )

    // Separate V1 and V2 segments
    val v1Segments = allSegments.filter(_.storageVersion == 0)
    val v2Segments = allSegments.filter(_.storageVersion == 2)

    val v1SegmentIDs = v1Segments.map(_.segmentID.toString)
    val v2SegmentIDs = v2Segments.map(_.segmentID.toString)
    (v1SegmentIDs, v2SegmentIDs)
  }

  def getPartitionInfos(
      client: MilvusClient
  ): Map[String, String] = {
    val result = client.getPartitionInfos(
      milvusOption.databaseName,
      milvusOption.collectionName
    )
    result
      .getOrElse(
        throw new Exception(
          s"Failed to get partition infos: ${result.failed.get.getMessage}"
        )
      )
      .map(partition => {
        partition.partitionID.toString -> partition.partitionName
      })
      .toMap
  }


  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val rootPath = readerOption.getFilePath(pathOption)
    val fs = readerOption.getFileSystem(rootPath)

    // segment path
    val rawPath = options.getOrDefault(MilvusOption.ReaderPath, "")
    val collection = milvusOption.collectionID
    val partition = milvusOption.partitionID
    val segment = milvusOption.segmentID

    val client = MilvusClient(milvusOption)

    // Get both V1 and V2 segments
    var validV1Segments = Seq[String]()
    var validV2Segments = Seq[String]()
    if (segment.isEmpty()) {
      val (v1Segs, v2Segs) = getValidSegments(client)
      validV1Segments = v1Segs
      validV2Segments = v2Segs
    }
    val containExtraPartition =
      milvusOption.extraColumns.contains(
        MilvusOption.MilvusExtraColumnPartition
      )
    var partitionInfos =
      if (containExtraPartition) {
        val infos = getPartitionInfos(client)
        infos
      } else {
        Map[String, String]()
      }
    // Use ConcurrentHashMap for thread-safe parallel processing
    val segment2Partition = new ConcurrentHashMap[String, String]()

    // V1 segments: field maps for binlog reading (thread-safe for parallel processing)
    val fieldMaps = new ConcurrentHashMap[String, Seq[Map[String, String]]]()

    // V2 segments: manifests for FFI reading (thread-safe for parallel processing)
    val v2Manifests = new ConcurrentHashMap[String, (String, String, String)]() // segmentID -> (collectionID, partitionID, manifest)

    // Get collection schema and S3 config for V2 manifest building
    val collectionInfo = client.getCollectionInfo(
      milvusOption.databaseName,
      milvusOption.collectionName
    ).getOrElse(
      throw new Exception(
        s"Collection ${milvusOption.collectionName} not found"
      )
    )
    val s3Bucket = milvusOption.options.getOrElse(Properties.FsConfig.FsBucketName, "a-bucket")
    val s3RootPath = milvusOption.options.getOrElse(Properties.FsConfig.FsRootPath, "files")

    // Batch fetch all V1 segment info in a single API call for better performance
    val v1SegmentLogInfoMap: Map[Long, MilvusSegmentLogInfo] = if (validV1Segments.nonEmpty) {
      val v1SegmentIDsLong = validV1Segments.flatMap { segStr =>
        try { Some(segStr.toLong) } catch { case _: NumberFormatException => None }
      }
      if (v1SegmentIDsLong.nonEmpty) {
        logInfo(s"Batch fetching segment info for ${v1SegmentIDsLong.size} V1 segments")
        client.getSegmentsInfoBatch(collectionInfo.collectionID, v1SegmentIDsLong).getOrElse(
          throw new Exception("Failed to batch fetch segment info")
        )
      } else {
        Map.empty[Long, MilvusSegmentLogInfo]
      }
    } else {
      Map.empty[Long, MilvusSegmentLogInfo]
    }

    if (rawPath.isEmpty) {
      if (!partition.isEmpty() && !segment.isEmpty()) {
        // Check if this segment is V1 or V2
        if (validV1Segments.contains(segment)) {
          val segmentIDLong = segment.toLong
          val segmentLogInfo = v1SegmentLogInfoMap.getOrElse(segmentIDLong,
            throw new Exception(s"Segment $segment not found in batch result"))
          fieldMaps.put(segment, getSegmentFieldMapWithLogInfo(rootPath, segmentLogInfo))
        } else if (validV2Segments.contains(segment)) {
          val manifest = ManifestBuilder.buildManifestForSegment(
            collectionInfo.schema,
            collection,
            partition,
            segment,
            client,
            s3Bucket,
            s3RootPath
          )
          v2Manifests.put(segment, (collection, partition, manifest))
        }
      } else if (!partition.isEmpty()) {
        var segmentStatuses = getCollectionOrPartitionStatuses(fs, rootPath)

        // Process V1 segments in parallel using batch-fetched segment info
        val v1SegmentStatuses = segmentStatuses
          .filter(status => validV1Segments.contains(status.getPath().getName))
        v1SegmentStatuses.par.foreach(status => {
          val segmentID = status.getPath().getName
          val segmentIDLong = segmentID.toLong
          val segmentLogInfo = v1SegmentLogInfoMap.getOrElse(segmentIDLong,
            throw new Exception(s"Segment $segmentID not found in batch result"))
          fieldMaps.put(segmentID, getSegmentFieldMapWithLogInfo(status.getPath(), segmentLogInfo))
          segment2Partition.put(segmentID, partition)
        })

        // Process V2 segments in parallel - use FFI instead of filesystem
        validV2Segments.par.foreach { segmentID =>
          val manifest = ManifestBuilder.buildManifestForSegment(
            collectionInfo.schema,
            collection,
            partition,
            segmentID,
            client,
            s3Bucket,
            s3RootPath
          )
          v2Manifests.put(segmentID, (collection, partition, manifest))
          segment2Partition.put(segmentID, partition)
        }
      } else {
        // For V1 segments, we need filesystem access
        if (validV1Segments.nonEmpty) {
          var partitionStatuses = getCollectionOrPartitionStatuses(fs, rootPath)

          // Collect all V1 segments with their partition info first
          val allV1SegmentInfos = partitionStatuses.flatMap { status =>
            val partitionID = status.getPath().getName
            val segmentStatuses = getCollectionOrPartitionStatuses(fs, status.getPath())
            segmentStatuses
              .filter(s => validV1Segments.contains(s.getPath().getName))
              .map(s => (s, partitionID))
          }

          // Process all V1 segments in parallel using batch-fetched segment info
          allV1SegmentInfos.par.foreach { case (status, partitionID) =>
            val segmentID = status.getPath().getName
            val segmentIDLong = segmentID.toLong
            val segmentLogInfo = v1SegmentLogInfoMap.getOrElse(segmentIDLong,
              throw new Exception(s"Segment $segmentID not found in batch result"))
            fieldMaps.put(segmentID, getSegmentFieldMapWithLogInfo(status.getPath(), segmentLogInfo))
            segment2Partition.put(segmentID, partitionID)
          }
        }

        // For V2 segments, use Milvus API to get partition info
        if (validV2Segments.nonEmpty) {
          // Get all segments with their partition IDs from Milvus API
          val allSegments = client.getSegments(
            milvusOption.databaseName,
            milvusOption.collectionName
          ).getOrElse(
            throw new Exception("Failed to get segments")
          )

          // Build map of segmentID -> partitionID
          val segmentToPartitionMap = allSegments
            .filter(seg => validV2Segments.contains(seg.segmentID.toString))
            .map(seg => seg.segmentID.toString -> seg.partitionID.toString)
            .toMap

          // Process V2 segments in parallel
          validV2Segments.par.foreach { segmentID =>
            val partitionID = segmentToPartitionMap.getOrElse(segmentID, {
              logWarning(s"Could not find partition for V2 segment $segmentID, skipping")
              ""
            })

            if (partitionID.nonEmpty) {
              val manifest = ManifestBuilder.buildManifestForSegment(
                collectionInfo.schema,
                collection,
                partitionID,
                segmentID,
                client,
                s3Bucket,
                s3RootPath
              )
              v2Manifests.put(segmentID, (collection, partitionID, manifest))
              segment2Partition.put(segmentID, partitionID)
            }
          }
        }
      }
    } else {
      // Raw path specified - assume V1 for backward compatibility
      val segmentName = rootPath.getName()
      val segmentIDLong = segmentName.toLong
      val segmentLogInfo = v1SegmentLogInfoMap.getOrElse(segmentIDLong,
        throw new Exception(s"Segment $segmentName not found in batch result"))
      fieldMaps.put(segmentName, getSegmentFieldMapWithLogInfo(rootPath, segmentLogInfo))
    }

    // Create V1 input partitions (convert ConcurrentHashMap to Scala Map)
    val v1Partitions = fieldMaps.asScala.map { case (segment, fieldMap) =>
      val partitionName = if (containExtraPartition)
        partitionInfos.getOrElse(
          segment2Partition.asScala.getOrElse(segment, "unknown"),
          "unknown"
        )
      else ""
      // Try to parse segment as Long, if it fails use -1
      val segmentIDLong = try {
        segment.toLong
      } catch {
        case _: NumberFormatException =>
          logWarning(s"Failed to parse segment '$segment' as Long, using -1")
          -1L
      }
      MilvusInputPartition(
        fieldMap,
        partitionName,
        segmentID = segmentIDLong  // Pass segment ID for tracking
      ): InputPartition
    }

    // Create V2 input partitions (convert ConcurrentHashMap to Scala Map)
    val v2Partitions = v2Manifests.asScala.map { case (segmentID, (collectionID, partitionID, manifest)) =>
      // Parse segmentID string to Long, default to -1 if parsing fails
      val segmentIDLong = try {
        segmentID.toLong
      } catch {
        case _: NumberFormatException => -1L
      }
      MilvusStorageV2InputPartition(
        manifest,
        collectionInfo.schema.toByteArray,
        partitionID,
        milvusOption,
        vectorSearchConfig.map(_.topK),
        vectorSearchConfig.map(_.queryVector),
        vectorSearchConfig.map(_.metricType),
        vectorSearchConfig.map(_.vectorColumn),
        segmentIDLong  // Pass segment ID
      ): InputPartition
    }

    val result = (v1Partitions ++ v2Partitions).toArray

    logInfo(s"Created ${v1Partitions.size} V1 partitions and ${v2Partitions.size} V2 partitions")

    fs.close()
    client.close()
    result
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Convert CaseInsensitiveStringMap to regular Map for serialization
    val optionsMap = options.asScala.toMap
    new MilvusPartitionReaderFactory(schema, optionsMap, pushedFilters)
  }
}

