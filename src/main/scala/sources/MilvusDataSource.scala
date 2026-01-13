package com.zilliz.spark.connector.sources

import java.{util => ju}
import java.util.{HashMap, Map => JMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
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
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{
  DataTypes => SparkDataTypes,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.{
  DataTypeUtil,
  MilvusClient,
  MilvusCollectionInfo,
  MilvusOption,
  VectorSearchConfig
}
import com.zilliz.spark.connector.read.{
  MilvusPartitionReaderFactory,
  MilvusStorageV2InputPartition
}
import com.zilliz.spark.connector.write.{MilvusWrite, MilvusWriteBuilder}
import com.zilliz.spark.connector.loon.{
  ManifestBuilder,
  Properties
}
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

  // Get vector search configuration from MilvusOption
  private val vectorSearchConfig = milvusOption.vectorSearchConfig

  // Log vector search configuration if enabled
  vectorSearchConfig.foreach { config =>
    logInfo(s"Vector search enabled: topK=${config.topK}, metric=${config.metricType}, column=${config.vectorColumn}")
  }

  override def readSchema(): StructType = {
    schema
  }

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // Validate required parameters
    if (milvusOption.collectionName.isEmpty) {
      throw new IllegalArgumentException("collectionName cannot be empty")
    }

    val collection = milvusOption.collectionID
    val partition = milvusOption.partitionID
    val segment = milvusOption.segmentID

    val client = MilvusClient(milvusOption)

    // Get collection schema and S3 config for manifest building
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

    // Helper function to create InputPartition from segment info
    def createPartition(segmentID: String, partitionID: String): InputPartition = {
      val manifest = ManifestBuilder.buildManifestForSegment(
        collectionInfo.schema,
        collection,
        partitionID,
        segmentID,
        client,
        s3Bucket,
        s3RootPath
      )
      val segmentIDLong = try { segmentID.toLong } catch { case _: NumberFormatException => -1L }
      MilvusStorageV2InputPartition(
        manifest,
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

    // Get all V2 segments with partition info
    val allV2Segments = client.getSegments(
      milvusOption.databaseName,
      milvusOption.collectionName
    ).getOrElse(
      throw new Exception("Failed to get segments")
    ).filter(_.storageVersion == 2)

    if (allV2Segments.isEmpty) {
      throw new IllegalArgumentException(
        s"No Storage V2 segments found in collection ${milvusOption.collectionName}. " +
        "This connector requires Milvus 2.6+ with Storage V2. " +
        "Please ensure the collection has been flushed and contains data."
      )
    }

    val partitions: Array[InputPartition] = if (!partition.isEmpty() && !segment.isEmpty()) {
      // Case 1: Specific segment specified - validate segment belongs to partition
      val segmentInfo = allV2Segments.find(_.segmentID.toString == segment)
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
            s"Segment $segment not found or not a V2 segment (Storage V2 required)"
          )
      }

    } else if (!partition.isEmpty()) {
      // Case 2: Partition specified - only process V2 segments in this partition
      allV2Segments
        .filter(_.partitionID.toString == partition)
        .map(seg => createPartition(seg.segmentID.toString, partition))
        .toArray

    } else {
      // Case 3: No partition specified - process all V2 segments
      allV2Segments.map { seg =>
        createPartition(seg.segmentID.toString, seg.partitionID.toString)
      }.toArray
    }

    logInfo(s"Created ${partitions.length} partitions for Storage V2 (Milvus 2.6+)")
    client.close()
    partitions
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Convert CaseInsensitiveStringMap to regular Map for serialization
    val optionsMap = options.asScala.toMap
    new MilvusPartitionReaderFactory(schema, optionsMap, pushedFilters)
  }
}
