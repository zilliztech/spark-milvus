package com.zilliz.spark.connector.sources

import java.{util => ju}
import java.io.FileNotFoundException
import java.util.{Collections, HashMap, Map => JMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  SupportsWrite,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  Scan,
  ScanBuilder
}
import org.apache.spark.sql.connector.read.{
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriterFactory,
  PhysicalWriteInfo,
  WriterCommitMessage
}
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  LogicalWriteInfo,
  Write,
  WriteBuilder
}
import org.apache.spark.sql.connector.write.{
  DataWriter,
  DataWriterFactory,
  WriterCommitMessage
}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.Filter
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
  MilvusS3Option,
  MilvusSegmentInfo
}
import com.zilliz.spark.connector.MilvusPartitionInfo

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

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    MilvusWriteBuilder(milvusOption, info)

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
    if (fieldIDs.isEmpty || fieldIDs.contains("0"))
      fields = fields :+ StructField("row_id", LongType, false)
    if (fieldIDs.isEmpty || fieldIDs.contains("1"))
      fields = fields :+ StructField("timestamp", LongType, false)
    fields = fields ++ milvusCollection.schema.fields
      .filter(field =>
        fieldIDs.isEmpty || fieldIDs.contains(fieldName2ID(field.name).toString)
      )
      .map(field =>
        StructField(
          field.name,
          DataTypeUtil.toDataType(field),
          field.nullable
        )
      )
    val maxFieldID = fieldName2ID.values.max
    if (
      milvusCollection.schema.enableDynamicField &&
      (fieldIDs.isEmpty || fieldIDs.contains((maxFieldID + 1).toString))
    )
      fields = fields :+ StructField("$meta", StringType, true)
    if (
      milvusOption.extraColumns.contains(
        MilvusOption.MilvusExtraColumnPartition
      )
    )
      fields = fields :+ StructField("partition", StringType, true)
    StructType(fields)
  }

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_WRITE,
      TableCapability.BATCH_READ
    ).asJava
  }
}

// 3. WriteBuilder and ScanBuilder
case class MilvusWriteBuilder(
    milvusOptions: MilvusOption,
    info: LogicalWriteInfo
) extends WriteBuilder
    with Serializable {
  override def build: Write = MilvusWrite(milvusOptions, info.schema())
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
      tmpMap.put(
        MilvusOption.ReaderFieldIDs,
        fieldNames
          .map(fieldName => fieldName2ID(fieldName).toString)
          .mkString(",")
      )
    }
    if (
      extraColumns.contains(MilvusOption.MilvusExtraColumnPartition) &&
      !fieldNames.contains("partition")
    ) {
      fieldNames = fieldNames :+ "partition"
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

  override def build(): Scan =
    new MilvusScan(currentSchema, currentOptions, pushedFilterArray)
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
      options
        .get(MilvusOption.ReaderFieldIDs)
        .split(",")
        .toSeq
        .filter(_.nonEmpty)
    } else {
      Seq[String]()
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

  def getSegmentFieldMap(
      fs: FileSystem,
      client: MilvusClient,
      rootPath: Path
  ): Seq[Map[String, String]] = {
    val paths = rootPath.toString().split("/")
    val segmentID = paths(paths.length - 1).toLong
    val collectionID = paths(paths.length - 3).toLong
    val result = client.getSegmentInfo(collectionID, segmentID)
    if (result.isFailure) {
      throw new IllegalArgumentException(
        s"Failed to get segment info: ${result.failed.get.getMessage}"
      )
    }
    val insertLogIDs = result.get.insertLogIDs

    val fileStatuses = if (fs.getFileStatus(rootPath).isDirectory) {
      try {
        val fieldDirStatuses = fs
          .listStatus(rootPath)
          .filterNot(_.getPath.getName.startsWith("_"))
          .filterNot(_.getPath.getName.startsWith(".")) // Ignore hidden files
        fieldDirStatuses
          .flatMap(fieldDirStatus => {
            val fieldPath = fieldDirStatus.getPath()
            if (fs.getFileStatus(fieldPath).isDirectory) {
              val deepFileStatuses = fs
                .listStatus(fieldPath)
                .filterNot(_.getPath.getName.startsWith("_"))
                .filterNot(
                  _.getPath.getName.startsWith(".")
                ) // Ignore hidden files
              deepFileStatuses
            } else {
              throw new IllegalArgumentException(
                s"fieldPath is not a directory: $fieldPath"
              )
            }
          })
          .toSeq
      } catch {
        case e: FileNotFoundException =>
          logWarning(s"Path $rootPath not found")
          Seq[FileStatus]()
      }
    } else {
      // Array(fs.getFileStatus(rootPath))
      throw new IllegalArgumentException(
        s"rootPath is not a directory: $rootPath"
      )
    }

    var filePathMap = mutable.Map[String, Seq[String]]()
    fileStatuses.foreach(status => {
      val filePath = status.getPath.toString
      val paths = filePath.split("/")
      val fileName = paths(paths.length - 1)
      val filedID = paths(paths.length - 2)
      if (insertLogIDs.contains(s"${filedID}/${fileName}")) {
        if (filePathMap.contains(filedID)) {
          filePathMap(filedID) = filePathMap(filedID) :+ fileName
        } else {
          filePathMap(filedID) = Seq(fileName)
        }
      }
    })

    if (fieldIDs.nonEmpty) {
      logInfo(
        s"Filtering filePathMap with fieldIDs: $fieldIDs, available fields: ${filePathMap.keys.mkString(", ")}"
      )
      filePathMap = filePathMap.filter(entry => fieldIDs.contains(entry._1))
      logInfo(s"After filtering: ${filePathMap.keys.mkString(", ")}")
    }

    // Sort the file names in ascending order for each field ID
    filePathMap.foreach { case (fieldId, fileNames) =>
      filePathMap(fieldId) = fileNames.sorted
    }

    val fieldMaps = filePathMap.head._2.indices.map { i =>
      filePathMap.map { case (fieldId, fileNames) =>
        val fullPath = s"${rootPath.toString()}/${fieldId}/${fileNames(i)}"
        // logInfo(s"field file fullPath: $fullPath")
        fieldId -> fullPath
      }.toMap
    }.toList
    return fieldMaps
  }

  def getValidSegments(client: MilvusClient): Seq[String] = {
    val result = client.getSegments(
      milvusOption.databaseName,
      milvusOption.collectionName
    )
    result
      .getOrElse(
        throw new Exception(
          s"Failed to get segment info: ${result.failed.get.getMessage}"
        )
      )
      .map(_.segmentID.toString)
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

    var validSegments = Seq[String]()
    if (segment.isEmpty()) {
      validSegments = getValidSegments(client)
    }
    val containExtraPartition =
      milvusOption.extraColumns.contains(
        MilvusOption.MilvusExtraColumnPartition
      )
    var partitionInfos =
      if (containExtraPartition) {
        getPartitionInfos(client)
      } else {
        Map[String, String]()
      }
    var segment2Partition = mutable.Map[String, String]()

    var fieldMaps = mutable.Map[String, Seq[Map[String, String]]]()
    if (rawPath.isEmpty) {
      if (!partition.isEmpty() && !segment.isEmpty()) {
        fieldMaps(segment) = getSegmentFieldMap(fs, client, rootPath)
      } else if (!partition.isEmpty()) {
        var segmentStatuses = getCollectionOrPartitionStatuses(fs, rootPath)
        segmentStatuses
          .filter(status => validSegments.contains(status.getPath().getName))
          .foreach(status => {
            fieldMaps(status.getPath().getName) =
              getSegmentFieldMap(fs, client, status.getPath())
          })
      } else {
        var partitionStatuses = getCollectionOrPartitionStatuses(fs, rootPath)
        partitionStatuses.foreach(status => {
          val partitionID = status.getPath().getName
          val segmentStatuses =
            getCollectionOrPartitionStatuses(fs, status.getPath())
          segmentStatuses
            .filter(status => validSegments.contains(status.getPath().getName))
            .foreach(status => {
              val segmentID = status.getPath().getName
              fieldMaps(segmentID) = getSegmentFieldMap(
                fs,
                client,
                status.getPath()
              )
              segment2Partition(
                segmentID
              ) = partitionID
            })
        })
      }
    } else {
      fieldMaps(rootPath.getName()) = getSegmentFieldMap(fs, client, rootPath)
    }

    val result = fieldMaps.map { case (segment, fieldMap) =>
      MilvusInputPartition(
        fieldMap,
        if (containExtraPartition)
          partitionInfos.getOrElse(
            segment2Partition.getOrElse(segment, "unknown"),
            "unknown"
          )
        else ""
      ): InputPartition
    }.toArray
    fs.close()
    client.close()
    result
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new MilvusReaderFactory(schema, options, pushedFilters)
  }
}

// 4. Write
case class MilvusWrite(milvusOptions: MilvusOption, schema: StructType)
    extends Write
    with Serializable {
  override def toBatch: BatchWrite = MilvusBatchWriter(milvusOptions, schema)
}

case class MilvusBatchWriter(milvusOptions: MilvusOption, schema: StructType)
    extends BatchWrite {
  override def createBatchWriterFactory(
      info: PhysicalWriteInfo
  ): DataWriterFactory = {
    MilvusDataWriterFactory(milvusOptions, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

case class MilvusDataWriterFactory(
    milvusOptions: MilvusOption,
    schema: StructType
) extends DataWriterFactory
    with Serializable {
  override def createWriter(
      partitionId: Int,
      taskId: Long
  ): DataWriter[InternalRow] = {
    MilvusInsertDataWriter(partitionId, taskId, milvusOptions, schema)
  }
}

case class MilvusCommitMessage(rowCount: Int) extends WriterCommitMessage

case class MilvusInputPartition(
    fieldFiles: Seq[Map[String, String]],
    partition: String = ""
) extends InputPartition

class MilvusReaderFactory(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends PartitionReaderFactory {

  private val readerOptions = MilvusS3Option(options)

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val milvusPartition = partition.asInstanceOf[MilvusInputPartition]
    // Create the data reader with the file map, schema, and options
    new MilvusPartitionReader(
      schema,
      milvusPartition.fieldFiles,
      milvusPartition.partition,
      readerOptions,
      pushedFilters
    )
  }
}
