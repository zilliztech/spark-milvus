package com.zilliz.spark.connector.sources

import java.io.InputStream
import java.util.concurrent.{
  CompletableFuture,
  ConcurrentHashMap,
  Executors,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks._

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{
  ArrayType,
  DataTypes => SparkDataTypes,
  StringType,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.spark.connector.binlog.{
  Constants,
  DeleteEventData,
  DescriptorEvent,
  DescriptorEventData,
  LogReader
}
import com.zilliz.spark.connector.binlog.InsertEventData
import com.zilliz.spark.connector.MilvusS3Option
import io.milvus.grpc.schema.{DataType => MilvusDataType}

class MilvusPartitionReader(
    schema: StructType,
    fieldFilesSeq: Seq[Map[String, String]],
    partition: String,
    options: MilvusS3Option,
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends PartitionReader[InternalRow]
    with Logging {

  @volatile private var fieldFileReaders: Map[String, FieldFileReader] =
    Map.empty
  private val fieldReadersLock = new Object()

  private var currentFieldFilesIndex: Int = 0 // Index for current reading

  private val preloadedBatches
      : concurrent.Map[Int, Map[String, FieldFileReader]] =
    new ConcurrentHashMap[Int, Map[String, FieldFileReader]]().asScala

  private val preloadFutures
      : concurrent.Map[Int, Future[Map[String, FieldFileReader]]] =
    new ConcurrentHashMap[Int, Future[Map[String, FieldFileReader]]]().asScala

  private val nextPreloadIndex: AtomicInteger = new AtomicInteger(0)

  private val preloadExecutor =
    Executors.newFixedThreadPool(
      math.min(options.s3PreloadPoolSize, fieldFilesSeq.length)
    )
  private implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(preloadExecutor)

  // Background preloading task
  private var backgroundPreloadTask: Option[Future[Unit]] = None
  private var stopBackgroundPreload: Boolean = false

  open()

  // Helper trait/class to abstract reading a single field file
  // This encapsulates the logic from MilvusBinlogPartitionReader
  private trait FieldFileReader {
    def open(): Unit // Open the file and prepare for reading
    def hasNext(): Boolean // Check if there are more records in this file
    def readNextRecord()
        : Any // Read and parse the next record, return the single field value
    def getDataType(): MilvusDataType // Get the data type of the field
    def moveToNextRecord(): Unit // Move to the next record
    def close(): Unit // Close the file stream
    def isNullOnly(): Boolean // Check if this binlog contains only null values
  }

  private class MilvusBinlogFieldFileReader(
      filePath: String,
      options: MilvusS3Option
  ) extends FieldFileReader
      with Logging {

    private val path = options.getFilePath(filePath)
    private var inputStream: InputStream = _
    private var fileSystem: FileSystem = _

    // Binlog specific state (adapt from MilvusBinlogPartitionReader)
    private val objectMapper =
      LogReader.getObjectMapper() // Assuming object mapper is reusable
    private var descriptorEvent: DescriptorEvent = _
    private var dataType: MilvusDataType = _ // Data type from descriptor event
    private var deleteEvent: DeleteEventData = null
    private var insertEvent: InsertEventData = null
    private var currentIndex: Int =
      0 // Index within the current event's data array
    private var currentReaderType: String =
      _ // Need to know if this file is insert or delete type

    // Track if this binlog contains only null values (dataSize == 0)
    // In this case, we need to sync with the expected row count from other binlogs
    private var isNullOnlyBinlog: Boolean = false
    private var expectedRowCount: Int = -1 // Expected total row count for this binlog
    private var virtualNullIndex: Int = 0 // Virtual index for null-only binlogs

    // You might need to pass the expected reader type (insert/delete) for this field file
    // Or infer it from the file content/name if possible
    // For simplicity, assuming insert type for now, you need to handle delete as well
    // This could be passed via options or determined during scan planning
    currentReaderType =
      Constants.LogReaderTypeInsert // Placeholder - determine actual type

    override def open(): Unit = {
      try {
        // Each reader creates a separate FileSystem
        fileSystem = options.getFileSystem(path)
        inputStream = fileSystem.open(path)
        // Read descriptor event to get data type
        descriptorEvent = LogReader.readDescriptorEvent(inputStream)
        dataType = descriptorEvent.data.payloadDataType
        hasNext()
        logInfo(s"Successfully opened field file: $filePath")
      } catch {
        case e: Exception =>
          logError(s"Failed to open field file $filePath: ${e.getMessage}", e)
          throw e
      }
    }

    override def getDataType(): MilvusDataType = dataType

    override def isNullOnly(): Boolean = isNullOnlyBinlog

    override def hasNext(): Boolean = {
      currentReaderType match {
        case Constants.LogReaderTypeInsert => hasNextInsertEvent()
        case Constants.LogReaderTypeDelete => hasNextDeleteEvent()
        case _ =>
          throw new IllegalStateException(
            s"Unknown reader type for file $filePath: $currentReaderType"
          )
      }
    }

    private def hasNextInsertEvent(): Boolean = {
      // Check if we need to read the next event
      if (insertEvent != null && currentIndex >= insertEvent.getDataSize()) {
        insertEvent = null
        currentIndex = 0
      }
      if (insertEvent == null) {
        insertEvent =
          LogReader.readInsertEvent(inputStream, objectMapper, dataType)
      }

      // Check if this is a null-only binlog (all values are null, dataSize == 0)
      if (insertEvent != null && insertEvent.getDataSize() == 0) {
        // This binlog has no actual data, meaning all values are null
        // We cannot determine hasNext on our own - we need to sync with other binlogs
        // Mark this as a null-only binlog and return false
        // The validation logic will skip this reader when checking alignment
        isNullOnlyBinlog = true
        return false // No actual data to read
      }

      // Normal case: check if we have actual data
      insertEvent != null && currentIndex < insertEvent.getDataSize()
    }

    private def hasNextDeleteEvent(): Boolean = {
      if (deleteEvent != null && currentIndex >= deleteEvent.pks.length) {
        deleteEvent = null
        currentIndex = 0
      }
      if (deleteEvent == null) {
        deleteEvent =
          LogReader.readDeleteEvent(inputStream, objectMapper, dataType)
      }

      // Ensure we have a valid event and currentIndex is within bounds
      deleteEvent != null && currentIndex < deleteEvent.pks.length
    }

    override def readNextRecord(): Any = {
      currentReaderType match {
        case Constants.LogReaderTypeInsert => readNextInsertRecord()
        case Constants.LogReaderTypeDelete => readNextDeleteRecord()
        case _ =>
          throw new IllegalStateException(
            s"Unknown reader type for file $filePath: $currentReaderType"
          )
      }
    }

    override def moveToNextRecord(): Unit = {
      // For null-only binlogs, we don't increment the actual index
      // since there's no real data - we just track that we've "read" a virtual null
      if (isNullOnlyBinlog) {
        virtualNullIndex += 1
      } else {
        currentIndex += 1
      }
    }

    private def readNextInsertRecord(): Any = {
      // If this is a null-only binlog, always return null
      if (isNullOnlyBinlog) {
        return null
      }

      if (insertEvent == null) {
        // This should not happen if hasNext() was called correctly
        throw new IllegalStateException(
          s"Attempted to read from null insert event in file $filePath"
        )
      }

      // Additional boundary check to prevent ArrayIndexOutOfBoundsException
      if (currentIndex >= insertEvent.getDataSize()) {
        throw new IllegalStateException(
          s"Index out of bounds: currentIndex=$currentIndex, dataSize=${insertEvent
              .getDataSize()} in file $filePath"
        )
      }

      val data = insertEvent.getData(currentIndex)
      data
    }

    private def readNextDeleteRecord(): String = {
      if (deleteEvent == null) {
        // This should not happen if hasNext() was called correctly
        throw new IllegalStateException(
          s"Attempted to read from null delete event in file $filePath"
        )
      }

      // Additional boundary check to prevent ArrayIndexOutOfBoundsException
      if (currentIndex >= deleteEvent.pks.length) {
        throw new IllegalStateException(
          s"Index out of bounds: currentIndex=$currentIndex, pks.length=${deleteEvent.pks.length} in file $filePath"
        )
      }

      val pk = deleteEvent.pks(currentIndex)
      pk
    }

    override def close(): Unit = {
      if (inputStream != null) {
        try {
          inputStream.close()
        } catch {
          case e: Exception =>
            logWarning(s"Error closing input stream for $filePath", e)
        }
        inputStream = null
      }

      // 关闭独立的 FileSystem
      if (fileSystem != null) {
        try {
          fileSystem.close()
        } catch {
          case e: Exception =>
            logWarning(s"Error closing FileSystem for $filePath", e)
        }
        fileSystem = null
      }
    }
  }

  // Open all field files when the reader is initialized
  // Note: Spark calls open() when the reader is ready to start reading
  def open(): Unit = {
    if (fieldFilesSeq.isEmpty) {
      throw new IllegalStateException("No field files provided in the sequence")
    }

    // Start background preloading task for all files
    startBackgroundPreloading()

    // Log file statistics for debugging input size issues
    val totalFiles = fieldFilesSeq.map(_.size).sum
    val totalBatches = fieldFilesSeq.length
    logInfo(
      s"Opened MilvusPartitionReader with $totalBatches batches, $totalFiles total files"
    )
    logInfo("Background preloading started for all batches.")
  }

  // Background preloading task that continuously submits preload jobs
  private def startBackgroundPreloading(): Unit = {
    backgroundPreloadTask = Some(Future {
      while (
        !stopBackgroundPreload && nextPreloadIndex.get() < fieldFilesSeq.length
      ) {
        val currentIndex = nextPreloadIndex.get()

        // Check if we already have a future for this batch
        if (!preloadFutures.contains(currentIndex)) {
          val batchIndex = currentIndex
          val future = Future {
            try {
              val batchFieldFiles = fieldFilesSeq(batchIndex)
              val batchReaders =
                batchFieldFiles.map { case (fieldName, filePath) =>
                  val reader = new MilvusBinlogFieldFileReader(
                    filePath,
                    options
                  )
                  reader.open()
                  fieldName -> reader
                }
              logInfo(
                s"Background preloaded batch $batchIndex with ${batchReaders.size} field readers"
              )
              batchReaders
            } catch {
              case e: Exception =>
                logWarning(
                  s"Background preload failed for batch $batchIndex: ${e.getMessage}",
                  e
                )
                Map.empty[String, FieldFileReader]
            }
          }

          // 原子性地添加future
          preloadFutures.putIfAbsent(batchIndex, future)

          // 异步处理完成的futures
          future.onComplete {
            case Success(readers) if readers.nonEmpty =>
              preloadedBatches.put(batchIndex, readers)
              preloadFutures.remove(batchIndex)
              logInfo(
                s"Moved completed preload for batch $batchIndex to preloadedBatches"
              )
            case Success(_) =>
              preloadFutures.remove(batchIndex)
              logWarning(
                s"Preload for batch $batchIndex completed but was empty"
              )
            case Failure(e) =>
              preloadFutures.remove(batchIndex)
              logWarning(
                s"Preload for batch $batchIndex failed: ${e.getMessage}"
              )
          }
        }

        nextPreloadIndex.incrementAndGet()
      }

      logInfo("Background preloading task completed for all batches")
    })
  }

  private def closeCurrentReaders(): Unit = {
    val readersToClose = fieldReadersLock.synchronized {
      val readers = fieldFileReaders
      fieldFileReaders = Map.empty
      readers
    }

    readersToClose.values.foreach { reader =>
      try {
        reader.close()
      } catch {
        case e: Exception =>
          logWarning(s"Error closing field reader", e)
      }
    }
  }

  // Check if there is a next row by checking if all field readers have a next record
  override def next(): Boolean = {
    // Check if we've reached the end of all batches
    if (currentFieldFilesIndex >= fieldFilesSeq.length) {
      return false
    }

    // Check if we need to load the current batch
    val currentReaders = fieldReadersLock.synchronized {
      fieldFileReaders
    }

    if (currentReaders.isEmpty) {
      // Wait for the current batch to be preloaded
      waitForBatchToBePreloaded(currentFieldFilesIndex)
    }

    // Get the updated readers after potential loading
    val updatedReaders = fieldReadersLock.synchronized {
      fieldFileReaders
    }

    // Check if any reader is not initialized (shouldn't happen after open)
    if (updatedReaders.isEmpty) {
      logWarning(
        "MilvusDataReader.next() called before open() or with empty readers."
      )
      return false
    }

    var hasNext = false
    do {
      // Get current readers safely
      val currentFieldReaders = fieldReadersLock.synchronized {
        fieldFileReaders
      }

      // Separate readers into null-only and normal readers
      val (nullOnlyReaders, normalReaders) = currentFieldReaders.partition {
        case (_, reader) => reader.isNullOnly()
      }

      // Check if ALL normal (non-null-only) field readers have a next record
      // Null-only readers don't have actual data, so we ignore them in the check
      val normalHaveNext = if (normalReaders.nonEmpty) {
        normalReaders.values.forall(_.hasNext())
      } else {
        // If all readers are null-only, we have no more data
        false
      }

      // Consistency check: If some normal readers have next and some don't, it's an alignment error
      // We only check normal readers, not null-only ones
      if (!normalHaveNext && normalReaders.values.exists(_.hasNext())) {
        val status = currentFieldReaders
          .map { case (name, reader) =>
            s"$name: hasNext=${reader.hasNext()}, isNullOnly=${reader.isNullOnly()}"
          }
          .mkString(", ")
        throw new IllegalStateException(
          s"Record count mismatch between field files in partition. Status: $status"
        )
      }

      val allHaveNext = normalHaveNext

      hasNext = allHaveNext

      // If we have a next record, check if it passes the filters
      if (hasNext) {
        val row = buildCurrentRow()
        if (applyFilters(row)) {
          return true // Found a row that passes the filters
        }
        // If the row doesn't pass the filters, move to next record and continue
        currentFieldReaders.values.foreach { reader =>
          reader.moveToNextRecord()
        }
      } else {
        // If no more records in current field files, try next set if available
        if (currentFieldFilesIndex < fieldFilesSeq.length - 1) {
          currentFieldFilesIndex += 1
          switchToNextBatch()
          return next() // Recursively try with next set of field files
        }
      }
    } while (hasNext)

    false // No more rows or no rows that pass the filters
  }

  private def switchToNextBatch(): Unit = {
    // Check if we've reached the end of all batches
    if (currentFieldFilesIndex >= fieldFilesSeq.length) {
      return
    }

    // Close current readers
    closeCurrentReaders()

    // Wait for the next batch to be preloaded
    waitForBatchToBePreloaded(currentFieldFilesIndex)
  }

  // Wait for a specific batch to be preloaded
  private def waitForBatchToBePreloaded(batchIndex: Int): Unit = {
    // Check if batchIndex is valid
    if (batchIndex >= fieldFilesSeq.length) {
      throw new IllegalStateException(
        s"Batch index $batchIndex is out of bounds (max: ${fieldFilesSeq.length - 1})"
      )
    }

    val maxRetries = 50
    val retryIntervalMs = 100L
    var retryCount = 0

    while (retryCount < maxRetries) {
      // First check if it's already in preloadedBatches
      preloadedBatches.get(batchIndex) match {
        case Some(preloadedReaders) if preloadedReaders.nonEmpty =>
          fieldReadersLock.synchronized {
            fieldFileReaders = preloadedReaders
          }
          preloadedBatches.remove(batchIndex)
          logInfo(s"Loaded batch $batchIndex from preloaded batches")
          return

        case _ =>
          // Check if there's a future for this batch
          preloadFutures.get(batchIndex) match {
            case Some(future) =>
              // Wait for the future to complete with infinite timeout and periodic warnings
              logInfo(s"Waiting for batch $batchIndex to be preloaded...")
              try {
                val preloadedReaders =
                  waitForFutureWithWarnings(future, batchIndex)
                preloadFutures.remove(batchIndex)
                if (preloadedReaders.nonEmpty) {
                  fieldReadersLock.synchronized {
                    fieldFileReaders = preloadedReaders
                  }
                  logInfo(
                    s"Successfully loaded batch $batchIndex after waiting"
                  )
                  return
                } else {
                  throw new IllegalStateException(
                    s"Batch $batchIndex preload returned empty readers"
                  )
                }
              } catch {
                case e: Exception =>
                  throw new IllegalStateException(
                    s"Batch $batchIndex preload failed: ${e.getMessage}"
                  )
              }
            case None =>
              // Future not found yet, wait and retry
              if (retryCount % 5 == 0) {
                logInfo(
                  s"Waiting for preload future to be created for batch $batchIndex..."
                )
              }
              Thread.sleep(retryIntervalMs)
              retryCount += 1
          }
      }
    }

    throw new IllegalStateException(
      s"No preload task found for batch $batchIndex after $maxRetries retries. " +
        s"Available futures: ${preloadFutures.keys.toSeq.sorted}, " +
        s"Available preloaded batches: ${preloadedBatches.keys.toSeq.sorted}"
    )
  }

  // Wait for future with infinite timeout and periodic warnings
  private def waitForFutureWithWarnings[T](
      future: Future[T],
      batchIndex: Int
  ): T = {
    val startTime = System.currentTimeMillis()
    val initialSleepTime = 500L // Start with 500ms
    val maxSleepTime = 5000L // Max sleep time of 5 seconds
    val backoffMultiplier = 2 // Multiply sleep time by this factor
    var currentSleepTime = initialSleepTime
    var lastWarningTime = 0L
    val warningInterval = 30000L // 30 seconds warning interval

    while (!future.isCompleted) {
      val elapsedTime = System.currentTimeMillis() - startTime

      // Output warning every 30 seconds
      if (elapsedTime - lastWarningTime >= warningInterval) {
        logWarning(
          s"Still waiting for batch $batchIndex to be preloaded after ${elapsedTime / 1000} seconds..."
        )
        lastWarningTime = elapsedTime
      }

      // Sleep with backoff strategy
      Thread.sleep(currentSleepTime)

      // Increase sleep time with backoff, but cap at maxSleepTime
      currentSleepTime =
        Math.min((currentSleepTime * backoffMultiplier).toLong, maxSleepTime)
    }

    // Now that future is completed, get the result
    future.value match {
      case Some(Success(result))    => result.asInstanceOf[T]
      case Some(Failure(exception)) => throw exception
      case None =>
        throw new IllegalStateException(
          s"Future for batch $batchIndex completed but has no value"
        )
    }
  }

  // Get the current row by reading one record from each field reader
  override def get(): InternalRow = {
    val currentReaders = fieldReadersLock.synchronized {
      fieldFileReaders
    }

    if (currentReaders.isEmpty) {
      throw new IllegalStateException(
        "MilvusDataReader.get() called before open() or with empty readers."
      )
    }

    // Determine the row size - add 1 extra column if partition is not empty
    val rowSize = if (partition != null && partition.nonEmpty) {
      currentReaders.size + 1
    } else {
      currentReaders.size
    }

    // Create a new InternalRow with the correct number of fields
    val row = InternalRow.fromSeq(Seq.fill(rowSize)(null))

    // Read one record from each field reader and set the corresponding field in the row
    // Ensure the order matches the schema's field order
    // Convert field names to integers and sort them
    val sortedFieldIndices = currentReaders.keys
      .map(fieldName => fieldName.toInt)
      .toSeq
      .sorted
      .zipWithIndex
    sortedFieldIndices.foreach { case (fieldID, index) =>
      currentReaders.get(fieldID.toString) match {
        case Some(reader) =>
          try {
            val fieldValue = reader.readNextRecord()
            reader.moveToNextRecord()
            setInternalRowValue(
              row,
              index,
              fieldValue,
              reader.getDataType()
            )
          } catch {
            case e: Exception =>
              logError(
                s"Error reading record for field '$fieldID' from file ${fieldFilesSeq(currentFieldFilesIndex)
                    .getOrElse(fieldID.toString, "N/A")}",
                e
              )
              // Handle error: set null, throw exception, or skip row
              row.setNullAt(index) // Example: set null on error
          }
        case None =>
          // This should not happen if planInputPartitions and open were correct
          logError(s"No reader found for schema field: $fieldID")
          row.setNullAt(index) // Set null if reader is missing
      }
    }

    // Add partition column value if partition is not empty
    if (partition != null && partition.nonEmpty) {
      row.update(currentReaders.size, UTF8String.fromString(partition))
    }

    row // Return the populated row
  }

  // Helper function to set value in InternalRow with type conversion
  // This needs to handle different Spark SQL data types
  private def setInternalRowValue(
      row: InternalRow,
      ordinal: Int,
      inputValue: Any,
      dataType: MilvusDataType
  ): Unit = {
    if (inputValue == null) {
      row.setNullAt(ordinal)
      return
    }
    val sparkFieldType = schema.fields(ordinal).dataType
    dataType match {
      case MilvusDataType.String | MilvusDataType.VarChar |
          MilvusDataType.JSON =>
        row.update(ordinal, UTF8String.fromString(inputValue.toString))
      case MilvusDataType.Int64 =>
        row.setLong(ordinal, inputValue.asInstanceOf[Long])
      case MilvusDataType.Int32 =>
        row.setInt(ordinal, inputValue.asInstanceOf[Int])
      case MilvusDataType.Int16 =>
        row.setShort(ordinal, inputValue.asInstanceOf[Short])
      case MilvusDataType.Int8 =>
        row.setByte(ordinal, inputValue.asInstanceOf[Byte])
      case MilvusDataType.Float =>
        row.setFloat(ordinal, inputValue.asInstanceOf[Float])
      case MilvusDataType.Double =>
        row.setDouble(ordinal, inputValue.asInstanceOf[Double])
      case MilvusDataType.Bool =>
        row.setBoolean(ordinal, inputValue.asInstanceOf[Boolean])
      case MilvusDataType.FloatVector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Float]])
        )
      case MilvusDataType.Float16Vector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Float]])
        )
      case MilvusDataType.BFloat16Vector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Float]])
        )
      case MilvusDataType.BinaryVector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Byte]])
        )
      case MilvusDataType.Int8Vector =>
        row.update(
          ordinal,
          ArrayData.toArrayData(inputValue.asInstanceOf[Array[Byte]])
        )
      case MilvusDataType.SparseFloatVector =>
        val sparseMap = inputValue.asInstanceOf[Map[Long, Float]]
        row.update(
          ordinal,
          ArrayBasedMapData.apply(
            sparseMap.keys.toArray,
            sparseMap.values.toArray
          )
        )
      case MilvusDataType.Array =>
        if (sparkFieldType.isInstanceOf[ArrayType]) {
          val sparkArrayType = sparkFieldType.asInstanceOf[ArrayType]
          val arrayValue = inputValue.asInstanceOf[Array[String]]
          sparkArrayType.elementType match {
            case SparkDataTypes.BooleanType =>
              val array = arrayValue.map(_.toBoolean)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.IntegerType =>
              val array = arrayValue.map(_.toInt)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.LongType =>
              val array = arrayValue.map(_.toLong)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.FloatType =>
              val array = arrayValue.map(_.toFloat)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.DoubleType =>
              val array = arrayValue.map(_.toDouble)
              row.update(ordinal, ArrayData.toArrayData(array))
            case SparkDataTypes.StringType =>
              val array = arrayValue.map(UTF8String.fromString(_))
              row.update(ordinal, ArrayData.toArrayData(array))
            case _ =>
              throw new UnsupportedOperationException(
                s"Unsupported data type for setting value at ordinal $ordinal: $dataType"
              )
          }
        } else {
          throw new UnsupportedOperationException(
            s"Unsupported data type for setting value at ordinal $ordinal: $dataType"
          )
        }
      case _ =>
        logWarning(
          s"Unsupported data type for setting value at ordinal $ordinal: $dataType"
        )
        row.setNullAt(ordinal)
    }
  }

  private def buildCurrentRow(): InternalRow = {
    val currentReaders = fieldReadersLock.synchronized {
      fieldFileReaders
    }

    if (currentReaders.isEmpty) {
      throw new IllegalStateException(
        "MilvusDataReader.buildCurrentRow() called before open() or with empty readers."
      )
    }

    // Determine the row size - add 1 extra column if partition is not empty
    val rowSize = if (partition != null && partition.nonEmpty) {
      currentReaders.size + 1
    } else {
      currentReaders.size
    }

    // Create a new InternalRow with the correct number of fields
    val row = InternalRow.fromSeq(Seq.fill(rowSize)(null))

    // Read one record from each field reader and set the corresponding field in the row
    // Ensure the order matches the schema's field order
    // Convert field names to integers and sort them
    val sortedFieldIndices = currentReaders.keys
      .map(fieldName => fieldName.toInt)
      .toSeq
      .sorted
      .zipWithIndex
    sortedFieldIndices.foreach { case (fieldID, index) =>
      currentReaders.get(fieldID.toString) match {
        case Some(reader) =>
          try {
            val fieldValue = reader.readNextRecord()
            setInternalRowValue(
              row,
              index,
              fieldValue,
              reader.getDataType()
            )
          } catch {
            case e: Exception =>
              logError(
                s"Error reading record for field '$fieldID' from file ${fieldFilesSeq(currentFieldFilesIndex)
                    .getOrElse(fieldID.toString, "N/A")}",
                e
              )
              // Handle error: set null, throw exception, or skip row
              row.setNullAt(index) // Example: set null on error
          }
        case None =>
          // This should not happen if planInputPartitions and open were correct
          logError(s"No reader found for schema field: $fieldID")
          row.setNullAt(index) // Set null if reader is missing
      }
    }

    // Add partition column value if partition is not empty
    if (partition != null && partition.nonEmpty) {
      row.update(currentReaders.size, UTF8String.fromString(partition))
    }

    row // Return the populated row
  }

  private def applyFilters(row: InternalRow): Boolean = {
    import org.apache.spark.sql.sources._
    import org.apache.spark.unsafe.types.UTF8String

    if (pushedFilters.isEmpty) {
      return true
    }

    pushedFilters.forall(filter => evaluateFilter(filter, row))
  }

  private def evaluateFilter(filter: Filter, row: InternalRow): Boolean = {
    import org.apache.spark.sql.sources._
    import org.apache.spark.unsafe.types.UTF8String

    filter match {
      case EqualTo(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        rowValue == value

      case GreaterThan(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) > 0

      case GreaterThanOrEqual(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) >= 0

      case LessThan(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) < 0

      case LessThanOrEqual(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) <= 0

      case In(attr, values) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        values.contains(rowValue)

      case IsNull(attr) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        row.isNullAt(columnIndex)

      case IsNotNull(attr) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        !row.isNullAt(columnIndex)

      case And(left, right) =>
        evaluateFilter(left, row) && evaluateFilter(right, row)

      case Or(left, right) =>
        evaluateFilter(left, row) || evaluateFilter(right, row)

      case _ => true // Unsupported filter, don't filter out
    }
  }

  private def getColumnIndex(columnName: String): Int = {
    try {
      schema.fieldIndex(columnName)
    } catch {
      case _: IllegalArgumentException => -1
    }
  }

  private def getRowValue(
      row: InternalRow,
      columnIndex: Int,
      columnName: String
  ): Any = {
    if (row.isNullAt(columnIndex)) {
      return null
    }

    schema.fields(columnIndex).dataType match {
      case SparkDataTypes.LongType    => row.getLong(columnIndex)
      case SparkDataTypes.IntegerType => row.getInt(columnIndex)
      case SparkDataTypes.FloatType   => row.getFloat(columnIndex)
      case SparkDataTypes.DoubleType  => row.getDouble(columnIndex)
      case SparkDataTypes.StringType =>
        row.getUTF8String(columnIndex).toString
      case SparkDataTypes.BooleanType => row.getBoolean(columnIndex)
      case _ => row.get(columnIndex, schema.fields(columnIndex).dataType)
    }
  }

  private def compareValues(rowValue: Any, filterValue: Any): Int = {
    (rowValue, filterValue) match {
      case (rv: Long, fv: Long)       => rv.compareTo(fv)
      case (rv: Long, fv: Int)        => rv.compareTo(fv.toLong)
      case (rv: Int, fv: Long)        => rv.toLong.compareTo(fv)
      case (rv: Int, fv: Int)         => rv.compareTo(fv)
      case (rv: Float, fv: Float)     => rv.compareTo(fv)
      case (rv: Double, fv: Double)   => rv.compareTo(fv)
      case (rv: String, fv: String)   => rv.compareTo(fv)
      case (rv: Boolean, fv: Boolean) => rv.compareTo(fv)
      case (rv: Long, fv: String)     => rv.toString.compareTo(fv)
      case (rv: String, fv: Long)     => rv.compareTo(fv.toString)
      case (rv: Boolean, fv: String)  => rv.toString.compareTo(fv)
      case (rv: String, fv: Boolean)  => rv.compareTo(fv.toString)
      case _ => 0 // Default to equal if types don't match
    }
  }

  // Close all field file readers and cleanup resources
  override def close(): Unit = {
    logInfo("MilvusDataReader closing all resources.")

    // Stop background preloading task
    stopBackgroundPreload = true
    backgroundPreloadTask.foreach { future =>
      if (!future.isCompleted) {
        logInfo("Background preload task is still running, will be stopped")
      }
    }
    backgroundPreloadTask = None
    fieldFileReaders = Map.empty

    // Cancel preload futures if still running
    preloadFutures.values.foreach { future =>
      if (!future.isCompleted) {
        // We can't actually cancel Scala Future, but we can ignore the result
        logInfo(
          "Preload future is still running, will be ignored on completion"
        )
      }
    }
    preloadFutures.clear()

    // Close current field file readers
    closeCurrentReaders()

    // Close all preloaded batch readers
    preloadedBatches.values.foreach { readers =>
      readers.values.foreach { reader =>
        try {
          reader.close()
        } catch {
          case e: Exception =>
            logWarning("Error closing preloaded batch field file reader", e)
        }
      }
    }
    preloadedBatches.clear()

    // Shutdown executor for preloading
    try {
      preloadExecutor.shutdown()
      if (!preloadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        preloadExecutor.shutdownNow()
        logWarning(
          "Preload executor did not terminate gracefully, forced shutdown"
        )
      } else {
        logInfo("Preload executor shutdown successfully")
      }
    } catch {
      case e: Exception =>
        logWarning("Error shutting down preload executor", e)
        preloadExecutor.shutdownNow()
    }

    logInfo("All MilvusDataReader resources closed.")
  }
}
