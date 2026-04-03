package com.zilliz.spark.connector.write

import java.{util => ju}
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriter,
  DataWriterFactory,
  LogicalWriteInfo,
  PhysicalWriteInfo,
  Write,
  WriteBuilder,
  WriterCommitMessage
}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.spark.connector.{
  MilvusOption,
  MilvusSchemaUtil
}
import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.serde.ArrowConverter
import io.milvus.storage.{
  ArrowUtils,
  MilvusStorageProperties,
  MilvusStorageTransaction,
  MilvusStorageWriter,
  NativeLibraryLoader
}

/**
 * MilvusLoonWriteTable provides write support using Storage V2 FFI
 * This table is used by MilvusLoonDataSource for write operations
 */
case class MilvusLoonWriteTable(
    milvusOption: MilvusOption,
    sparkSchema: StructType
) extends Table with SupportsWrite with Logging {

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new MilvusLoonWriteBuilder(sparkSchema, milvusOption)
  }

  override def name(): String = s"MilvusLoonWrite[${milvusOption.collectionName}]"

  override def schema(): StructType = sparkSchema

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_WRITE
    ).asJava
  }
}

/**
 * Write builder for Storage V2
 */
class MilvusLoonWriteBuilder(
    schema: StructType,
    milvusOption: MilvusOption
) extends WriteBuilder with Logging {

  override def build(): Write = new MilvusLoonWrite(schema, milvusOption)
}

/**
 * Write implementation for Loon
 */
class MilvusLoonWrite(
    schema: StructType,
    milvusOption: MilvusOption
) extends Write with Logging {

  override def toBatch: BatchWrite = {
    new MilvusLoonBatchWrite(schema, milvusOption)
  }
}

/**
 * Batch write implementation for Storage V2
 */
class MilvusLoonBatchWrite(
    schema: StructType,
    milvusOption: MilvusOption
) extends BatchWrite with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new MilvusLoonWriterFactory(schema, milvusOption)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Committed ${messages.length} partitions")
    messages.foreach {
      case msg: MilvusLoonCommitMessage =>
        logInfo(s"Partition ${msg.partitionId} wrote ${msg.recordCount} records, manifest: ${msg.manifestPath}, version: ${msg.committedVersion}")
      case _ =>
        logWarning("Unknown commit message type")
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logWarning(s"Aborting write for ${messages.length} partitions")
    // TODO: Clean up S3 files if needed
  }
}

/**
 * Writer factory for creating partition writers
 */
class MilvusLoonWriterFactory(
    schema: StructType,
    milvusOption: MilvusOption
) extends DataWriterFactory with Serializable {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new MilvusLoonPartitionWriter(
      partitionId,
      taskId,
      schema,
      milvusOption
    )
  }
}

object MilvusLoonPartitionWriter {
  @volatile private var writeInitialized: Boolean = false
  private val writeLock = new Object()

  /**
   * Ensures the first write operation is serialized to avoid race conditions
   * in native library's S3 client initialization. Once a write succeeds,
   * subsequent writes can run in parallel.
   */
  def synchronizedWrite(doWrite: => Unit): Unit = {
    if (!writeInitialized) {
      writeLock.synchronized {
        if (!writeInitialized) {
          doWrite
          writeInitialized = true
        } else {
          // Another thread completed initialization while we waited
          doWrite
        }
      }
    } else {
      doWrite
    }
  }
}

/**
 * Partition writer using Storage V2 FFI
 */
class MilvusLoonPartitionWriter(
    partitionId: Int,
    taskId: Long,
    sparkSchema: StructType,
    milvusOption: MilvusOption
) extends DataWriter[InternalRow] with Logging {

  private val allocator = new RootAllocator(Long.MaxValue)

  // Create Arrow schema from Spark schema
  // Note: For vector fields, pass vector dimensions via milvusOption if needed
  private val vectorDimensions = extractVectorDimensions(sparkSchema, milvusOption)
  private val fieldIds = parseFieldIds(milvusOption)
  private val arrowSchema = MilvusSchemaUtil.convertSparkSchemaToArrow(sparkSchema, vectorDimensions, fieldIds)

  // Create VectorSchemaRoot to accumulate batches
  // IMPORTANT: root must be var because we create a new one for each flush.
  // The C++ writer caches RecordBatch shared_ptrs that reference Java-owned buffers
  // via Arrow C Data Interface (zero-copy). Reusing the same root would overwrite
  // buffer contents that C++ still references, causing data corruption.
  private var root = VectorSchemaRoot.create(arrowSchema, allocator)

  // Track exported roots whose buffers are still referenced by C++ RecordBatches.
  // These must stay alive until the C++ writer is closed.
  private val exportedRoots = new java.util.ArrayList[VectorSchemaRoot]()

  // Batch size configuration
  private val batchSize = milvusOption.insertMaxBatchSize
  private var currentBatchSize = 0
  private var totalRecordCount = 0L

  // Allocate initial capacity for vectors
  allocateVectors()

  // Base path for writing - use custom path if provided, otherwise generate
  private val basePath = {
    // Note: options map has lowercase keys due to CaseInsensitiveStringMap conversion
    milvusOption.options.get(MilvusOption.WriterCustomPath.toLowerCase) match {
      case Some(customPath) =>
        logInfo(s"Using custom write path: $customPath")
        customPath
      case None =>
        val generated = generateBasePath()
        logInfo(s"Using generated write path: $generated")
        generated
    }
  }

  // Create Storage V2 writer
  private val (writer, writerProperties, arrowSchemaC) = {
    // Writer properties from MilvusOption
    val props = Properties.fromMilvusOption(milvusOption)

    // Create Storage V2 writer
    val schemaC = ArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, arrowSchema, null, schemaC)

    val w = new MilvusStorageWriter()
    w.create(basePath, schemaC.memoryAddress(), props)

    if (!w.isValid) {
      schemaC.close()
      props.free()
      throw new IllegalStateException("Failed to create MilvusStorageWriter")
    }

    (w, props, schemaC)
  }

  logInfo(s"Created Storage V2 writer for partition $partitionId, task $taskId, basePath: $basePath")

  override def write(record: InternalRow): Unit = {
    // Add record to current batch
    addRecordToBatch(record)
    currentBatchSize += 1

    // Flush batch if it reaches the batch size
    if (currentBatchSize >= batchSize) {
      flushBatch()
    }
  }

  override def commit(): WriterCommitMessage = {
    try {
      // Flush remaining records
      if (currentBatchSize > 0) {
        flushBatch()
      }

      // Close writer and get column groups pointer
      val columnGroupsPtr = writer.close()

      logInfo(s"Writer closed: partition=$partitionId, records=$totalRecordCount, columnGroupsPtr=$columnGroupsPtr")

      // Commit column groups to manifest using Transaction
      val transaction = new MilvusStorageTransaction()
      transaction.begin(basePath, writerProperties)

      // Determine commit type: ADDFIELD (1) for backfill, ADDFILES (0) for normal writes
      val commitType = milvusOption.options.get(MilvusOption.WriterCommitType.toLowerCase) match {
        case Some("addfield") => 1  // ADDFIELD
        case _ => 0                 // ADDFILES (default)
      }
      val committedVersion = transaction.commit(commitType, 0, columnGroupsPtr)
      transaction.destroy()

      if (committedVersion < 0) {
        throw new IllegalStateException(s"Failed to commit manifest for partition $partitionId")
      }

      logInfo(s"Manifest committed: partition=$partitionId, records=$totalRecordCount, basePath=$basePath, version=$committedVersion")

      MilvusLoonCommitMessage(partitionId, totalRecordCount, basePath, columnGroupsPtr, committedVersion)
    } finally {
      cleanup()
    }
  }

  override def abort(): Unit = {
    logWarning(s"Aborting write for partition $partitionId, task $taskId")
    cleanup()
  }

  override def close(): Unit = {
    cleanup()
  }

  /**
   * Add a Spark InternalRow to the current Arrow batch
   */
  private def addRecordToBatch(record: InternalRow): Unit = {
    ArrowConverter.internalRowToArrow(root, currentBatchSize, record, sparkSchema)
    root.setRowCount(currentBatchSize + 1)
  }

  /**
   * Flush current batch to Storage V2 writer
   */
  private def flushBatch(): Unit = {
    if (currentBatchSize == 0) {
      return
    }

    // Set final row count before export
    root.setRowCount(currentBatchSize)

    // Export Arrow array to C interface
    val arrowArrayC = ArrowArray.allocateNew(allocator)
    Data.exportVectorSchemaRoot(allocator, root, null, arrowArrayC)

    try {
      // Use synchronized write for the first operation to avoid race conditions
      // in native library's S3 client initialization
      MilvusLoonPartitionWriter.synchronizedWrite {
        writer.write(arrowArrayC.memoryAddress())
        writer.flush()
      }

      totalRecordCount += currentBatchSize

      // CRITICAL: Do NOT reuse the same root for the next batch!
      // The C++ ParquetFileWriter caches RecordBatch shared_ptrs that wrap pointers
      // to this root's buffers (via Arrow C Data Interface zero-copy import).
      // If we reuse the same root, writing new data overwrites the buffer memory
      // that C++ cached batches still reference, causing data corruption.
      // Instead, keep the old root alive and create a fresh one with new buffers.
      exportedRoots.add(root)
      root = VectorSchemaRoot.create(arrowSchema, allocator)
      allocateVectors()
      currentBatchSize = 0

    } finally {
      arrowArrayC.close()
    }
  }

  /**
   * Allocate or reallocate vectors for the current batch
   */
  private def allocateVectors(): Unit = {
    import scala.collection.JavaConverters._
    import org.apache.arrow.vector.{VarCharVector, BaseVariableWidthVector}

    // For each vector, set appropriate initial capacity
    root.getFieldVectors.asScala.foreach { vector =>
      vector match {
        case varCharVector: VarCharVector =>
          // For VarChar vectors, allocate both row capacity and byte capacity
          // This will auto-expand if needed, but starts small to avoid OOM
          val estimatedBytesPerValue = 32
          val totalByteCapacity = batchSize * estimatedBytesPerValue
          varCharVector.setInitialCapacity(batchSize, totalByteCapacity)

        case baseVarVector: BaseVariableWidthVector =>
          // For other variable-width vectors (like VarBinary)
          val estimatedBytesPerValue = 32
          val totalByteCapacity = batchSize * estimatedBytesPerValue
          baseVarVector.setInitialCapacity(batchSize, totalByteCapacity)

        case _ =>
          // For fixed-width vectors, just set row capacity
          vector.setInitialCapacity(batchSize)
      }
    }

    root.allocateNew()
    root.setRowCount(0)
  }

  /**
   * Generate S3 base path for this writer
   *
   * For S3FileSystem, the path format should be: bucket/root_path/...
   * Arrow S3FileSystem expects paths in the format: bucket_name/path/to/object
   */
  private def generateBasePath(): String = {
    val timestamp = System.currentTimeMillis()
    val collectionName = if (milvusOption.collectionName.nonEmpty) milvusOption.collectionName else "default"
    val partitionName = if (milvusOption.partitionName.nonEmpty) milvusOption.partitionName else "default"

    // Extract S3 configuration from MilvusOption
    val bucket = milvusOption.options.getOrElse(Properties.FsConfig.FsBucketName, "a-bucket")
    val rootPath = milvusOption.options.getOrElse(Properties.FsConfig.FsRootPath, "files")

    // Include bucket name in the path for S3FileSystem
    s"$bucket/$rootPath/spark_write/$collectionName/$partitionName/$timestamp/task_${partitionId}_$taskId"
  }

  /**
   * Extract vector dimensions from MilvusOption for vector fields
   * Vector fields are identified as Array[Float] in Spark schema
   */
  private def extractVectorDimensions(
      schema: StructType,
      option: MilvusOption
  ): Map[String, Int] = {
    // Check if vector dimensions are provided in options
    // Format: vector.field_name.dim = dimension_value
    val vectorFields = schema.fields.collect {
      case field if field.dataType.isInstanceOf[ArrayType] &&
                    field.dataType.asInstanceOf[ArrayType].elementType == FloatType =>
        field.name
    }

    vectorFields.flatMap { fieldName =>
      option.options.get(MilvusOption.vectorDimKey(fieldName)).flatMap { dimStr =>
        Try(dimStr.toInt).toOption.map(fieldName -> _)
      }
    }.toMap
  }

  /**
   * Parse field ID mapping from MilvusOption
   * Format: "field_name:field_id,field_name2:field_id2"
   */
  private def parseFieldIds(option: MilvusOption): Map[String, Long] = {
    option.options.get(MilvusOption.WriterFieldIds.toLowerCase).map { str =>
      str.split(",").flatMap { pair =>
        val parts = pair.split(":", 2)
        if (parts.length == 2) {
          Try(parts(1).trim.toLong).toOption.map(parts(0).trim -> _)
        } else None
      }.toMap
    }.getOrElse(Map.empty)
  }

  /**
   * Clean up resources
   */
  private def cleanup(): Unit = {
    Try {
      if (writer != null && writer.isValid) {
        writer.destroy()
      }
    }.recover {
      case e: Exception => logError(s"Error destroying writer: ${e.getMessage}")
    }

    // Close current root (not yet exported)
    Try {
      if (root != null) root.close()
    }.recover {
      case e: Exception => logError(s"Error closing VectorSchemaRoot: ${e.getMessage}")
    }

    // Close exported roots whose buffers were referenced by C++ RecordBatches.
    // By this point the C++ writer is destroyed, so all release callbacks have fired.
    Try {
      exportedRoots.forEach(r => Try(r.close()))
      exportedRoots.clear()
    }.recover {
      case e: Exception => logError(s"Error closing exported roots: ${e.getMessage}")
    }

    Try {
      if (arrowSchemaC != null) arrowSchemaC.close()
    }.recover {
      case e: Exception => logError(s"Error closing ArrowSchema: ${e.getMessage}")
    }

    Try {
      if (writerProperties != null) writerProperties.free()
    }.recover {
      case e: Exception => logError(s"Error freeing properties: ${e.getMessage}")
    }

    Try {
      if (allocator != null) allocator.close()
    }.recover {
      case e: Exception => logError(s"Error closing allocator: ${e.getMessage}")
    }
  }
}

/**
 * Commit message containing write metadata
 */
case class MilvusLoonCommitMessage(
    partitionId: Int,
    recordCount: Long,
    manifestPath: String,
    columnGroupsPtr: Long,
    committedVersion: Long
) extends WriterCommitMessage

/**
 * Helper object for DataFrame write operations
 */
object MilvusLoonWriter extends Logging {

  /**
   * Write a DataFrame to S3 using Storage V2 format (FFI)
   * This method writes directly to S3 without connecting to Milvus
   *
   * @param df DataFrame to write
   * @param options S3 configuration and write options
   *                Required options:
   *                - fs.endpoint or fs.address: S3 endpoint (e.g., "localhost:9000")
   *                - fs.bucket_name: S3 bucket name
   *                - fs.access_key_id: S3 access key
   *                - fs.access_key_value: S3 secret key
   *                - fs.use_ssl: "true" or "false"
   *                Optional:
   *                - fs.root_path: Root path in bucket (default: "files")
   *                - milvus.collection.name: Collection name for path generation
   *                - vector.{field_name}.dim: Vector dimension for float array fields
   * @return Try containing manifest paths on success
   */
  def writeDataFrame(
      df: DataFrame,
      options: Map[String, String]
  ): Try[Seq[String]] = {

    try {
      val optionsMap = new CaseInsensitiveStringMap(options.asJava)
      val milvusOption = MilvusOption(optionsMap)

      // Write using Storage V2 FFI directly
      val manifestPaths = writeWithLoon(df, milvusOption)

      Success(manifestPaths)

    } catch {
      case e: Exception =>
        logError(s"Failed to write DataFrame to Storage V2: ${e.getMessage}", e)
        Failure(e)
    }
  }

  /**
   * Internal method to write using Storage V2 API
   */
  private def writeWithLoon(
      df: DataFrame,
      milvusOption: MilvusOption
  ): Seq[String] = {

    val sparkSchema = df.schema

    // Create batch write
    val batchWrite = new MilvusLoonBatchWrite(sparkSchema, milvusOption)
    val writerFactory = batchWrite.createBatchWriterFactory(null)

    // Execute write on each partition using queryExecution to get InternalRow
    val messages = df.queryExecution.toRdd.mapPartitionsWithIndex { (partitionId, rows) =>
      val writer = writerFactory.createWriter(partitionId, System.currentTimeMillis())

      try {
        rows.foreach { row =>
          writer.write(row)
        }
        val commitMessage = writer.commit()
        Iterator(commitMessage)
      } catch {
        case e: Exception =>
          writer.abort()
          throw e
      } finally {
        writer.close()
      }
    }.collect()

    // Commit all partitions
    batchWrite.commit(messages)

    // Extract manifest paths
    messages.collect {
      case msg: MilvusLoonCommitMessage => msg.manifestPath
    }.toSeq
  }
}
