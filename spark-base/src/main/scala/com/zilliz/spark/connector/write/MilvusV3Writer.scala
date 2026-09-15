package com.zilliz.spark.connector.write

import java.{util => ju}
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  SupportsWrite,
  Table,
  TableCapability
}
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

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.schema.MilvusTypes
import com.zilliz.milvus.storage.write.commit.{
  CommitOutcome,
  CommittedSegment,
  Committer
}
import com.zilliz.milvus.storage.write.exec.{
  ManifestTransaction,
  StagingLayout,
  V3SegmentWriter
}
import com.zilliz.spark.connector.options.{HadoopStorageKeys, MilvusOption}
import com.zilliz.spark.connector.types.{SparkSchemaMapper, SparkTypes}
import com.zilliz.spark.connector.types.ArrowConverter
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Write support for `storage_version = 3`: parquet column groups under a
  * manifest, written through milvus-storage's `loon_*` writer. V2 and V3 in
  * this repository always mean the snapshot's `storage_version`; milvus-storage
  * calls the same manifest format its "format v2".
  */
case class MilvusV3WriteTable(
    milvusOption: MilvusOption,
    sparkSchema: StructType
) extends Table
    with SupportsWrite
    with Logging {

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new MilvusV3WriteBuilder(sparkSchema, milvusOption)
  }

  override def name(): String =
    s"MilvusV3Write[${milvusOption.collectionName}]"

  override def schema(): StructType = sparkSchema

  override def capabilities(): ju.Set[TableCapability] = {
    Set[TableCapability](
      TableCapability.BATCH_WRITE
    ).asJava
  }
}

/** Write builder for Storage V2
  */
class MilvusV3WriteBuilder(
    schema: StructType,
    milvusOption: MilvusOption
) extends WriteBuilder
    with Logging {

  override def build(): Write = new MilvusV3Write(schema, milvusOption)
}

/** The V3 write.
  */
class MilvusV3Write(
    schema: StructType,
    milvusOption: MilvusOption
) extends Write
    with Logging {

  override def toBatch: BatchWrite = {
    new MilvusV3BatchWrite(schema, milvusOption)
  }
}

/** The V3 batch write: one job id, one writer factory, and the job-level commit
  * through `core.write.commit.Committer`: the job manifest and the marker under
  * the staging prefix on commit, the prefix's files deleted on abort.
  */
class MilvusV3BatchWrite(
    schema: StructType,
    milvusOption: MilvusOption
) extends BatchWrite
    with Logging {

  /** One id per write job: every task writes under `StagingLayout(root, jobId)`
    * unless `milvus.writer.customPath` names the segment directory itself
    * (backfill).
    */
  val jobId: String = java.util.UUID.randomUUID().toString

  private val layout = StagingLayout(
    milvusOption.options.getOrElse(StorageProperties.RootPath, "files"),
    jobId
  )

  override def createBatchWriterFactory(
      info: PhysicalWriteInfo
  ): DataWriterFactory = {
    new MilvusV3WriterFactory(schema, milvusOption, jobId)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val segments = messages.toSeq.map {
      case msg: MilvusV3CommitMessage =>
        CommittedSegment(
          msg.partitionId,
          msg.manifestPath,
          msg.committedVersion,
          msg.recordCount
        )
      case other =>
        throw new IllegalStateException(
          s"unexpected commit message ${other.getClass.getName} in job $jobId"
        )
    }
    withCommitter(_.commit(segments)) match {
      case CommitOutcome.Committed =>
        logInfo(
          s"Job $jobId committed: ${segments.size} segments, ${segments.map(_.rowCount).sum} rows, manifest ${layout.manifest}"
        )
      case CommitOutcome.AlreadyCommitted =>
        logInfo(s"Job $jobId was already committed; nothing written")
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    val deleted = withCommitter(_.abort())
    logWarning(
      s"Job $jobId aborted: ${messages.length} task messages, $deleted files deleted under ${layout.prefix}"
    )
  }

  private def withCommitter[A](f: Committer => A): A = {
    val store = HadoopStorageKeys.storeFrom(milvusOption.options.toMap)
    try f(new Committer(store, layout))
    finally store.close()
  }
}

/** Writer factory for creating partition writers
  */
class MilvusV3WriterFactory(
    schema: StructType,
    milvusOption: MilvusOption,
    jobId: String
) extends DataWriterFactory
    with Serializable {

  override def createWriter(
      partitionId: Int,
      taskId: Long
  ): DataWriter[InternalRow] = {
    new MilvusV3PartitionWriter(
      partitionId,
      taskId,
      schema,
      milvusOption,
      jobId
    )
  }
}

object MilvusV3PartitionWriter {

  private[connector] def parsePositiveDoubleOption(
      options: scala.collection.Map[String, String],
      key: String,
      defaultValue: Double
  ): Double = {
    options
      .get(key.toLowerCase)
      .filter(_.trim.nonEmpty)
      .map { value =>
        val parsed = Try(value.trim.toDouble).getOrElse {
          throw new IllegalArgumentException(
            s"$key must be a finite positive number, got '$value'"
          )
        }
        if (!java.lang.Double.isFinite(parsed) || parsed <= 0.0) {
          throw new IllegalArgumentException(
            s"$key must be a finite positive number, got '$value'"
          )
        }
        parsed
      }
      .getOrElse(defaultValue)
  }
}

/** One task's segment: rows become Arrow batches here, the batches go to
  * `core.write.exec.V3SegmentWriter`, and `commit` records the column groups in
  * the segment's manifest through `ManifestTransaction`.
  */
class MilvusV3PartitionWriter(
    partitionId: Int,
    taskId: Long,
    sparkSchema: StructType,
    milvusOption: MilvusOption,
    jobId: String = "job"
) extends DataWriter[InternalRow]
    with Logging {

  private val batchSize = milvusOption.insertMaxBatchSize
  private val variableWidthBytesPerValue =
    MilvusV3PartitionWriter.parsePositiveDoubleOption(
      milvusOption.options,
      MilvusOption.WriterVariableWidthBytesPerValue,
      defaultValue = 32.0
    )
  private val writerProperties: Map[String, String] =
    StorageProperties.from(milvusOption.options)

  private val allocator = new RootAllocator(Long.MaxValue)

  private val vectorDimensions =
    extractVectorDimensions(sparkSchema, milvusOption)
  private val fieldIds = parseFieldIds(milvusOption)
  private val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(
    sparkSchema,
    vectorDimensions,
    fieldIds
  )

  // The segment directory: `milvus.writer.customPath` names it outright
  // (backfill writes into an existing segment's base path); otherwise the job
  // writes under its staging prefix. Both are keys relative to the bucket.
  private val basePath: String =
    milvusOption.options.get(MilvusOption.WriterCustomPath.toLowerCase) match {
      case Some(customPath) =>
        logInfo(s"Using custom write path: $customPath")
        customPath
      case None =>
        val path = StagingLayout(
          milvusOption.options.getOrElse(StorageProperties.RootPath, "files"),
          jobId
        ).segment(partitionId, taskId)
        logInfo(s"Writing to staging path: $path")
        path
    }

  private val segmentWriter =
    new V3SegmentWriter(basePath, arrowSchema, writerProperties, allocator)

  // The root accumulates one batch. A fresh one is built per flush: the C++
  // writer keeps referring to an exported batch's buffers until it flushes,
  // so a reused root would overwrite what C++ still reads.
  private var root = VectorSchemaRoot.create(arrowSchema, allocator)
  private var currentBatchSize = 0
  private var cleanedUp = false
  allocateVectors(root)

  logInfo(
    s"Created V3 writer for partition $partitionId, task $taskId, basePath: $basePath"
  )

  override def write(record: InternalRow): Unit = {
    ArrowConverter.internalRowToArrow(
      root,
      currentBatchSize,
      record,
      sparkSchema
    )
    currentBatchSize += 1
    root.setRowCount(currentBatchSize)
    if (currentBatchSize >= batchSize) flushBatch()
  }

  override def commit(): WriterCommitMessage = {
    try {
      if (currentBatchSize > 0) flushBatch()
      val rows = segmentWriter.rows
      val groups = segmentWriter.finish()
      val committedVersion =
        try {
          val change = milvusOption.options.get(
            MilvusOption.WriterCommitType.toLowerCase
          ) match {
            case Some("addfield") =>
              // Backfill replaces the target columns. Manifest columns are
              // keyed by Milvus field id (the Arrow column names are the ids),
              // so the drop names the id, not the Spark field name.
              ManifestTransaction.ReplaceColumns(sparkSchema.fields.map { f =>
                fieldIds
                  .getOrElse(
                    f.name,
                    throw new IllegalStateException(
                      s"Missing field ID for backfill column '${f.name}'"
                    )
                  )
                  .toString
              })
            case _ => ManifestTransaction.AppendFiles
          }
          ManifestTransaction.commit(basePath, writerProperties, groups, change)
        } finally groups.close()
      logInfo(
        s"Manifest committed: partition=$partitionId, records=$rows, basePath=$basePath, version=$committedVersion"
      )
      MilvusV3CommitMessage(partitionId, rows, basePath, committedVersion)
    } finally cleanup()
  }

  override def abort(): Unit = {
    logWarning(s"Aborting write for partition $partitionId, task $taskId")
    cleanup()
  }

  override def close(): Unit = cleanup()

  private def flushBatch(): Unit = {
    if (currentBatchSize == 0) return
    root.setRowCount(currentBatchSize)
    segmentWriter.write(root)
    // Build and allocate the replacement before swapping, so an allocation
    // failure leaves `root` pointing at the old one for cleanup to release.
    val newRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    try allocateVectors(newRoot)
    catch {
      case t: Throwable =>
        Try(newRoot.close())
        throw t
    }
    val oldRoot = root
    root = newRoot
    currentBatchSize = 0
    // Closing the old root drops the JVM's reference only; the export keeps
    // the buffers alive until C++ flushes. A failure here is logged, not
    // thrown: the batch is already durable and a retry would duplicate it.
    Try(oldRoot.close()).recover { case e: Exception =>
      logError(
        s"Error closing old VectorSchemaRoot after flush: ${e.getMessage}"
      )
    }
  }

  /** Sets each vector's initial capacity for one batch. Variable-width vectors
    * take a per-value density (bytes per value), not a total.
    */
  private def allocateVectors(r: VectorSchemaRoot): Unit = {
    import scala.collection.JavaConverters._
    import org.apache.arrow.vector.{VarCharVector, BaseVariableWidthVector}
    r.getFieldVectors.asScala.foreach {
      case v: VarCharVector =>
        v.setInitialCapacity(batchSize, variableWidthBytesPerValue)
      case v: BaseVariableWidthVector =>
        v.setInitialCapacity(batchSize, variableWidthBytesPerValue)
      case v => v.setInitialCapacity(batchSize)
    }
    r.allocateNew()
    r.setRowCount(0)
  }

  /** Vector dimensions from `vector.<field>.dim` options, for the dense vector
    * fields of the schema.
    */
  private def extractVectorDimensions(
      schema: StructType,
      option: MilvusOption
  ): Map[String, Int] = {
    val vectorFields = schema.fields.collect {
      case field
          if field.metadata.contains(
            FieldMetadata.MilvusDataTypeMetadataKey
          ) && MilvusTypes.isDenseVectorType(
            MilvusDataType.fromValue(
              field.metadata
                .getLong(FieldMetadata.MilvusDataTypeMetadataKey)
                .toInt
            )
          ) =>
        field.name
      case field @ StructField(_, ArrayType(FloatType, _), _, _) => field.name
    }
    vectorFields.flatMap { fieldName =>
      option.options.get(MilvusOption.vectorDimKey(fieldName)).flatMap {
        dimStr =>
          Try(dimStr.toInt).toOption.map(fieldName -> _)
      }
    }.toMap
  }

  /** `milvus.writer.fieldIds`: `name:id,name:id`. */
  private def parseFieldIds(option: MilvusOption): Map[String, Long] = {
    option.options
      .get(MilvusOption.WriterFieldIds.toLowerCase)
      .map { str =>
        str
          .split(",")
          .flatMap { pair =>
            val parts = pair.split(":", 2)
            if (parts.length == 2) {
              Try(parts(1).trim.toLong).toOption.map(parts(0).trim -> _)
            } else None
          }
          .toMap
      }
      .getOrElse(Map.empty)
  }

  /** Releases the native writer and every Arrow resource. Idempotent: Spark
    * calls commit and then close, and abort reaches it too.
    */
  private def cleanup(): Unit = {
    if (cleanedUp) return
    cleanedUp = true
    Try(segmentWriter.close()).recover { case e: Exception =>
      logError(s"Error closing the segment writer: ${e.getMessage}")
    }
    Try(if (root != null) root.close()).recover { case e: Exception =>
      logError(s"Error closing VectorSchemaRoot: ${e.getMessage}")
    }
    Try(allocator.close()).recover { case e: Exception =>
      logError(s"Error closing allocator: ${e.getMessage}")
    }
  }
}

/** Commit message containing write metadata
  */
case class MilvusV3CommitMessage(
    partitionId: Int,
    recordCount: Long,
    manifestPath: String,
    committedVersion: Long
) extends WriterCommitMessage

/** Helper object for DataFrame write operations
  */
object MilvusV3Writer extends Logging {

  /** Write a DataFrame to S3 using Storage V2 format (FFI) This method writes
    * directly to S3 without connecting to Milvus
    *
    * @param df
    *   DataFrame to write
    * @param options
    *   S3 configuration and write options Required options:
    *   - fs.endpoint or fs.address: S3 endpoint (e.g., "localhost:9000")
    *   - fs.bucket_name: S3 bucket name
    *   - fs.access_key_id: S3 access key
    *   - fs.access_key_value: S3 secret key
    *   - fs.use_ssl: "true" or "false" Optional:
    *   - fs.root_path: Root path in bucket (default: "files")
    *   - milvus.collection.name: Collection name for path generation
    *   - vector.{field_name}.dim: Vector dimension for float array fields
    *   - milvus.writer.variableWidthBytesPerValue: initial bytes per
    *     variable-width value (default: 32.0)
    * @return
    *   Try containing manifest paths on success
    */
  def writeDataFrame(
      df: DataFrame,
      options: Map[String, String]
  ): Try[Seq[String]] = {

    try {
      val optionsMap = new CaseInsensitiveStringMap(options.asJava)
      val milvusOption = MilvusOption(optionsMap)

      // Write using Storage V2 FFI directly
      val manifestPaths = writeV3(df, milvusOption)

      Success(manifestPaths)

    } catch {
      case e: Exception =>
        logError(s"Failed to write DataFrame to Storage V2: ${e.getMessage}", e)
        Failure(e)
    }
  }

  /** Internal method to write using Storage V2 API
    */
  private def writeV3(
      df: DataFrame,
      milvusOption: MilvusOption
  ): Seq[String] = {

    val sparkSchema = df.schema

    // Create batch write
    val batchWrite = new MilvusV3BatchWrite(sparkSchema, milvusOption)
    val writerFactory = batchWrite.createBatchWriterFactory(null)

    // Execute write on each partition using queryExecution to get InternalRow
    val messages = df.queryExecution.toRdd
      .mapPartitionsWithIndex { (partitionId, rows) =>
        val writer =
          writerFactory.createWriter(partitionId, System.currentTimeMillis())

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
      }
      .collect()

    // Commit all partitions
    batchWrite.commit(messages)

    // Extract manifest paths
    messages.collect { case msg: MilvusV3CommitMessage =>
      msg.manifestPath
    }.toSeq
  }
}
