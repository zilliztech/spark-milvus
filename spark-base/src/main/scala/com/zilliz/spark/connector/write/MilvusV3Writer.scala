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
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.schema.MilvusTypes
import com.zilliz.milvus.storage.stats.PrimaryKeyStats
import com.zilliz.milvus.storage.write.commit.{
  CommitOutcome,
  CommittedSegment,
  Committer
}
import com.zilliz.milvus.storage.write.exec.{
  ColumnGroupSplit,
  ManifestTransaction,
  StagingLayout,
  V3SegmentWriter
}
import com.zilliz.spark.connector.options.{HadoopStorageKeys, MilvusOption}
import com.zilliz.spark.connector.types.{SparkSchemaMapper, SparkTypes}
import com.zilliz.spark.connector.types.ArrowConverter
import io.milvus.grpc.schema.{CollectionSchema, DataType => MilvusDataType}

/** The write builder `MilvusTable.newWriteBuilder` returns: `build()` checks
  * the DataFrame's schema against the collection's (`WriteSchema`) and the
  * write goes out as `storage_version = 3` segments, parquet column groups
  * under a manifest written through milvus-storage's `loon_*` writer. V2 and V3
  * in this repository always mean the snapshot's `storage_version`;
  * milvus-storage calls the same manifest format its "format v2".
  *
  * @param dataFrameSchema
  *   what Spark passed in `LogicalWriteInfo`: the DataFrame's own schema
  * @param collection
  *   the collection schema of the snapshot the table was resolved from
  */
class MilvusV3WriteBuilder(
    dataFrameSchema: StructType,
    collection: CollectionSchema,
    milvusOption: MilvusOption
) extends WriteBuilder
    with Logging {

  override def build(): Write = new MilvusV3Write(
    WriteSchema.resolve(dataFrameSchema, collection, WriteSchema.Mode.Append),
    milvusOption
  )
}

/** The V3 write. `schema` is the resolved write schema: every column carries
  * its Milvus type, field id and, for dense vectors, dimension.
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

  // Field ids and dimensions come from the column metadata WriteSchema put
  // there; a column without a field id would be named by position, which
  // does not match any collection field, so it is refused.
  private val fieldIds: Map[String, Long] = sparkSchema.fields.map { f =>
    if (!f.metadata.contains(FieldMetadata.MilvusFieldIdMetadataKey)) {
      throw new IllegalArgumentException(
        s"Column '${f.name}' carries no Milvus field id; resolve the schema through WriteSchema first"
      )
    }
    f.name -> f.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
  }.toMap
  private val arrowSchema = SparkSchemaMapper.convertSparkSchemaToArrow(
    sparkSchema,
    fieldIds = fieldIds
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

  // Opened at the first flush, because the column-group split wants the
  // average value size of each column and the first batch is the sample;
  // a task that never flushes opens it in commit, split by schema alone.
  private var segmentWriter: V3SegmentWriter = null

  // The primary key column, when this write carries it (an append does, a
  // backfill of other columns does not): its position, field id and type.
  // Its keys are collected as batches flush and become the segment's
  // bloom-filter stats at commit, the file Milvus keeps for delete routing
  // and primary-key pruning.
  private val primaryKey: Option[(Int, Long, MilvusDataType)] =
    sparkSchema.fields.zipWithIndex.collectFirst {
      case (f, i)
          if f.metadata.contains(FieldMetadata.MilvusPrimaryKeyMetadataKey) &&
            f.metadata.getBoolean(FieldMetadata.MilvusPrimaryKeyMetadataKey) =>
        (
          i,
          fieldIds(f.name),
          MilvusDataType.fromValue(
            f.metadata.getLong(FieldMetadata.MilvusDataTypeMetadataKey).toInt
          )
        )
    }
  private val primaryKeyStats: Option[PrimaryKeyStats.Builder] =
    primaryKey.map { case (_, id, dataType) =>
      new PrimaryKeyStats.Builder(id, dataType)
    }

  private def collectPrimaryKeys(root: VectorSchemaRoot): Unit =
    primaryKey.foreach { case (index, _, _) =>
      val stats = primaryKeyStats.get
      root.getVector(index) match {
        case v: BigIntVector =>
          var i = 0
          while (i < root.getRowCount) { stats.addLong(v.get(i)); i += 1 }
        case v: VarCharVector =>
          var i = 0
          while (i < root.getRowCount) {
            stats.addString(
              new String(v.get(i), java.nio.charset.StandardCharsets.UTF_8)
            )
            i += 1
          }
        case other =>
          throw new IllegalStateException(
            s"primary key column is ${other.getClass.getSimpleName}, not an Int64 or VarChar vector"
          )
      }
    }

  /** Writes `_stats/bloom_filter.<pk>/<id>` and returns the manifest entry for
    * it, or nothing when this write carries no primary key or no row.
    */
  private def writePrimaryKeyStats(): Seq[ManifestTransaction.Stat] =
    primaryKeyStats.filter(_.size > 0).toSeq.map { builder =>
      val stats = builder.build()
      val bytes = stats.toBytes
      val path =
        s"$basePath/_stats/bloom_filter.${stats.fieldId}/${System.currentTimeMillis()}"
      val store = NativeObjectStore.Factory(writerProperties).open()
      try store.write(path, bytes)
      finally store.close()
      logInfo(
        s"Primary-key stats of $basePath: ${builder.size} keys, ${bytes.length} bytes at $path"
      )
      ManifestTransaction.Stat(
        key = s"bloom_filter.${stats.fieldId}",
        files = Seq(path),
        metadata = Map("memory_size" -> bytes.length.toString)
      )
    }

  private def openSegmentWriter(sample: Option[VectorSchemaRoot]): Unit = {
    val rows = sample.map(_.getRowCount).getOrElse(0)
    val avgBytes: Map[Long, Long] = sample match {
      case Some(root) if rows > 0 =>
        sparkSchema.fields.zipWithIndex.map { case (f, i) =>
          fieldIds(f.name) -> root.getVector(i).getBufferSize.toLong / rows
        }.toMap
      case _ => Map.empty
    }
    val columns = sparkSchema.fields.map { f =>
      ColumnGroupSplit.Column(
        fieldId = fieldIds(f.name),
        dataType = MilvusDataType.fromValue(
          f.metadata.getLong(FieldMetadata.MilvusDataTypeMetadataKey).toInt
        ),
        isKey = Seq(
          FieldMetadata.MilvusPrimaryKeyMetadataKey,
          FieldMetadata.MilvusPartitionKeyMetadataKey,
          FieldMetadata.MilvusClusteringKeyMetadataKey
        ).exists(k => f.metadata.contains(k) && f.metadata.getBoolean(k))
      )
    }
    val patterns = ColumnGroupSplit.milvusPatterns(columns, avgBytes.get)
    logInfo(s"Column groups of $basePath: ${patterns.mkString(", ")}")
    segmentWriter = new V3SegmentWriter(
      basePath,
      arrowSchema,
      writerProperties,
      allocator,
      patterns
    )
  }

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
      if (segmentWriter == null) openSegmentWriter(None)
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
          ManifestTransaction.commit(
            basePath,
            writerProperties,
            groups,
            change,
            writePrimaryKeyStats()
          )
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
    if (segmentWriter == null) openSegmentWriter(Some(root))
    collectPrimaryKeys(root)
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

  /** Releases the native writer and every Arrow resource. Idempotent: Spark
    * calls commit and then close, and abort reaches it too.
    */
  private def cleanup(): Unit = {
    if (cleanedUp) return
    cleanedUp = true
    Try(if (segmentWriter != null) segmentWriter.close()).recover {
      case e: Exception =>
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

/** The direct entry point, for callers that are not Spark's write protocol:
  * backfill and the tests. `df.write.format("milvus")` goes through
  * `MilvusTable.newWriteBuilder` and ends in the same `MilvusV3BatchWrite`.
  */
object MilvusV3Writer extends Logging {

  /** Writes a DataFrame as V3 segments, one per Spark partition, and commits
    * the job through `core.write.commit`.
    *
    * The columns are checked against `collection` first: with
    * `milvus.writer.commitType=addfield` (backfill) only the given columns have
    * to be fields; otherwise the DataFrame has to carry every field. The `fs.*`
    * options name the storage; `fs.root_path` is the root the staging prefix
    * goes under; `milvus.writer.customPath` names an existing segment directory
    * instead (backfill).
    *
    * @return
    *   the base path of every segment written
    */
  def writeDataFrame(
      df: DataFrame,
      options: Map[String, String],
      collection: CollectionSchema
  ): Try[Seq[String]] = {

    try {
      val optionsMap = new CaseInsensitiveStringMap(options.asJava)
      val milvusOption = MilvusOption(optionsMap)
      val mode =
        if (
          milvusOption.options
            .get(MilvusOption.WriterCommitType.toLowerCase)
            .contains("addfield")
        ) WriteSchema.Mode.Columns
        else WriteSchema.Mode.Append
      val schema = WriteSchema.resolve(df.schema, collection, mode)
      Success(writeV3(df, schema, milvusOption))
    } catch {
      case e: Exception =>
        logError(
          s"Failed to write DataFrame as V3 segments: ${e.getMessage}",
          e
        )
        Failure(e)
    }
  }

  private def writeV3(
      df: DataFrame,
      schema: StructType,
      milvusOption: MilvusOption
  ): Seq[String] = {

    // Create batch write
    val batchWrite = new MilvusV3BatchWrite(schema, milvusOption)
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
