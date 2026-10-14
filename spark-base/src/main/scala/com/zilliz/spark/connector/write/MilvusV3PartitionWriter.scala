package com.zilliz.spark.connector.write

import java.lang.{Double => JavaDouble}
import java.nio.charset.StandardCharsets
import scala.collection.{Map => CollectionMap}
import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BaseVariableWidthVector,
  BigIntVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{
  DataWriter,
  DataWriterFactory,
  WriterCommitMessage
}
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.schema.{
  FieldMetadata,
  HybridTimestamp,
  SchemaMapper
}
import com.zilliz.milvus.storage.stats.PrimaryKeyStats
import com.zilliz.milvus.storage.write.exec.{
  ColumnGroupSplit,
  ManifestTransaction,
  StagingLayout,
  V3SegmentWriter,
  WriteMetrics
}
import com.zilliz.spark.connector.metrics.WriteMetricsReport
import com.zilliz.spark.connector.options.{MilvusOption, OptionParsing}
import com.zilliz.spark.connector.types.ArrowConverter
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Writer factory for creating partition writers
  */
class MilvusV3WriterFactory(
    schema: StructType,
    milvusOption: MilvusOption,
    jobId: String,
    storage: Map[String, String]
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
      storage,
      jobId
    )
  }
}

object MilvusV3PartitionWriter {

  private[connector] def parsePositiveDoubleOption(
      options: CollectionMap[String, String],
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
        if (!JavaDouble.isFinite(parsed) || parsed <= 0.0) {
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
    storage: Map[String, String],
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
  private val storageProperties: Map[String, String] = storage
  private val writerProperties: Map[String, String] =
    MilvusOption.writerProperties(milvusOption, storage)

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
  // A write that carries the primary key is writing a new segment, so it also
  // writes the system columns; a backfill adds columns to a segment Milvus
  // already wrote and leaves them alone (decision 22).
  private val writesSystemFields: Boolean = sparkSchema.fields.exists(f =>
    f.metadata.contains(FieldMetadata.MilvusPrimaryKeyMetadataKey) &&
      f.metadata.getBoolean(FieldMetadata.MilvusPrimaryKeyMetadataKey)
  )
  private val jobTimestamp: Long =
    HybridTimestamp.ofMillis(System.currentTimeMillis())
  private var writtenRows: Long = 0L

  private val arrowSchema =
    WriteSchema.arrowSchemaFor(sparkSchema, fieldIds, writesSystemFields)

  // The segment directory: `milvus.writer.customPath` names it outright
  // (backfill writes into an existing segment's base path); otherwise the job
  // writes under its staging prefix. Both are keys relative to the bucket.
  private val basePath: String =
    OptionParsing
      .value(milvusOption.options, MilvusOption.WriterCustomPath)
      .flatMap(Option(_))
      .map(_.trim)
      .filter(_.nonEmpty) match {
      case Some(customPath) =>
        logInfo(s"Using custom write path: $customPath")
        customPath
      case None =>
        val path = StagingLayout(StagingLayout.rootFor(storage), jobId)
          .segment(partitionId, taskId)
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
              new String(v.get(i), StandardCharsets.UTF_8)
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
      val directory = s"$basePath/_stats/bloom_filter.${stats.fieldId}"
      val path = s"$directory/${System.currentTimeMillis()}"
      val store = NativeObjectStore.Factory(storageProperties).open()
      try {
        // The store does not create parent directories: object storage has
        // none, the local backend needs them (io.ObjectStore).
        store.createDir(directory, recursive = true)
        store.write(path, bytes)
      } finally store.close()
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
    // Decision 22: a segment this connector writes carries the two system
    // columns Milvus's QueryNode looks for, with placeholder values — the row's
    // place in the segment and the job's time as a HybridTS. Milvus discards
    // RowID on load, and the timestamp only has to be older than any delete.
    if (writesSystemFields) {
      root
        .getVector(SchemaMapper.RowIdFieldId.toString)
        .asInstanceOf[BigIntVector]
        .setSafe(currentBatchSize, writtenRows + currentBatchSize)
      root
        .getVector(SchemaMapper.TimestampFieldId.toString)
        .asInstanceOf[BigIntVector]
        .setSafe(currentBatchSize, jobTimestamp)
    }
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
          val change = OptionParsing
            .value(milvusOption.options, MilvusOption.WriterCommitType)
            .flatMap(Option(_))
            .map(_.trim) match {
            case Some(value) if value.equalsIgnoreCase("addfield") =>
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
            storageProperties,
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

  override def currentMetricsValues(): Array[CustomTaskMetric] =
    WriteMetricsReport.taskValues(
      if (segmentWriter != null) segmentWriter.metrics else WriteMetrics.Zero
    )

  private def flushBatch(): Unit = {
    if (currentBatchSize == 0) return
    root.setRowCount(currentBatchSize)
    if (segmentWriter == null) openSegmentWriter(Some(root))
    collectPrimaryKeys(root)
    segmentWriter.write(root)
    writtenRows += currentBatchSize.toLong
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
