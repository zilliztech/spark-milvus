package com.zilliz.spark.connector.write

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BaseVariableWidthVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.write.exec.V2SegmentWriter
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.ArrowConverter
import com.zilliz.spark.connector.types.SparkSchemaMapper

/** Describes one parquet file produced by a backfill write into a StorageV2
  * (non-manifest packed parquet) segment.
  *
  * @param fieldId
  *   Milvus field ID of the new column.
  * @param logId
  *   The `logID` we allocated for this file (also the last path segment).
  * @param path
  *   Bucket-relative path under `insert_log/...` ready to be stored back in the
  *   snapshot AVRO. Does NOT include scheme or bucket prefix.
  * @param rowsWritten
  *   Row count actually written (for validation / result JSON).
  */
case class V2BinlogFile(
    fieldId: Long,
    logId: Long,
    path: String,
    rowsWritten: Long
)

object MilvusV2Writer {
  private[connector] def parseVariableWidthBytesPerValue(
      options: scala.collection.Map[String, String]
  ): Double =
    MilvusV3PartitionWriter.parsePositiveDoubleOption(
      options,
      MilvusOption.WriterVariableWidthBytesPerValue,
      defaultValue = 32.0
    )
}

/** Writes StorageV2 per-field binlog parquet files for a single segment.
  *
  * Backfill always emits **single-field column groups**, so the writer creates
  * one parquet file per new field at
  * {{{
  *   {rootPath}/insert_log/{coll}/{part}/{seg}/{fieldID}/{logID}
  * }}}
  *
  * Files are produced via milvus-storage's `loon_packed_writer_*` (the C++
  * `PackedRecordBatchWriter`), so the on-disk footer carries milvus-storage's
  * full KV metadata trio:
  *   - `row_group_metadata` (per-row-group memsize/rownum/offset)
  *   - `storage_version = "1.0.0"`
  *   - `group_field_id_list = "{fieldID}"`
  * matching what Milvus's compaction worker produces — so the resulting segment
  * loads cleanly via `FileRowGroupReader::Make` / `PackedFileMetadata::Make`.
  *
  * The writer is NOT thread-safe; create one per segment on the executor.
  *
  * Rows must arrive with fields in the same positional order as `targetSchema`.
  */
class MilvusV2Writer(
    collectionId: Long,
    partitionId: Long,
    segmentId: Long,
    newFieldNames: Seq[String], // positional in targetSchema
    newFieldIds: Seq[Long], // same size as newFieldNames
    targetSchema: StructType, // one StructField per new field, no metadata cols
    milvusOption: MilvusOption,
    allocateLogId: () => Long
) extends Logging {

  require(
    newFieldNames.size == newFieldIds.size && newFieldIds.size == targetSchema.fields.length,
    s"newFieldNames/newFieldIds/targetSchema size mismatch: " +
      s"${newFieldNames.size} / ${newFieldIds.size} / ${targetSchema.fields.length}"
  )

  private val variableWidthBytesPerValue: Double =
    MilvusV2Writer.parseVariableWidthBytesPerValue(milvusOption.options)

  // Storage root comes from the same FS config the V3 writer already consumes
  // — see com.zilliz.milvus.storage.credential.StorageProperties. Paths passed to the
  // native writer are bucket-relative keys: milvus-storage's FilesystemCache
  // wraps the raw S3 filesystem in a FileSystemProxy(bucket, s3_fs) subtree,
  // so the bucket is prepended automatically. Passing "<bucket>/<key>" would
  // produce doubled-bucket paths like "a-bucket/a-bucket/files/...".
  private val rootPath: String = milvusOption.options
    .getOrElse(StorageProperties.RootPath, "files")
    .stripSuffix("/")

  private case class PerFieldEntry(
      fieldId: Long,
      logId: Long,
      bucketRelativePath: String
  )

  private val fields: Array[PerFieldEntry] = newFieldNames.indices.map { idx =>
    val fid = newFieldIds(idx)
    val logId = allocateLogId()
    val bucketRelative =
      s"$rootPath/insert_log/$collectionId/$partitionId/$segmentId/$fid/$logId"
    PerFieldEntry(fid, logId, bucketRelative)
  }.toArray

  private val allocator = new RootAllocator(Long.MaxValue)
  private val fieldNameToId: Map[String, Long] =
    newFieldNames.zip(newFieldIds).toMap
  // V2 parquet written by Milvus names its columns by the logical field name
  // (with `PARQUET:field_id` metadata), and the V2 reader matches on that, so
  // the writer keeps the logical name rather than V3's id-as-name.
  private val arrowSchema =
    SparkSchemaMapper.convertSparkSchemaToArrow(
      targetSchema,
      vectorDimensions = Map.empty,
      fieldIds = fieldNameToId,
      useFieldIdAsName = false
    )
  // Single-field column groups: one parquet file per field, in schema order.
  private val segmentWriter = new V2SegmentWriter(
    paths = fields.map(_.bucketRelativePath).toSeq,
    columnGroups = newFieldNames.indices.map(Seq(_)),
    arrowSchema = arrowSchema,
    properties = MilvusOption.writerProperties(milvusOption),
    allocator = allocator
  )
  logInfo(
    s"V2 packed writer opened: segment=$segmentId, fields=${newFieldIds.mkString(",")}, " +
      s"paths=${fields.map(_.bucketRelativePath).mkString("[", ", ", "]")}"
  )

  private val batchSize: Int =
    if (milvusOption.insertMaxBatchSize > 0) milvusOption.insertMaxBatchSize
    else 5000
  private var root: VectorSchemaRoot =
    VectorSchemaRoot.create(arrowSchema, allocator)
  allocateVectors(root)
  private var currentBatchSize: Int = 0
  private var totalRows: Long = 0L
  private var closed: Boolean = false

  /** Consume one Spark row. Each field in `row` must be at the same positional
    * index as its entry in `targetSchema`.
    */
  def write(row: InternalRow): Unit = {
    if (closed) throw new IllegalStateException("writer already closed")
    ArrowConverter.internalRowToArrow(root, currentBatchSize, row, targetSchema)
    currentBatchSize += 1
    root.setRowCount(currentBatchSize)
    if (currentBatchSize >= batchSize) {
      flushBatch()
    }
  }

  /** Close all per-field writers and return the list of produced binlog files.
    */
  def close(): Seq[V2BinlogFile] = {
    if (closed) {
      return fields.map(f =>
        V2BinlogFile(f.fieldId, f.logId, f.bucketRelativePath, totalRows)
      )
    }
    var firstErr: Throwable = null
    try {
      if (currentBatchSize > 0) flushBatch()
      totalRows = segmentWriter.finish()
    } catch {
      case e: Throwable => firstErr = e
    } finally {
      cleanup()
      closed = true
    }
    if (firstErr != null) throw firstErr

    // Every field receives the same row count: each Spark row produces one
    // value in every field's parquet (single-field column groups).
    fields
      .map(f =>
        V2BinlogFile(f.fieldId, f.logId, f.bucketRelativePath, totalRows)
      )
      .toSeq
  }

  /** Abort: best-effort destroy + resource release. Errors are logged, not
    * thrown — the caller's surrounding `try`/`catch` should surface the
    * original exception, not a secondary abort failure.
    */
  def abort(): Unit = {
    if (closed) return
    cleanup()
    closed = true
  }

  // -------------------------------------------------------------------------

  private def flushBatch(): Unit = {
    if (currentBatchSize == 0) return
    root.setRowCount(currentBatchSize)
    segmentWriter.write(root)
    totalRows += currentBatchSize
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
    Try(oldRoot.close()).failed.foreach(e =>
      logError(
        s"error closing old VectorSchemaRoot after flush: ${e.getMessage}"
      )
    )
  }

  private def allocateVectors(r: VectorSchemaRoot): Unit = {
    import scala.collection.JavaConverters._
    r.getFieldVectors.asScala.foreach {
      case v: VarCharVector =>
        // Second arg is density (bytes per value), NOT total bytes.
        // Total buffer = batchSize × density. Passing batchSize*32 here would
        // give batchSize² × 32 bytes — a quadratic over-allocation that dwarfs
        // actual data (20-byte values) by ~1000× and made a single allocator
        // peak ~1 GiB for a 20k-row segment.
        v.setInitialCapacity(batchSize, variableWidthBytesPerValue)
      case v: BaseVariableWidthVector =>
        v.setInitialCapacity(batchSize, variableWidthBytesPerValue)
      case other =>
        other.setInitialCapacity(batchSize)
    }
    r.allocateNew()
    r.setRowCount(0)
  }

  private def cleanup(): Unit = {
    // The writer first: destroying it releases the buffers C++ still pins,
    // and only then is the allocator free to close.
    Try(segmentWriter.close()).failed.foreach(e =>
      logError(s"error closing the segment writer: ${e.getMessage}")
    )
    Try(if (root != null) root.close()).failed.foreach(e =>
      logError(s"error closing current root: ${e.getMessage}")
    )
    Try(allocator.close()).failed.foreach(e =>
      logError(s"error closing allocator: ${e.getMessage}")
    )
  }
}
