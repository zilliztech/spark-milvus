package com.zilliz.spark.connector.write

import scala.collection.mutable
import scala.util.Try

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BaseVariableWidthVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import com.zilliz.spark.connector.{MilvusOption, MilvusSchemaUtil}
import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.serde.ArrowConverter
import io.milvus.storage.{MilvusPackedWriter, MilvusStorageProperties}

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

/** Writes StorageV2 per-field binlog parquet files for a single segment.
  *
  * Backfill always emits **single-field column groups**, so the writer creates
  * one parquet file per new field at
  * {{{
  *   {rootPath}/insert_log/{coll}/{part}/{seg}/{fieldID}/{logID}
  * }}}
  *
  * Files are produced via milvus-storage's
  * [[io.milvus.storage.MilvusPackedWriter]] (the JNI wrapper around C++
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
class MilvusV2BinlogWriter(
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

  // Storage root comes from the same FS config the V3 writer already consumes
  // — see com.zilliz.spark.connector.loon.Properties. Paths passed to the
  // native writer are bucket-relative keys: milvus-storage's FilesystemCache
  // wraps the raw S3 filesystem in a FileSystemProxy(bucket, s3_fs) subtree,
  // so the bucket is prepended automatically. Passing "<bucket>/<key>" would
  // produce doubled-bucket paths like "a-bucket/a-bucket/files/...".
  private val rootPath: String = milvusOption.options
    .getOrElse(Properties.FsConfig.FsRootPath, "files")
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

  // Allocator + Arrow schema for the WHOLE row (all N new fields together).
  // The packed writer splits per column group internally, so we feed it
  // batches that contain every column.
  private val allocator = new RootAllocator(Long.MaxValue)
  private val fieldNameToId: Map[String, Long] =
    newFieldNames.zip(newFieldIds).toMap
  private val arrowSchema =
    MilvusSchemaUtil.convertSparkSchemaToArrow(
      targetSchema,
      vectorDimensions = Map.empty,
      fieldIds = fieldNameToId
    )

  // Single-field column groups: one parquet output per field, paths in the
  // same order as `targetSchema.fields`.
  private val columnGroups: Array[Array[Int]] =
    Array.tabulate(newFieldNames.length)(i => Array(i))
  private val outputPaths: Array[String] = fields.map(_.bucketRelativePath)

  // Build native properties from the carrier MilvusOption (same plumbing the
  // V3 writer uses).
  private val nativeProperties: MilvusStorageProperties =
    Properties.fromMilvusOption(milvusOption)

  // Open the native writer.
  private val arrowSchemaC: ArrowSchema = ArrowSchema.allocateNew(allocator)
  Data.exportSchema(allocator, arrowSchema, null, arrowSchemaC)

  private val writer: MilvusPackedWriter = {
    val w = new MilvusPackedWriter()
    try {
      w.create(
        outputPaths,
        columnGroups,
        arrowSchemaC.memoryAddress(),
        nativeProperties,
        bufferSize = 0L
      )
    } catch {
      case e: Throwable =>
        // Cleanup partial state before rethrowing — the caller's `abort()` won't
        // run if the constructor itself threw.
        Try(arrowSchemaC.close())
        Try(nativeProperties.free())
        Try(allocator.close())
        throw e
    }
    w
  }
  logInfo(
    s"V2 packed writer opened: segment=$segmentId, fields=${newFieldIds.mkString(",")}, " +
      s"paths=${outputPaths.mkString("[", ", ", "]")}"
  )

  // Batch accumulation. The current `root` collects rows; each flush exports
  // it via the Arrow C Data Interface. The C++ writer caches RecordBatch
  // shared_ptrs that wrap the JVM-owned buffers (zero-copy), so we MUST
  // keep flushed roots alive until the writer is closed — same trap
  // documented in MilvusLoonPartitionWriter.
  private val batchSize: Int =
    if (milvusOption.insertMaxBatchSize > 0) milvusOption.insertMaxBatchSize
    else 5000
  private var root: VectorSchemaRoot =
    VectorSchemaRoot.create(arrowSchema, allocator)
  allocateVectors(root)
  private var currentBatchSize: Int = 0
  private var totalRows: Long = 0L
  private val exportedRoots = new java.util.ArrayList[VectorSchemaRoot]()
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
      writer.close()
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

    val cArray = ArrowArray.allocateNew(allocator)
    try {
      Data.exportVectorSchemaRoot(allocator, root, null, cArray)
      writer.write(cArray.memoryAddress())
      totalRows += currentBatchSize

      // CRITICAL: do NOT reuse `root`. C++ caches RecordBatch ptrs into
      // these buffers. Hand the old root off to `exportedRoots` (closed
      // after writer.close()) and create a fresh one for subsequent rows.
      exportedRoots.add(root)
      root = VectorSchemaRoot.create(arrowSchema, allocator)
      allocateVectors(root)
      currentBatchSize = 0
    } finally {
      cArray.close()
    }
  }

  private def allocateVectors(r: VectorSchemaRoot): Unit = {
    import scala.collection.JavaConverters._
    r.getFieldVectors.asScala.foreach {
      case v: VarCharVector =>
        v.setInitialCapacity(batchSize, batchSize.toLong * 32L)
      case v: BaseVariableWidthVector =>
        v.setInitialCapacity(batchSize, batchSize.toLong * 32L)
      case other =>
        other.setInitialCapacity(batchSize)
    }
    r.allocateNew()
    r.setRowCount(0)
  }

  private def cleanup(): Unit = {
    Try(writer.destroy()).failed.foreach(e =>
      logError(s"error destroying packed writer: ${e.getMessage}")
    )
    Try(if (root != null) root.close()).failed.foreach(e =>
      logError(s"error closing current root: ${e.getMessage}")
    )
    Try {
      exportedRoots.forEach(r => Try(r.close()))
      exportedRoots.clear()
    }.failed.foreach(e =>
      logError(s"error closing exported roots: ${e.getMessage}")
    )
    Try(if (arrowSchemaC != null) arrowSchemaC.close()).failed.foreach(e =>
      logError(s"error closing ArrowSchema: ${e.getMessage}")
    )
    Try(nativeProperties.free()).failed.foreach(e =>
      logError(s"error freeing native properties: ${e.getMessage}")
    )
    Try(allocator.close()).failed.foreach(e =>
      logError(s"error closing allocator: ${e.getMessage}")
    )
  }
}
