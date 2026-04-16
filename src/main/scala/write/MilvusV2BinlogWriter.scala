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
  // it via Arrow C Data Interface, hands the cArray to the C++ writer, then
  // immediately closes the source root. The export retains each buffer on
  // its own (verified in ArrowCDataRefcountTest across every backfill-
  // relevant field type), so closing the source only drops the JVM-side ref —
  // C++'s cached RecordBatch shared_ptr keeps the buffers alive and fires the
  // release callback when it drops. This bounds per-writer direct memory to
  // ~16 MB × numGroups, down from O(segment rows) that the old "retain every
  // root until writer.close()" strategy incurred.
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
    } finally {
      // The C++ writer's ImportRecordBatch moves the release callback out of
      // this struct on success; on failure the release is still here and
      // closing will fire it, dropping the export-side ref (source root still
      // holds one ref, so buffers remain alive until abort/cleanup runs).
      cArray.close()
    }

    // Fully build + allocate the replacement root BEFORE swapping `root`, so
    // that if allocation throws, the field still points at the old root and
    // cleanup() can release it. Otherwise an allocation failure mid-swap would
    // leak the half-allocated newRoot and forget the old one.
    //
    // Old root's buffers are now referenced only by C++ via the export's
    // retained ref, so closing `oldRoot` at the end just drops the JVM ref —
    // buffers stay alive until C++ flushes the cached shared_ptr.
    val newRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      allocateVectors(newRoot)
    } catch {
      case t: Throwable =>
        Try(newRoot.close()).failed.foreach(e =>
          logError(
            s"error closing newRoot after allocation failure: ${e.getMessage}"
          )
        )
        throw t
    }
    val oldRoot = root
    root = newRoot
    currentBatchSize = 0
    // Best-effort: the native write already succeeded and the batch is durable.
    // Throwing out of flushBatch here would escape to Spark, trigger task retry,
    // and duplicate the write. Log and continue — the allocator will still
    // reclaim buffers once the C++ writer flushes its cached shared_ptrs.
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
        v.setInitialCapacity(batchSize, 32.0)
      case v: BaseVariableWidthVector =>
        v.setInitialCapacity(batchSize, 32.0)
      case other =>
        other.setInitialCapacity(batchSize)
    }
    r.allocateNew()
    r.setRowCount(0)
  }

  private def cleanup(): Unit = {
    // Destroying the writer first drops all C++-cached RecordBatch shared_ptrs,
    // which fires every outstanding release callback and returns the buffers
    // they were pinning to the allocator. Only then is it safe to close the
    // allocator.
    Try(writer.destroy()).failed.foreach(e =>
      logError(s"error destroying packed writer: ${e.getMessage}")
    )
    Try(if (root != null) root.close()).failed.foreach(e =>
      logError(s"error closing current root: ${e.getMessage}")
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
