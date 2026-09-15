package com.zilliz.milvus.storage.read.exec

import scala.collection.JavaConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.jni.storage.StorageNative
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.Logging
import com.zilliz.milvus.storage.snapshot.SegmentLayout

/** Pulls Arrow batches out of one segment.
  *
  * The only thing in core that opens a native handle. A batch arrives as a
  * freshly owned `VectorSchemaRoot`: the caller closes each one and closes the
  * reader when it is done.
  */
trait SegmentReader extends AutoCloseable {

  /** The next batch, or nothing at end of stream. */
  def next(): Option[VectorSchemaRoot]

  /** Rows handed over so far. A caller that knows how many to expect compares
    * the two at the end: reading fewer than promised has to be an error, not a
    * short result.
    */
  def deliveredRows: Long
}

/** Opens a [[SegmentReader]] for a segment's layout.
  *
  * Dispatch lives here rather than in the Spark layer, so the two layouts have
  * one entry point instead of a branch per call site.
  *
  * Column naming is the caller's, and the two lines genuinely differ: a
  * manifest matches columns by field id and its schema carries ids as names,
  * while a column-group layout uses the field's own name. That is what
  * `columnNameFor` is for; it is not something this object can decide.
  */
object SegmentReaderRegistry {

  def open(
      task: SegmentReadTask,
      arrowSchema: Schema,
      neededColumns: Seq[String],
      columnNameFor: Long => Option[String],
      allocator: BufferAllocator
  ): SegmentReader =
    new NativeSegmentReader(
      task,
      arrowSchema,
      neededColumns,
      columnNameFor,
      allocator
    )
}

/** [[SegmentReader]] over milvus-storage's C reader.
  *
  * Three rules this class exists to keep, all of them from getting them wrong
  * once (see docs/design/architecture/storage-io.html section 3):
  *
  *   - A handle never crosses a serialization boundary. It is opened here, on
  *     the executor, from the description in `task`.
  *   - A constructor that throws releases what it already took. Nothing calls
  *     `close()` on an object that never finished being built.
  *   - `close()` is idempotent, because an error path may call it twice.
  */
private[exec] final class NativeSegmentReader(
    task: SegmentReadTask,
    arrowSchema: Schema,
    neededColumns: Seq[String],
    columnNameFor: Long => Option[String],
    allocator: BufferAllocator
) extends SegmentReader
    with Logging {

  private var schemaStruct: ArrowSchema = null
  private var manifestHandle: Long = 0L
  private var columnGroupsHandle: Long = 0L
  private var ownsColumnGroups: Boolean = false
  private var readerHandle: Long = 0L
  private var batchReaderHandle: Long = 0L
  private var closed: Boolean = false
  private var delivered: Long = 0L

  try {
    schemaStruct = ArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, arrowSchema, null, schemaStruct)
    val properties = task.properties.asJava
    val columns = neededColumns.toArray

    task.layout match {
      case SegmentLayout.Manifest(basePath, readVersion) =>
        val manifest =
          StorageNative.manifestOpen(basePath, properties, readVersion)
        manifestHandle = manifest(0)
        columnGroupsHandle = manifest(1)
        if (manifest(2) == 0L) {
          throw new IllegalStateException(
            s"no manifest at $basePath; the segment has not been compacted " +
              "into the milvus-storage format"
          )
        }
        readerHandle = StorageNative.readerNewNative(
          columnGroupsHandle,
          schemaStruct.memoryAddress(),
          columns,
          properties
        )

      case SegmentLayout.ColumnGroups(groups) =>
        val columnNames =
          // `.toList` rather than passing columnNameFor straight to flatMap:
          // Scala 2.12 does not convert an Option result of a function value,
          // and the 3.5 line cross-compiles for 2.12.
          groups
            .map(_.fieldIds.flatMap(id => columnNameFor(id).toList).toArray)
            .toArray
        val files = groups.map(_.filePaths.toArray).toArray
        val rowCounts = groups.map { group =>
          require(
            group.fileRowCounts.size == group.filePaths.size,
            s"column group with fields ${group.fieldIds} has " +
              s"${group.filePaths.size} files but " +
              s"${group.fileRowCounts.size} row counts"
          )
          group.fileRowCounts.toArray
        }.toArray
        columnGroupsHandle = StorageNative.columnGroupsCreate(
          columnNames,
          files,
          rowCounts,
          "parquet"
        )
        ownsColumnGroups = true
        readerHandle = StorageNative.readerNew(
          columnGroupsHandle,
          schemaStruct.memoryAddress(),
          columns,
          properties
        )
    }

    if (readerHandle == 0L) {
      throw new IllegalStateException(
        s"could not open a native reader for segment ${task.segmentId}"
      )
    }
    batchReaderHandle = StorageNative.recordBatchReaderNew(readerHandle, null)
  } catch {
    case e: Throwable =>
      release()
      throw e
  }

  override def next(): Option[VectorSchemaRoot] = {
    if (batchReaderHandle == 0L) return None
    val array = ArrowArray.allocateNew(allocator)
    val schema = ArrowSchema.allocateNew(allocator)
    try {
      val hasBatch = StorageNative.recordBatchReaderReadNext(
        batchReaderHandle,
        array.memoryAddress(),
        schema.memoryAddress()
      )
      if (!hasBatch) None
      else {
        val root = Data.importVectorSchemaRoot(allocator, array, schema, null)
        delivered += root.getRowCount.toLong
        Some(root)
      }
    } finally {
      // The two structs are shells. Importing moved the data to the root, whose
      // buffers the allocator owns; closing them here does not touch it.
      array.close()
      schema.close()
    }
  }

  override def deliveredRows: Long = delivered

  override def close(): Unit = release()

  /** Releases everything still held, in reverse order of acquisition, and
    * survives being called twice or part-way through construction.
    */
  private def release(): Unit = synchronized {
    if (closed) return
    closed = true

    if (batchReaderHandle != 0L) {
      try StorageNative.recordBatchReaderDestroy(batchReaderHandle)
      catch {
        case e: Throwable => logWarning("destroying the batch reader failed", e)
      }
      batchReaderHandle = 0L
    }
    if (readerHandle != 0L) {
      try StorageNative.readerDestroySegment(readerHandle)
      catch {
        case e: Throwable => logWarning("destroying the reader failed", e)
      }
      readerHandle = 0L
    }
    if (ownsColumnGroups && columnGroupsHandle != 0L) {
      try StorageNative.columnGroupsDestroy(columnGroupsHandle)
      catch {
        case e: Throwable =>
          logWarning("destroying the column groups failed", e)
      }
    }
    if (manifestHandle != 0L) {
      // The manifest owns the column groups it handed out, so destroying it
      // covers both. The code this replaces never did, leaking one manifest
      // per partition.
      try StorageNative.manifestDestroy(manifestHandle)
      catch {
        case e: Throwable => logWarning("destroying the manifest failed", e)
      }
      manifestHandle = 0L
    }
    columnGroupsHandle = 0L
    if (schemaStruct != null) {
      try schemaStruct.close()
      catch {
        case e: Throwable => logWarning("closing the arrow schema failed", e)
      }
      schemaStruct = null
    }
  }
}
