package com.zilliz.milvus.storage.read.exec

import scala.collection.JavaConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.jni.storage.StorageNative
import com.zilliz.milvus.storage.{Logging, NativeCalls}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
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

  /** Retrieves physical rows in strictly increasing, unique index order. Empty
    * `columns` uses this reader's default projection. Empty indices return no
    * batches without opening a native take operation.
    *
    * The result owns the batches not yet handed to the caller, independently of
    * this reader. Close the result even when stopping early, and close each
    * returned root separately. The allocator must outlive both.
    */
  def take(
      rowIndices: Array[Long],
      columns: Seq[String],
      parallelism: Int = 1
  ): SegmentReader.TakeResult

  /** Rows handed over by sequential `next()` calls. The registry wraps a reader
    * when its task declares an expected count, so reaching EOF with another
    * count is an error rather than a partial result. `take` does not consume
    * the sequential stream or contribute to this count.
    */
  def deliveredRows: Long

  /** What the read has cost on the crossing so far; valid after `close()` too,
    * for a task that reports at its end.
    */
  def metrics: ReadMetrics
}

object SegmentReader {

  /** Selected Arrow batches. `next` transfers one root to the caller; `close`
    * releases only the batches still owned by this result and is idempotent.
    */
  trait TakeResult extends AutoCloseable {
    def next(): Option[VectorSchemaRoot]
  }

  private[exec] object EmptyTakeResult extends TakeResult {
    override def next(): Option[VectorSchemaRoot] = None
    override def close(): Unit = ()
  }

  /** Imports one native batch and releases both C struct shells on every path.
    * A failed import also releases any C buffers not transferred to a root.
    */
  private[exec] def readBatch(
      handle: Long,
      allocator: BufferAllocator,
      calls: NativeCalls
  ): Option[VectorSchemaRoot] = {
    val array = ArrowArray.allocateNew(allocator)
    var schema: ArrowSchema = null
    try {
      schema = ArrowSchema.allocateNew(allocator)
      schema.save(new ArrowSchema.Snapshot())
      val hasBatch = calls.timed(
        StorageNative.recordBatchReaderReadNext(
          handle,
          array.memoryAddress(),
          schema.memoryAddress()
        )
      )
      if (!hasBatch) None
      else {
        // Arrow's import routines close their input wrappers after consuming
        // the callbacks. Borrowed wrappers keep our struct allocations alive
        // until finally, including when import fails before the move.
        val importedSchema = Data.importSchema(
          allocator,
          ArrowSchema.wrap(schema.memoryAddress()),
          null
        )
        val root = VectorSchemaRoot.create(importedSchema, allocator)
        try {
          Data.importIntoVectorSchemaRoot(
            allocator,
            ArrowArray.wrap(array.memoryAddress()),
            root,
            null
          )
          Some(root)
        } catch {
          case failure: Throwable =>
            try root.close()
            catch {
              case closeFailure: Throwable =>
                failure.addSuppressed(closeFailure)
            }
            throw failure
        }
      }
    } finally {
      // Import moves the array callback and releases the schema. Failure may
      // leave either callback here; close alone frees only the struct shell.
      try {
        if (array.snapshot().release != 0L) array.release()
      } finally {
        try array.close()
        finally {
          if (schema != null) {
            try {
              if (schema.snapshot().release != 0L) schema.release()
            } finally schema.close()
          }
        }
      }
    }
  }
}

private[exec] final class NativeTakeResult(
    private var handle: Long,
    allocator: BufferAllocator,
    calls: NativeCalls,
    recordBatch: VectorSchemaRoot => Unit,
    recordCopies: (Long, Long) => Unit
) extends SegmentReader.TakeResult {

  private var reportedCopies: Long = 0L
  private var reportedBytes: Long = 0L

  override def next(): Option[VectorSchemaRoot] = synchronized {
    if (handle == 0L) return None
    var batch: Option[VectorSchemaRoot] = None
    try {
      batch = SegmentReader.readBatch(handle, allocator, calls)
      reportStats()
      batch.foreach(recordBatch)
      if (batch.isEmpty) close()
      batch
    } catch {
      case failure: Throwable =>
        batch.foreach { root =>
          try root.close()
          catch {
            case closeFailure: Throwable => failure.addSuppressed(closeFailure)
          }
        }
        try close()
        catch {
          case closeFailure: Throwable => failure.addSuppressed(closeFailure)
        }
        throw failure
    }
  }

  override def close(): Unit = synchronized {
    if (handle != 0L) {
      val owned = handle
      try reportStats()
      finally {
        handle = 0L
        calls.timed(StorageNative.recordBatchReaderDestroy(owned))
      }
    }
  }

  private def reportStats(): Unit = {
    val stats = StorageNative.recordBatchReaderStats(handle)
    recordCopies(stats(1) - reportedCopies, stats(2) - reportedBytes)
    reportedCopies = stats(1)
    reportedBytes = stats(2)
  }
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
  ): SegmentReader = {
    val reader = new NativeSegmentReader(
      task,
      arrowSchema,
      neededColumns,
      columnNameFor,
      allocator
    )
    withExpectedRows(task, reader)
  }

  /** Apply the task's EOF row-count contract to any reader implementation. Kept
    * separate from the native reader so row and columnar Spark consumers get
    * the same check, and so the contract is testable without JNI.
    */
  private[exec] def withExpectedRows(
      task: SegmentReadTask,
      reader: SegmentReader
  ): SegmentReader =
    task.expectedRows match {
      case Some(expected) =>
        new ExpectedRowsSegmentReader(reader, task.segmentId, expected)
      case None => reader
    }
}

/** Verifies a physical row count only when the delegate reaches EOF. Closing a
  * reader early is valid for a limit and deliberately does not run the check.
  */
private[exec] final class ExpectedRowsSegmentReader(
    delegate: SegmentReader,
    segmentId: Long,
    expectedRows: Long
) extends SegmentReader {

  private var verified: Boolean = false

  override def next(): Option[VectorSchemaRoot] = {
    val batch = delegate.next()
    if (batch.isEmpty) verify()
    batch
  }

  override def take(
      rowIndices: Array[Long],
      columns: Seq[String],
      parallelism: Int
  ): SegmentReader.TakeResult = delegate.take(rowIndices, columns, parallelism)

  override def deliveredRows: Long = delegate.deliveredRows

  override def metrics: ReadMetrics = delegate.metrics

  override def close(): Unit = delegate.close()

  private def verify(): Unit = {
    if (verified) return
    if (deliveredRows != expectedRows) {
      throw new IllegalStateException(
        s"segment $segmentId reader delivered $deliveredRows physical rows, " +
          s"expected $expectedRows; refusing to return a partial or " +
          "corrupt result"
      )
    }
    verified = true
  }
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

  private val calls = new NativeCalls
  private var batches: Long = 0L
  private var arrowBytes: Long = 0L
  private var allocatedMax: Long = 0L
  private var takeCopies: Long = 0L
  private var takeCopiedBytes: Long = 0L
  // The C side's counters, read while the handle lives and kept past close.
  private var nativeStats: Array[Long] = Array(0L, 0L, 0L)

  try {
    schemaStruct = ArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, arrowSchema, null, schemaStruct)
    val properties = task.properties.asJava
    val columns = neededColumns.toArray

    task.layout match {
      case SegmentLayout.Manifest(basePath, readVersion) =>
        val manifest = calls.timed(
          StorageNative.manifestOpen(basePath, properties, readVersion)
        )
        manifestHandle = manifest(0)
        columnGroupsHandle = manifest(1)
        if (manifest(2) == 0L) {
          throw new IllegalStateException(
            s"no manifest at $basePath; the segment has not been compacted " +
              "into the milvus-storage format"
          )
        }
        readerHandle = calls.timed(
          StorageNative.readerNewNative(
            columnGroupsHandle,
            schemaStruct.memoryAddress(),
            columns,
            properties
          )
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
        columnGroupsHandle = calls.timed(
          StorageNative.columnGroupsCreate(
            columnNames,
            files,
            rowCounts,
            "parquet"
          )
        )
        ownsColumnGroups = true
        readerHandle = calls.timed(
          StorageNative.readerNew(
            columnGroupsHandle,
            schemaStruct.memoryAddress(),
            columns,
            properties
          )
        )
    }

    if (readerHandle == 0L) {
      throw new IllegalStateException(
        s"could not open a native reader for segment ${task.segmentId}"
      )
    }
  } catch {
    case e: Throwable =>
      release()
      throw e
  }

  override def next(): Option[VectorSchemaRoot] = {
    if (closed) return None
    // A take-only caller must not open the sequential stream or start reading
    // the default projection, which can include every raw vector in the segment.
    if (batchReaderHandle == 0L) {
      batchReaderHandle = calls.timed(
        StorageNative.recordBatchReaderNew(readerHandle, null)
      )
    }
    val batch = SegmentReader.readBatch(batchReaderHandle, allocator, calls)
    batch.foreach { root =>
      delivered += root.getRowCount.toLong
      recordBatch(root)
    }
    batch
  }

  override def take(
      rowIndices: Array[Long],
      columns: Seq[String],
      parallelism: Int
  ): SegmentReader.TakeResult = {
    if (closed) throw new IllegalStateException("segment reader is closed")
    require(rowIndices != null, "row indices must not be null")
    require(columns != null, "columns must not be null")
    require(
      columns.forall(name => name != null && name.nonEmpty),
      "column names must not be null or empty"
    )
    require(parallelism > 0, "take parallelism must be positive")
    var previous = -1L
    rowIndices.foreach { index =>
      require(
        index >= 0L && index > previous,
        "row indices must be nonnegative, sorted and unique"
      )
      previous = index
    }
    if (rowIndices.isEmpty) return SegmentReader.EmptyTakeResult
    task.expectedRows.foreach { rows =>
      require(
        rowIndices.last < rows,
        s"row index ${rowIndices.last} is outside segment row count $rows"
      )
    }
    val handle = calls.timed(
      StorageNative.readerTake(
        readerHandle,
        rowIndices,
        columns.toArray,
        parallelism
      )
    )
    if (handle == 0L) {
      throw new IllegalStateException(
        s"take returned no reader for segment ${task.segmentId}"
      )
    }
    try
      new NativeTakeResult(
        handle,
        allocator,
        calls,
        recordBatch,
        (copies, bytes) => {
          takeCopies += copies
          takeCopiedBytes += bytes
        }
      )
    catch {
      case failure: Throwable =>
        calls.timed(StorageNative.recordBatchReaderDestroy(handle))
        throw failure
    }
  }

  override def deliveredRows: Long = delivered

  private def recordBatch(root: VectorSchemaRoot): Unit = {
    batches += 1
    val vectors = root.getFieldVectors
    var index = 0
    while (index < vectors.size()) {
      arrowBytes += vectors.get(index).getBufferSize.toLong
      index += 1
    }
    allocatedMax = math.max(allocatedMax, allocator.getAllocatedMemory)
  }

  override def metrics: ReadMetrics = synchronized {
    if (batchReaderHandle != 0L) {
      nativeStats = StorageNative.recordBatchReaderStats(batchReaderHandle)
    }
    ReadMetrics(
      jniCalls = calls.calls,
      jniNanos = calls.nanos,
      batches = batches,
      arrowBytes = arrowBytes,
      copies = nativeStats(1) + takeCopies,
      copiedBytes = nativeStats(2) + takeCopiedBytes,
      allocatedMax = allocatedMax
    )
  }

  override def close(): Unit = release()

  /** Releases everything still held, in reverse order of acquisition, and
    * survives being called twice or part-way through construction.
    */
  private def release(): Unit = synchronized {
    if (closed) return
    closed = true

    if (batchReaderHandle != 0L) {
      try nativeStats = StorageNative.recordBatchReaderStats(batchReaderHandle)
      catch {
        case e: Throwable =>
          logWarning("reading the batch reader stats failed", e)
      }
      try calls.timed(StorageNative.recordBatchReaderDestroy(batchReaderHandle))
      catch {
        case e: Throwable => logWarning("destroying the batch reader failed", e)
      }
      batchReaderHandle = 0L
    }
    if (readerHandle != 0L) {
      try calls.timed(StorageNative.readerDestroySegment(readerHandle))
      catch {
        case e: Throwable => logWarning("destroying the reader failed", e)
      }
      readerHandle = 0L
    }
    if (ownsColumnGroups && columnGroupsHandle != 0L) {
      try calls.timed(StorageNative.columnGroupsDestroy(columnGroupsHandle))
      catch {
        case e: Throwable =>
          logWarning("destroying the column groups failed", e)
      }
    }
    if (manifestHandle != 0L) {
      // The manifest owns the column groups it handed out, so destroying it
      // covers both. The code this replaces never did, leaking one manifest
      // per partition.
      try calls.timed(StorageNative.manifestDestroy(manifestHandle))
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
