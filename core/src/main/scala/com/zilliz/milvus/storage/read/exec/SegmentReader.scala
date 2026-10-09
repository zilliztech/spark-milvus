package com.zilliz.milvus.storage.read.exec

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.storage.{Logging, NativeCalls}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import io.milvus.storage.{
  MilvusStorageColumnGroups,
  MilvusStorageManifest,
  MilvusStorageManifestHandle,
  MilvusStorageProperties,
  MilvusStorageReader,
  MilvusStorageRuntime
}

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
      reader: MilvusStorageReader,
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
        reader.readNextBatchScala(
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
    reader: MilvusStorageReader,
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
      batch = SegmentReader.readBatch(reader, handle, allocator, calls)
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
        calls.timed(reader.destroyRecordBatchReaderScala(owned))
      }
    }
  }

  private def reportStats(): Unit = {
    val stats = reader.recordBatchReaderStatsScala(handle)
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
  private[exec] val RecordBatchMaxRows = "reader.record_batch_max_rows"
  private[exec] val RecordBatchMaxSize = "reader.record_batch_max_size"
  private[exec] val PrebufferLazy = "reader.parquet.prebuffer.lazy"
  private[exec] val PrebufferRangeSizeLimit =
    "reader.parquet.prebuffer.range_size_limit"

  /** Object storage requests one batch read makes at once.
    *
    * A batch is one Parquet read call over the row groups it covers. Arrow
    * coalesces their column chunks into ranges of at most the range size, and
    * each range is one GET on one connection, which moves 40 to 60 MB/s from
    * S3. Arrow's default cache requests the ranges one at a time as decoding
    * reaches them, so a task read at one connection's speed whatever the range
    * size; requested together, eight ranges use eight connections
    * (docs/design/architecture/search-resources.html section 3.6).
    */
  private[exec] val RangesPerBatch = 8

  /** The smallest range: below it the first-byte wait of each GET outweighs its
    * transfer.
    */
  private[exec] val MinRangeBytes: Long = 4L << 20

  /** The range size that cuts a batch of `batchMaxBytes` into
    * [[RangesPerBatch]] requests, never below [[MinRangeBytes]].
    */
  private[exec] def rangeBytes(batchMaxBytes: Long): Long =
    math.max(
      MinRangeBytes,
      (batchMaxBytes + RangesPerBatch - 1) / RangesPerBatch
    )

  /** Native properties for one validated task. Typed limits deliberately win
    * over any raw key in the filesystem property bag, and so do the range
    * settings: every range of a batch is requested when the read call starts.
    */
  private[exec] def nativeProperties(
      task: SegmentReadTask
  ): Map[String, String] =
    task.properties ++ Map(
      RecordBatchMaxRows -> task.limits.batchMaxRows.toString,
      RecordBatchMaxSize -> task.limits.batchMaxBytes.toString,
      PrebufferLazy -> "false",
      PrebufferRangeSizeLimit -> rangeBytes(task.limits.batchMaxBytes).toString
    )

  /** Threads Arrow's IO pool needs so that every task of this executor has all
    * ranges of its batch in flight. The pool runs each requested range as a
    * blocking GET on one of its threads, is shared by the whole process and
    * starts with 8; an executor runs at most one task per processor.
    */
  private[exec] def ioThreads(processors: Int): Int =
    Math.multiplyExact(processors, RangesPerBatch)

  /** Raise Arrow's IO pool to what this executor's tasks need, and never lower
    * it: the pool is process-wide, so another reader in this JVM may already
    * have asked for more. Upstream leaves that policy to the caller.
    */
  private def raiseIoThreads(wanted: Int): Unit = synchronized {
    if (MilvusStorageRuntime.ioThreadPoolCapacity < wanted)
      MilvusStorageRuntime.setArrowIoThreadPoolCapacity(wanted)
  }

  def open(
      task: SegmentReadTask,
      arrowSchema: Schema,
      neededColumns: Seq[String],
      columnNameFor: Long => Option[String],
      allocator: BufferAllocator
  ): SegmentReader = {
    NativeStorageLibrary.load()
    // Cheap, and a failure fails this read rather than leaving it at one
    // request at a time.
    raiseIoThreads(ioThreads(Runtime.getRuntime.availableProcessors()))
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
  private var manifest: MilvusStorageManifestHandle = null
  private var properties: MilvusStorageProperties = null
  private var columnGroupsHandle: Long = 0L
  private var ownsColumnGroups: Boolean = false
  private var reader: MilvusStorageReader = null
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
    schemaStruct.save(new ArrowSchema.Snapshot())
    Data.exportSchema(allocator, arrowSchema, null, schemaStruct)
    properties = calls.timed(new MilvusStorageProperties())
    calls.timed(properties.create(SegmentReaderRegistry.nativeProperties(task)))
    reader = new MilvusStorageReader()
    val columns = neededColumns.toArray

    task.layout match {
      case SegmentLayout.Manifest(basePath, readVersion) =>
        manifest = calls.timed(
          MilvusStorageManifest.open(basePath, properties, readVersion)
        )
        columnGroupsHandle = manifest.columnGroupsPtr
        if (manifest.readVersion == 0L) {
          throw new IllegalStateException(
            s"no manifest at $basePath; the segment has not been compacted " +
              "into the milvus-storage format"
          )
        }
        calls.timed(
          reader.create(
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
          MilvusStorageColumnGroups.createFromGroups(
            columnNames,
            files,
            rowCounts
          )
        )
        ownsColumnGroups = true
        calls.timed(
          reader.create(
            columnGroupsHandle,
            schemaStruct.memoryAddress(),
            columns,
            properties
          )
        )
    }

    if (!reader.isValid) {
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
        reader.openRecordBatchReaderScala()
      )
    }
    val batch =
      SegmentReader.readBatch(reader, batchReaderHandle, allocator, calls)
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
      reader.takeRecordBatchReaderScala(
        rowIndices,
        parallelism.toLong,
        columns.toArray
      )
    )
    if (handle == 0L) {
      throw new IllegalStateException(
        s"take returned no reader for segment ${task.segmentId}"
      )
    }
    try
      new NativeTakeResult(
        reader,
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
        calls.timed(reader.destroyRecordBatchReaderScala(handle))
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
      nativeStats = reader.recordBatchReaderStatsScala(batchReaderHandle)
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
      try nativeStats = reader.recordBatchReaderStatsScala(batchReaderHandle)
      catch {
        case e: Throwable =>
          logWarning("reading the batch reader stats failed", e)
      }
      try calls.timed(reader.destroyRecordBatchReaderScala(batchReaderHandle))
      catch {
        case e: Throwable => logWarning("destroying the batch reader failed", e)
      }
      batchReaderHandle = 0L
    }
    if (reader != null) {
      try calls.timed(reader.destroy())
      catch {
        case e: Throwable => logWarning("destroying the reader failed", e)
      }
      reader = null
    }
    if (ownsColumnGroups && columnGroupsHandle != 0L) {
      try calls.timed(MilvusStorageColumnGroups.destroy(columnGroupsHandle))
      catch {
        case e: Throwable =>
          logWarning("destroying the column groups failed", e)
      }
    }
    if (manifest != null) {
      // The manifest owns the column groups it handed out, so destroying it
      // covers both. The code this replaces never did, leaking one manifest
      // per partition.
      try calls.timed(manifest.close())
      catch {
        case e: Throwable => logWarning("destroying the manifest failed", e)
      }
      manifest = null
    }
    columnGroupsHandle = 0L
    if (properties != null) {
      try calls.timed(properties.free())
      catch {
        case e: Throwable =>
          logWarning("freeing the reader properties failed", e)
      }
      properties = null
    }
    if (schemaStruct != null) {
      try {
        try if (schemaStruct.snapshot().release != 0L) schemaStruct.release()
        finally schemaStruct.close()
      } catch {
        case e: Throwable => logWarning("closing the arrow schema failed", e)
      }
      schemaStruct = null
    }
  }
}
