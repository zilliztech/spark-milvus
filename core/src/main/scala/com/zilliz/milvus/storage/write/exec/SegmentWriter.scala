package com.zilliz.milvus.storage.write.exec

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.jni.storage.StorageNative
import com.zilliz.milvus.storage.{Logging, NativeCalls}

/** Writes Arrow batches into one segment.
  *
  * `write` exports the batch through the Arrow C Data Interface and hands the
  * address to the C++ writer, which keeps a reference to the batch's buffers
  * until it flushes them. The caller therefore does not reuse a root after
  * `write`: it builds a fresh one per batch and closes the old one, which only
  * drops the JVM's reference. `close()` releases the native writer and the
  * exported schema without finishing the write; it is what abort calls and it
  * is idempotent.
  */
trait SegmentWriter extends AutoCloseable {
  def write(batch: VectorSchemaRoot): Unit

  /** Rows handed to the native writer so far. */
  def rows: Long

  /** What the write has cost on the crossing so far; valid after `close()` too.
    */
  def metrics: WriteMetrics
}

/** The counting every [[SegmentWriter]] does the same way: calls and their time
  * through [[NativeCalls]], batches and their buffer bytes at `write`, the
  * allocator's high-water mark after each batch.
  */
private[exec] trait CountingWriter { this: SegmentWriter =>
  protected def allocator: BufferAllocator
  protected val calls = new NativeCalls
  private var batches: Long = 0L
  private var arrowBytes: Long = 0L
  private var allocatedMax: Long = 0L

  protected def countBatch(batch: VectorSchemaRoot): Unit = {
    batches += 1
    var bytes = 0L
    val vectors = batch.getFieldVectors
    var i = 0
    while (i < vectors.size()) {
      bytes += vectors.get(i).getBufferSize.toLong
      i += 1
    }
    arrowBytes += bytes
    allocatedMax = math.max(allocatedMax, allocator.getAllocatedMemory)
  }

  override def metrics: WriteMetrics = WriteMetrics(
    jniCalls = calls.calls,
    jniNanos = calls.nanos,
    batches = batches,
    arrowBytes = arrowBytes,
    allocatedMax = allocatedMax
  )
}

object SegmentWriter {
  @volatile private var firstWriteDone: Boolean = false
  private val firstWriteLock = new Object()

  /** The first batch any writer in this JVM sends runs alone: the native
    * library initializes its object storage client on the first write and that
    * initialization is not safe to race.
    */
  private[exec] def serializingFirstWrite(doWrite: => Unit): Unit = {
    if (!firstWriteDone) {
      firstWriteLock.synchronized {
        doWrite
        firstWriteDone = true
      }
    } else doWrite
  }

  /** Exports `batch` and hands its address to `send`; the C struct is released
    * afterwards. On success the C++ side has already moved the release callback
    * out of the struct, so closing it frees only the struct; on failure the
    * release is still there and closing it drops the export's reference.
    */
  private[exec] def exported(
      allocator: BufferAllocator,
      batch: VectorSchemaRoot
  )(send: Long => Unit): Unit = {
    val array = ArrowArray.allocateNew(allocator)
    try {
      Data.exportVectorSchemaRoot(allocator, batch, null, array)
      send(array.memoryAddress())
    } finally array.close()
  }
}

/** A `storage_version = 3` segment: column groups under `basePath`, split by
  * milvus-storage's column group policy. `finish()` closes the writer and
  * returns what it wrote; the caller then records it in the manifest through
  * [[ManifestTransaction]].
  *
  * @param basePath
  *   the segment directory as a key relative to `fs.bucket_name` (the C
  *   filesystem is rooted at the bucket), or a local directory under
  *   `fs.root_path` for the local backend.
  * @param properties
  *   the `fs.*` map, already validated by `core.credential.StorageProperties`.
  * @param columnGroupPatterns
  *   how the columns split into column groups, from `ColumnGroupSplit`; empty
  *   writes every column into one group.
  */
final class V3SegmentWriter(
    val basePath: String,
    arrowSchema: Schema,
    properties: Map[String, String],
    protected val allocator: BufferAllocator,
    columnGroupPatterns: Seq[String] = Seq.empty
) extends SegmentWriter
    with CountingWriter
    with Logging {

  private val schemaStruct: ArrowSchema = ArrowSchema.allocateNew(allocator)
  private var handle: Long = 0L
  private var written: Long = 0L
  private var closed = false

  try {
    Data.exportSchema(allocator, arrowSchema, null, schemaStruct)
    handle = calls.timed(
      StorageNative.writerNew(
        basePath,
        schemaStruct.memoryAddress(),
        (properties ++ ColumnGroupSplit.writerProperties(
          columnGroupPatterns
        )).asJava
      )
    )
    if (handle == 0L) {
      throw new IllegalStateException(
        s"could not open the native writer at $basePath"
      )
    }
  } catch {
    case NonFatal(e) =>
      release()
      throw e
  }

  override def write(batch: VectorSchemaRoot): Unit = {
    val count = batch.getRowCount
    if (count == 0) return
    countBatch(batch)
    SegmentWriter.exported(allocator, batch) { address =>
      SegmentWriter.serializingFirstWrite {
        calls.timed(StorageNative.writerWrite(handle, address))
        calls.timed(StorageNative.writerFlush(handle))
      }
    }
    written += count
  }

  override def rows: Long = written

  /** Closes the native writer and returns the column groups it produced. The
    * writer is released afterwards; `close()` then does nothing more.
    */
  def finish(): WrittenColumnGroups = {
    if (closed) {
      throw new IllegalStateException(s"writer at $basePath already closed")
    }
    try {
      val groups = calls.timed(StorageNative.writerClose(handle, null, null))
      new WrittenColumnGroups(groups)
    } finally release()
  }

  override def close(): Unit = release()

  private def release(): Unit = synchronized {
    if (closed) return
    closed = true
    if (handle != 0L) {
      try StorageNative.writerDestroy(handle)
      catch {
        case NonFatal(e) => logWarning(s"destroying the writer failed", e)
      }
      handle = 0L
    }
    try schemaStruct.close()
    catch {
      case NonFatal(e) => logWarning("closing the arrow schema failed", e)
    }
  }
}

/** A `storage_version = 2` segment: one parquet file per column group, at the
  * paths the caller names, written through milvus-storage's packed writer so
  * each file carries the footer metadata Milvus's own compaction writes
  * (`row_group_metadata`, `storage_version`, `group_field_id_list`).
  *
  * @param paths
  *   one output path per column group, keys relative to `fs.bucket_name`.
  * @param columnGroups
  *   for each group, the indices of its columns in `arrowSchema`.
  */
final class V2SegmentWriter(
    val paths: Seq[String],
    columnGroups: Seq[Seq[Int]],
    arrowSchema: Schema,
    properties: Map[String, String],
    protected val allocator: BufferAllocator
) extends SegmentWriter
    with CountingWriter
    with Logging {
  require(
    paths.size == columnGroups.size,
    s"${paths.size} paths for ${columnGroups.size} column groups"
  )
  // The packed writer stamps `group_field_id_list` into each footer from the
  // field's `PARQUET:field_id` metadata; without it the C layer fails after
  // the schema has crossed over, with a message that names no caller.
  arrowSchema.getFields.asScala.foreach { field =>
    val md = field.getMetadata
    require(
      md != null && md.containsKey("PARQUET:field_id"),
      s"field '${field.getName}' has no PARQUET:field_id metadata; the V2 " +
        "writer needs the Milvus field id of every column"
    )
  }

  private val schemaStruct: ArrowSchema = ArrowSchema.allocateNew(allocator)
  private var handle: Long = 0L
  private var written: Long = 0L
  private var closed = false

  try {
    Data.exportSchema(allocator, arrowSchema, null, schemaStruct)
    // The C layer takes the per-group column indices flattened: group g owns
    // indices[offsets(g) until offsets(g + 1)].
    val offsets = columnGroups.scanLeft(0)(_ + _.size).toArray
    handle = calls.timed(
      StorageNative.packedWriterNew(
        paths.toArray,
        offsets,
        columnGroups.flatten.toArray,
        schemaStruct.memoryAddress(),
        properties.asJava,
        0L
      )
    )
    if (handle == 0L) {
      throw new IllegalStateException(
        s"could not open the native packed writer for ${paths.mkString(", ")}"
      )
    }
  } catch {
    case NonFatal(e) =>
      release()
      throw e
  }

  override def write(batch: VectorSchemaRoot): Unit = {
    val count = batch.getRowCount
    if (count == 0) return
    countBatch(batch)
    SegmentWriter.exported(allocator, batch) { address =>
      SegmentWriter.serializingFirstWrite {
        calls.timed(StorageNative.packedWriterWrite(handle, address))
      }
    }
    written += count
  }

  override def rows: Long = written

  /** Closes the files. Every group holds `rows` rows: each batch put one value
    * of every column into every group.
    */
  def finish(): Long = {
    if (closed) {
      throw new IllegalStateException("packed writer already closed")
    }
    try {
      calls.timed(StorageNative.packedWriterClose(handle))
      written
    } finally release()
  }

  override def close(): Unit = release()

  private def release(): Unit = synchronized {
    if (closed) return
    closed = true
    if (handle != 0L) {
      try StorageNative.packedWriterDestroy(handle)
      catch {
        case NonFatal(e) => logWarning("destroying the packed writer failed", e)
      }
      handle = 0L
    }
    try schemaStruct.close()
    catch {
      case NonFatal(e) => logWarning("closing the arrow schema failed", e)
    }
  }
}
