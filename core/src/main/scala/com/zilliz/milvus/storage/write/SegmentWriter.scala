package com.zilliz.milvus.storage.write

import scala.collection.JavaConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.storage.NativeCalls
import io.milvus.storage.{MilvusStorageProperties, MilvusStorageWriter}

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
trait CountingWriter { this: SegmentWriter =>
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
  def serializingFirstWrite(doWrite: => Unit): Unit = {
    if (!firstWriteDone) {
      firstWriteLock.synchronized {
        doWrite
        firstWriteDone = true
      }
    } else doWrite
  }

  /** Exports `batch` and hands its address to `send`; the C struct is released
    * afterwards. On success the C++ side has already moved the release callback
    * out of the struct, so closing it frees only the struct; on failure any
    * remaining release callback is invoked before the struct is closed.
    */
  def exported(
      allocator: BufferAllocator,
      batch: VectorSchemaRoot
  )(send: Long => Unit): Unit = {
    val array = ArrowArray.allocateNew(allocator)
    WriterResources.closing(
      WriterResources.closeAll(
        null,
        () => if (array.snapshot().release != 0L) array.release(),
        () => array.close()
      )
    ) {
      Data.exportVectorSchemaRoot(allocator, batch, null, array)
      send(array.memoryAddress())
    }
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
    with CountingWriter {

  NativeStorageLibrary.load()

  private val schemaStruct: ArrowSchema = ArrowSchema.allocateNew(allocator)
  private var writer: MilvusStorageWriter = null
  private var nativeProperties: MilvusStorageProperties = null
  private var written: Long = 0L
  private var closed = false

  try {
    schemaStruct.save(new ArrowSchema.Snapshot())
    Data.exportSchema(allocator, arrowSchema, null, schemaStruct)
    nativeProperties = calls.timed(new MilvusStorageProperties())
    calls.timed(
      nativeProperties.create(
        properties ++ ColumnGroupSplit.writerProperties(columnGroupPatterns)
      )
    )
    writer = new MilvusStorageWriter()
    calls.timed(
      writer.create(basePath, schemaStruct.memoryAddress(), nativeProperties)
    )
    if (!writer.isValid) {
      throw new IllegalStateException(
        s"could not open the native writer at $basePath"
      )
    }
  } catch {
    case e: Throwable =>
      WriterResources.closeAll(e, () => release())
      throw e
  }

  override def write(batch: VectorSchemaRoot): Unit = {
    val count = batch.getRowCount
    if (count == 0) return
    countBatch(batch)
    SegmentWriter.exported(allocator, batch) { address =>
      SegmentWriter.serializingFirstWrite {
        calls.timed(writer.write(address))
        calls.timed(writer.flush())
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
    WriterResources.handoff(release()) {
      val groups = calls.timed(writer.close())
      new WrittenColumnGroups(groups)
    }
  }

  override def close(): Unit = release()

  private def release(): Unit = synchronized {
    if (closed) return
    closed = true
    val ownedWriter = writer
    val ownedProperties = nativeProperties
    writer = null
    nativeProperties = null
    WriterResources.closeAll(
      null,
      () => if (ownedWriter != null) calls.timed(ownedWriter.destroy()),
      () => if (ownedProperties != null) calls.timed(ownedProperties.free()),
      () => if (schemaStruct.snapshot().release != 0L) schemaStruct.release(),
      () => schemaStruct.close()
    )
  }
}
