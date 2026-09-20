package com.zilliz.milvus.storage.read.exec

import java.util.BitSet

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.storage.index.KnowhereBuffers
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.VectorLayout

/** One segment's vectors, batch by batch, with the rows a search must skip.
  *
  * This is the Milvus format side of an exact scan: it opens the segment, reads
  * the vector column together with the columns deletes and the filter need, and
  * hands out the buffer Knowhere reads, the exclusion bitmap of that batch and
  * where the batch starts in the segment. The computation never sees the
  * reader, the columns or the delete rules
  * (docs/design/architecture/vector-search.html section 2.3).
  */
final class SegmentVectors private (
    reader: SegmentReader,
    vectorColumn: String,
    layout: VectorLayout,
    exclusions: RowExclusions,
    allocator: BufferAllocator,
    batchMaxBytes: Long
) extends AutoCloseable {
  private var nextRow = 0L
  private var closed = false

  /** The next batch, or None at the end of the segment. The caller closes each
    * batch before asking for the next one.
    *
    * What the storage layer returns is not what a search wants to work on: it
    * closes a Parquet row group at a megabyte, so a 1024-dimension float vector
    * arrives 256 rows at a time whatever `milvus.read.batch.max.rows` says, and
    * every one of those becomes its own call into the engine over its own
    * reload of the query matrix. Batches are joined here until they fill
    * `milvus.read.batch.max.bytes`, which is what that option was for.
    */
  def next(): Option[SegmentVectors.Batch] = {
    val first = readBatch()
    if (first.isEmpty) return first
    val rowBytes = layout.rowBytes.toLong
    var parts = List(first.get)
    var bytes = first.get.rows.toLong * rowBytes
    var reading = true
    while (reading && bytes < batchMaxBytes) {
      readBatch() match {
        case Some(batch) =>
          parts = batch :: parts
          bytes += batch.rows.toLong * rowBytes
        case None => reading = false
      }
    }
    if (parts.size == 1) first
    else Some(SegmentVectors.join(parts.reverse, layout, allocator))
  }

  private def readBatch(): Option[SegmentVectors.Batch] = {
    require(!closed, "Segment vectors are closed")
    reader.next().map { root =>
      var base: KnowhereBuffers.Base = null
      try {
        val rows = root.getRowCount
        val excluded = new BitSet(math.max(rows, 1))
        var row = 0
        while (row < rows) {
          if (exclusions.excludes(root, row)) excluded.set(row)
          row += 1
        }
        base = KnowhereBuffers.base(
          root.getVector(vectorColumn),
          layout,
          allocator
        )(excluded.set)
        val batch =
          new SegmentVectors.Batch(base, excluded, nextRow, rows, Seq(root))
        nextRow += rows
        batch
      } catch {
        case failure: Throwable =>
          if (base != null) base.close()
          root.close()
          throw failure
      }
    }
  }

  /** How many rows of this segment have been handed out. */
  def rows: Long = nextRow

  def metrics: ReadMetrics = reader.metrics

  override def close(): Unit = if (!closed) {
    closed = true
    reader.close()
  }
}

object SegmentVectors {

  /** One batch: the vectors as Knowhere reads them, the rows to skip, and where
    * the batch sits in the segment. Row `firstRow + i` of the segment is row
    * `i` of this batch.
    */
  final class Batch private[exec] (
      val base: KnowhereBuffers.Base,
      val excluded: BitSet,
      val firstRow: Long,
      val rows: Int,
      private[exec] val backing: Seq[AutoCloseable]
  ) extends AutoCloseable {

    /** The rows this batch offers a search, after deletes, the filter and null
      * vectors.
      */
    def visibleRows: Int = rows - excluded.cardinality()

    override def close(): Unit = {
      try base.close()
      finally {
        val failures = backing.flatMap(closeable =>
          scala.util.Try(closeable.close()).failed.toOption
        )
        failures.headOption.foreach(throw _)
      }
    }
  }

  /** Several read batches as one, contiguous in rows and in bytes.
    *
    * The exclusion bitmaps move with the rows: a row excluded at position `i`
    * of the third part is excluded at `rowsBefore + i` of the result. The parts
    * are closed by [[KnowhereBuffers.joined]] once their bytes are copied, so
    * the peak is one part above the result.
    */
  private[exec] def join(
      parts: Seq[Batch],
      layout: VectorLayout,
      allocator: BufferAllocator
  ): Batch = {
    require(parts.nonEmpty, "A joined batch needs at least one part")
    val rows = parts.map(_.rows.toLong).sum
    require(
      rows <= Int.MaxValue.toLong,
      s"A joined batch of $rows rows exceeds what one batch addresses"
    )
    val excluded = new BitSet(math.max(rows.toInt, 1))
    var before = 0
    parts.foreach { part =>
      var row = part.excluded.nextSetBit(0)
      while (row >= 0) {
        excluded.set(before + row)
        row = part.excluded.nextSetBit(row + 1)
      }
      before += part.rows
    }
    val base =
      KnowhereBuffers.joined(parts.map(_.base), layout.rowBytes, allocator)
    // The bytes are the result's now, so what the parts were reading out of
    // goes. Keeping it would hold the reader's Arrow data beside the copy for
    // as long as the batch lives, which in a held segment set is every byte of
    // the set twice over, and `hold` would count one of them.
    val failures = parts.flatMap(part =>
      part.backing.flatMap(closeable =>
        scala.util.Try(closeable.close()).failed.toOption
      )
    )
    failures.headOption.foreach { failure =>
      base.close()
      throw failure
    }
    new Batch(base, excluded, parts.head.firstRow, rows.toInt, Seq.empty)
  }

  /** Over a reader that is already open, which is how a test supplies batches
    * without a segment.
    */
  private[exec] def over(
      reader: SegmentReader,
      vectorColumn: String,
      layout: VectorLayout,
      exclusions: RowExclusions,
      allocator: BufferAllocator,
      batchMaxBytes: Long
  ): SegmentVectors =
    new SegmentVectors(
      reader,
      vectorColumn,
      layout,
      exclusions,
      allocator,
      batchMaxBytes
    )

  def open(
      task: SegmentReadTask,
      arrowSchema: Schema,
      columnNameFor: Long => Option[String],
      vectorColumn: String,
      layout: VectorLayout,
      exclusions: RowExclusions,
      allocator: BufferAllocator
  ): SegmentVectors = {
    val columns = (vectorColumn +: exclusions.neededColumns).distinct
    val reader = SegmentReaderRegistry.open(
      task,
      arrowSchema,
      columns,
      columnNameFor,
      allocator
    )
    over(
      reader,
      vectorColumn,
      layout,
      exclusions,
      allocator,
      task.limits.batchMaxBytes
    )
  }
}
