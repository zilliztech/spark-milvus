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
    allocator: BufferAllocator
) extends AutoCloseable {
  private var nextRow = 0L
  private var closed = false

  /** The next batch, or None at the end of the segment. The caller closes each
    * batch before asking for the next one.
    */
  def next(): Option[SegmentVectors.Batch] = {
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
          new SegmentVectors.Batch(base, excluded, nextRow, rows, root)
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
      private val root: VectorSchemaRoot
  ) extends AutoCloseable {

    /** The rows this batch offers a search, after deletes, the filter and null
      * vectors.
      */
    def visibleRows: Int = rows - excluded.cardinality()

    override def close(): Unit = {
      try base.close()
      finally root.close()
    }
  }

  /** Over a reader that is already open, which is how a test supplies batches
    * without a segment.
    */
  private[exec] def over(
      reader: SegmentReader,
      vectorColumn: String,
      layout: VectorLayout,
      exclusions: RowExclusions,
      allocator: BufferAllocator
  ): SegmentVectors =
    new SegmentVectors(reader, vectorColumn, layout, exclusions, allocator)

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
    over(reader, vectorColumn, layout, exclusions, allocator)
  }
}
