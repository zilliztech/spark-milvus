package com.zilliz.milvus.storage.index

import java.util.BitSet

import org.apache.arrow.memory.BufferAllocator

/** A batch of vectors as a search computes over it, whichever input it came
  * from: `rows` vectors laid out end to end in a buffer Knowhere reads, the
  * rows the search skips, and where the batch starts in its unit, so that row
  * `firstRow + i` of the unit is row `i` of the batch
  * (docs/design/architecture/table-version.html section 3). A Milvus segment's
  * batches come from `SegmentVectors`, a DataFrame input's from
  * [[VectorBatch.ofFloats]].
  */
trait VectorBatch extends AutoCloseable {
  def base: KnowhereBuffers.Base
  def excluded: BitSet
  def firstRow: Long
  def rows: Int

  /** The rows this batch offers a search. */
  def visibleRows: Int = rows - excluded.cardinality()
}

object VectorBatch {

  /** Float vectors of `dimension` elements as a batch that owns its buffer: row
    * `i` is `vectors(i)`, and a null one is excluded.
    */
  def ofFloats(
      vectors: Array[Array[Float]],
      rows: Int,
      dimension: Int,
      firstRow: Long,
      allocator: BufferAllocator
  ): VectorBatch = {
    val excluded = new BitSet(rows)
    var row = 0
    while (row < rows) {
      if (vectors(row) == null) excluded.set(row)
      row += 1
    }
    new Owned(
      KnowhereBuffers.ofFloats(vectors, rows, dimension, allocator),
      excluded,
      firstRow,
      rows
    )
  }

  private final class Owned(
      val base: KnowhereBuffers.Base,
      val excluded: BitSet,
      val firstRow: Long,
      val rows: Int
  ) extends VectorBatch {
    override def close(): Unit = base.close()
  }
}
