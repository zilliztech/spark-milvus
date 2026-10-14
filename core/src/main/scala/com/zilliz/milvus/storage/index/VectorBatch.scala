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

  /** The input cost a search reports, sampled at the end of a read or when a
    * held set exceeds its budget. The format keeps its fuller reader metrics.
    */
  final case class ReadCost(bytes: Long, nanos: Long) {
    def +(other: ReadCost): ReadCost =
      ReadCost(bytes + other.bytes, nanos + other.nanos)
  }

  object ReadCost {
    val Zero: ReadCost = ReadCost(0L, 0L)
  }

  /** Owned input to a scan or build. Each returned batch belongs to the caller
    * until it closes it; batches may be kept while the stream advances. Closing
    * the stream drains any read in flight and releases unread resources,
    * without releasing batches already handed to the caller.
    */
  trait Stream extends AutoCloseable {
    def next(): Option[VectorBatch]
    def readCost: ReadCost
  }

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
