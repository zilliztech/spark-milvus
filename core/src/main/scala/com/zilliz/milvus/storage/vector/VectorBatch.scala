package com.zilliz.milvus.storage.vector

import java.nio.{ByteBuffer, ByteOrder}
import java.util.BitSet

/** A batch of vectors as a search computes over it, whichever input it came
  * from: `rows` vectors laid out end to end in a buffer Knowhere reads, the
  * rows the search skips, and where the batch starts in its unit, so that row
  * `firstRow + i` of the unit is row `i` of the batch. A Milvus segment's
  * batches come from `SegmentVectors`, a DataFrame input's from
  * [[VectorBatch.borrow]].
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

  /** Borrows the remaining bytes of a direct buffer, containing `rows` vectors
    * in the search's layout. The caller owns the buffer and exclusion bitmap,
    * and may reuse them only after the synchronous scan returns. Closing this
    * batch does not release the borrowed memory.
    */
  def borrow(
      buffer: ByteBuffer,
      rows: Int,
      firstRow: Long,
      excluded: BitSet
  ): VectorBatch = {
    require(buffer.isDirect, "A borrowed vector buffer must be direct")
    require(rows >= 0, s"A batch holds $rows rows")
    require(excluded.length() <= rows, "Excluded rows must belong to the batch")
    new Borrowed(
      new KnowhereBuffers.Base(
        buffer.slice().order(ByteOrder.nativeOrder()),
        rows,
        true,
        None
      ),
      excluded,
      firstRow,
      rows
    )
  }

  private final class Borrowed(
      val base: KnowhereBuffers.Base,
      val excluded: BitSet,
      val firstRow: Long,
      val rows: Int
  ) extends VectorBatch {
    override def close(): Unit = base.close()
  }
}
