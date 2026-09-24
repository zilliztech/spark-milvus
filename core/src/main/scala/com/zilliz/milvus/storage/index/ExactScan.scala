package com.zilliz.milvus.storage.index

import java.lang.{Float => JavaFloat}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.BitSet

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}

import com.zilliz.milvus.jni.vector.NativeVectorSearch
import com.zilliz.milvus.storage.read.exec.SegmentVectors

import io.knowhere.DType

/** Searches a segment by computing every distance, one native call per batch.
  *
  * The batches, their exclusion bitmaps and their row offsets come from the
  * Milvus format side; this only calls the engine and turns what comes back
  * into candidates (docs/design/architecture/vector-search.html section 2.3).
  *
  * A float32 batch goes to the batched distance entry, which hands every query
  * of the group to the bundled faiss in one call so that it takes its SGEMM
  * path (decision 27). That entry takes no bitmap, so a batch with excluded
  * rows is compacted first: its visible rows are copied in order into one
  * buffer and the row numbers the engine returns are mapped back. Every other
  * element type goes to Knowhere's per-query brute force with the bitmap as it
  * is.
  */
object ExactScan {

  /** The metrics the batched entry computes; the others belong to binary
    * vectors, which never reach it.
    */
  private val BatchedMetrics = Set("L2", "IP", "COSINE")

  /** Adds this segment's candidates to `merger`, which counts its queries the
    * way the group does: query 0 is the group's first query. The buffers of a
    * batch are borrowed for the length of the call and released with it.
    */
  def run(
      vectors: SegmentVectors,
      queries: QueryMatrix,
      segmentId: Long,
      k: Int,
      metric: String,
      allocator: BufferAllocator,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit = _ => ()
  ): Unit = {
    var next = vectors.next()
    while (next.nonEmpty) {
      val current = next.get
      try
        batch(
          current,
          queries,
          segmentId,
          k,
          metric,
          allocator,
          merger,
          onProgress
        )
      finally current.close()
      next = vectors.next()
    }
  }

  /** One batch, one native call. The batch stays open: a task that answers more
    * than one query group keeps its batches and calls this once per group
    * (section 2.3).
    */
  def batch(
      current: SegmentVectors.Batch,
      queries: QueryMatrix,
      segmentId: Long,
      k: Int,
      metric: String,
      allocator: BufferAllocator,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit = _ => ()
  ): Unit = {
    require(k > 0, s"topK must be positive: $k")
    require(Candidate.metricRanks(metric), s"Unsupported metric: $metric")
    if (current.visibleRows <= 0) return
    val dtype = queries.layout.dtype
    if (dtype == DType.FLOAT32 && BatchedMetrics(metric))
      batched(
        current,
        queries,
        segmentId,
        k,
        metric,
        allocator,
        merger,
        onProgress
      )
    else
      perQuery(
        current,
        queries,
        segmentId,
        k,
        metric,
        allocator,
        merger,
        onProgress
      )
  }

  /** The visible rows of a batch as one contiguous buffer, and for each of its
    * rows the batch row it came from. `None` when nothing is excluded: the
    * batch's own buffer serves as it is.
    */
  final class Compacted private[index] (
      val buffer: ArrowBuf,
      val rows: Int,
      val batchRows: Array[Int]
  ) extends AutoCloseable {
    override def close(): Unit = buffer.close()
  }

  /** Copies the rows of `source` that `excluded` does not name, in order, each
    * `rowBytes` long, run by run so that a batch with few deletes is a few
    * large copies. The copy is what a batch with excluded rows costs on the
    * batched path: its visible bytes, once per batch and query group.
    */
  private[index] def compact(
      source: ByteBuffer,
      rows: Int,
      excluded: BitSet,
      rowBytes: Int,
      allocator: BufferAllocator
  ): Compacted = {
    val visible = rows - excluded.get(0, rows).cardinality()
    require(visible > 0, "A batch with no visible rows is not compacted")
    val target = allocator.buffer(visible.toLong * rowBytes)
    try {
      val batchRows = new Array[Int](visible)
      var written = 0
      var start = excluded.nextClearBit(0)
      while (start < rows) {
        val end = math.min(
          rows,
          excluded.nextSetBit(start) match {
            case -1   => rows
            case next => next
          }
        )
        val run = source.duplicate()
        run.position(start * rowBytes)
        run.limit(end * rowBytes)
        target.setBytes(written.toLong * rowBytes, run)
        var row = start
        while (row < end) {
          batchRows(written) = row
          written += 1
          row += 1
        }
        start = excluded.nextClearBit(end)
      }
      require(written == visible, s"Compacted $written rows, expected $visible")
      new Compacted(target, visible, batchRows)
    } catch {
      case failure: Throwable =>
        target.close()
        throw failure
    }
  }

  private def batched(
      current: SegmentVectors.Batch,
      queries: QueryMatrix,
      segmentId: Long,
      k: Int,
      metric: String,
      allocator: BufferAllocator,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit
  ): Unit = {
    val parameters = s"""{"metric_type":"$metric"}"""
    val count = math.min(k, current.visibleRows)
    val compacted =
      if (current.excluded.isEmpty) None
      else
        Some(
          compact(
            current.base.buffer,
            current.rows,
            current.excluded,
            queries.layout.rowBytes,
            allocator
          )
        )
    val ids = allocator.buffer(queries.queries.toLong * count * 8L)
    val scores = allocator.buffer(queries.queries.toLong * count * 4L)
    try {
      val (base, rows) = compacted match {
        case Some(c) => (bytes(c.buffer, c.buffer.capacity()), c.rows)
        case None    => (current.base.buffer, current.rows)
      }
      val started = System.nanoTime()
      NativeVectorSearch.bruteForceBatched(
        queries.layout.dtype,
        base,
        rows.toLong,
        queries.buffer,
        queries.queries.toLong,
        queries.dimension,
        count,
        bytes(ids, queries.queries.toLong * count * 8L),
        bytes(scores, queries.queries.toLong * count * 4L),
        parameters
      )
      onProgress(
        SegmentSearch.Progress(
          1,
          System.nanoTime() - started,
          queries.queries.toLong * current.visibleRows.toLong,
          0
        )
      )
      collect(
        current,
        queries.queries,
        count,
        ids,
        scores,
        segmentId,
        merger,
        rows,
        compacted.map(_.batchRows)
      )
    } finally {
      scores.close()
      ids.close()
      compacted.foreach(_.close())
    }
  }

  private def perQuery(
      current: SegmentVectors.Batch,
      queries: QueryMatrix,
      segmentId: Long,
      k: Int,
      metric: String,
      allocator: BufferAllocator,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit
  ): Unit = {
    val parameters = s"""{"metric_type":"$metric"}"""
    val dtype = queries.layout.dtype
    val count = math.min(k, current.visibleRows)
    val ids = allocator.buffer(queries.queries.toLong * count * 8L)
    val scores = allocator.buffer(queries.queries.toLong * count * 4L)
    val mask = allocator.buffer((current.rows.toLong + 7L) / 8L)
    try {
      writeMask(current, mask)
      val started = System.nanoTime()
      NativeVectorSearch.bruteForce(
        dtype,
        current.base.buffer,
        current.rows.toLong,
        queries.buffer,
        queries.queries.toLong,
        queries.dimension,
        count,
        bytes(mask, mask.capacity()),
        bytes(ids, queries.queries.toLong * count * 8L),
        bytes(scores, queries.queries.toLong * count * 4L),
        parameters
      )
      // The queries and the rows that survived the mask are both in scope
      // here and nowhere above: this is the only point that can say how many
      // pairs a step measured.
      onProgress(
        SegmentSearch.Progress(
          1,
          System.nanoTime() - started,
          queries.queries.toLong * current.visibleRows.toLong,
          0
        )
      )
      collect(
        current,
        queries.queries,
        count,
        ids,
        scores,
        segmentId,
        merger,
        current.rows,
        None
      )
    } finally {
      mask.close()
      scores.close()
      ids.close()
    }
  }

  private def writeMask(
      batch: SegmentVectors.Batch,
      mask: ArrowBuf
  ): Unit = {
    mask.setZero(0, mask.capacity())
    var row = batch.excluded.nextSetBit(0)
    while (row >= 0 && row < batch.rows) {
      val index = row.toLong / 8L
      mask.setByte(index, mask.getByte(index) | (1 << (row % 8)))
      row = batch.excluded.nextSetBit(row + 1)
    }
  }

  /** `rows` is what the engine saw and `batchRows` maps its row numbers back to
    * the batch when the batch was compacted.
    */
  private def collect(
      batch: SegmentVectors.Batch,
      queries: Int,
      count: Int,
      ids: ArrowBuf,
      scores: ArrowBuf,
      segmentId: Long,
      merger: TopKMerger,
      rows: Int,
      batchRows: Option[Array[Int]]
  ): Unit = {
    var query = 0
    while (query < queries) {
      var slot = 0
      while (slot < count) {
        val position = query.toLong * count + slot
        val id = ids.getLong(position * 8L)
        if (id != -1L) {
          require(
            id >= 0 && id < rows,
            s"The engine returned row $id of a batch with $rows rows"
          )
          val row = batchRows match {
            case Some(mapping) => mapping(id.toInt)
            case None          => id.toInt
          }
          require(
            !batch.excluded.get(row),
            s"The engine returned excluded row $row"
          )
          val score = scores.getFloat(position * 4L)
          require(
            JavaFloat.isFinite(score),
            s"The engine returned a score that is not finite for row $row"
          )
          merger.add(query, segmentId, batch.firstRow + row, score.toDouble)
        }
        slot += 1
      }
      query += 1
    }
  }

  private def bytes(buffer: ArrowBuf, length: Long) =
    buffer.nioBuffer(0, length.toInt).order(ByteOrder.nativeOrder())
}
