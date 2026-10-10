package com.zilliz.milvus.storage.index

import java.lang.{Float => JavaFloat}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.BitSet

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}

import com.zilliz.milvus.jni.vector.NativeVectorSearch
import com.zilliz.milvus.storage.read.exec.SegmentVectors
import com.zilliz.milvus.storage.schema.MetricType

import io.knowhere.DType

/** Searches a unit by computing every distance, one native call per batch.
  *
  * The batches, their exclusion bitmaps and their row offsets come from the
  * input: a Milvus segment's from the format side, a DataFrame input's from its
  * rows ([[VectorBatch]]). This only calls the engine and turns what comes back
  * into candidates, a candidate's place being the unit's id and the row's
  * position in it (docs/design/architecture/vector-search.html section 2.3).
  *
  * A float32 batch goes to the batched distance entry, which hands every query
  * of the group to the bundled faiss in one call so that it takes its SGEMM
  * path (decision 27). That entry takes no bitmap, so a batch with excluded
  * rows is compacted first: its visible rows are copied in order into one
  * buffer and the row numbers the engine returns are mapped back. Every other
  * element type goes to Knowhere's per-query brute force with the bitmap as it
  * is.
  *
  * A search ranked by a [[RankingFunction]] hands Knowhere only the rows in
  * [[EngineRange]]; the function scores the batch's other rows against every
  * query of the group, and both kinds of score meet in the group's merger
  * (docs/design/architecture/dataframe-api.html section 2).
  */
object ExactScan {

  /** Adds this segment's candidates to `merger`, which counts its queries the
    * way the group does: query 0 is the group's first query. The buffers of a
    * batch are borrowed for the length of the call and released with it.
    */
  def run(
      vectors: SegmentVectors,
      queries: QueryMatrix,
      segmentId: Long,
      k: Int,
      metric: MetricType,
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
    * (section 2.3). `ranked` is the batch as a ranking function sees it, worked
    * out once for all the groups.
    */
  def batch(
      current: VectorBatch,
      queries: QueryMatrix,
      segmentId: Long,
      k: Int,
      metric: MetricType,
      allocator: BufferAllocator,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit = _ => (),
      ranked: Option[EngineRange.Batch] = None
  ): Unit = {
    require(k > 0, s"topK must be positive: $k")
    val excluded = ranked.fold(current.excluded)(_.engineExcluded)
    val visible = ranked.fold(current.visibleRows)(_.engineRows)
    if (visible > 0) {
      if (queries.layout.dtype == DType.FLOAT32)
        batched(
          current,
          excluded,
          visible,
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
          excluded,
          visible,
          queries,
          segmentId,
          k,
          metric,
          allocator,
          merger,
          onProgress
        )
    }
    ranked.foreach(
      scoreByFunction(current, _, queries, segmentId, merger, onProgress)
    )
  }

  /** The rows Knowhere was not given, scored by the function against every
    * query of the group. A query is decoded from the matrix once, as the engine
    * received it.
    */
  private def scoreByFunction(
      current: VectorBatch,
      ranked: EngineRange.Batch,
      queries: QueryMatrix,
      segmentId: Long,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit
  ): Unit = if (ranked.rankedRows.nonEmpty) {
    val query = new Array[Float](queries.dimension)
    var at = 0
    while (at < queries.queries) {
      EngineRange.decode(queries.buffer, at, queries.layout, query)
      var row = 0
      while (row < ranked.rankedRows.length) {
        val score = ranked.function.score(query, ranked.rankedValues(row))
        if (score != null && !merger.rejectsScore(at, score.doubleValue()))
          merger.add(
            at,
            segmentId,
            current.firstRow + ranked.rankedRows(row),
            score.doubleValue()
          )
        row += 1
      }
      at += 1
    }
    onProgress(
      SegmentSearch.Progress(
        0,
        0L,
        queries.queries.toLong * ranked.rankedRows.length,
        0
      )
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
      current: VectorBatch,
      excluded: BitSet,
      visible: Int,
      queries: QueryMatrix,
      segmentId: Long,
      k: Int,
      metric: MetricType,
      allocator: BufferAllocator,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit
  ): Unit = {
    val parameters = s"""{"metric_type":"${metric.name}"}"""
    val count = math.min(k, visible)
    val compacted =
      if (excluded.isEmpty) None
      else
        Some(
          compact(
            current.base.buffer,
            current.rows,
            excluded,
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
          queries.queries.toLong * visible.toLong,
          0
        )
      )
      collect(
        excluded,
        queries.queries,
        count,
        ids,
        scores,
        segmentId,
        current.firstRow,
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
      current: VectorBatch,
      excluded: BitSet,
      visible: Int,
      queries: QueryMatrix,
      segmentId: Long,
      k: Int,
      metric: MetricType,
      allocator: BufferAllocator,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit
  ): Unit = {
    val parameters = s"""{"metric_type":"${metric.name}"}"""
    val dtype = queries.layout.dtype
    val count = math.min(k, visible)
    val ids = allocator.buffer(queries.queries.toLong * count * 8L)
    val scores = allocator.buffer(queries.queries.toLong * count * 4L)
    val mask = allocator.buffer((current.rows.toLong + 7L) / 8L)
    try {
      writeMask(excluded, current.rows, mask)
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
          queries.queries.toLong * visible.toLong,
          0
        )
      )
      collect(
        excluded,
        queries.queries,
        count,
        ids,
        scores,
        segmentId,
        current.firstRow,
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
      excluded: BitSet,
      rows: Int,
      mask: ArrowBuf
  ): Unit = {
    mask.setZero(0, mask.capacity())
    var row = excluded.nextSetBit(0)
    while (row >= 0 && row < rows) {
      val index = row.toLong / 8L
      mask.setByte(index, mask.getByte(index) | (1 << (row % 8)))
      row = excluded.nextSetBit(row + 1)
    }
  }

  /** `rows` is what the engine saw and `batchRows` maps its row numbers back to
    * the batch when the batch was compacted; `excluded` is what the engine was
    * told to skip, and `firstRow` where the batch starts in the segment.
    */
  private def collect(
      excluded: BitSet,
      queries: Int,
      count: Int,
      ids: ArrowBuf,
      scores: ArrowBuf,
      segmentId: Long,
      firstRow: Long,
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
            !excluded.get(row),
            s"The engine returned excluded row $row"
          )
          val score = scores.getFloat(position * 4L)
          require(
            JavaFloat.isFinite(score),
            s"The engine returned a score that is not finite for row $row"
          )
          merger.add(query, segmentId, firstRow + row, score.toDouble)
        }
        slot += 1
      }
      query += 1
    }
  }

  private def bytes(buffer: ArrowBuf, length: Long) =
    buffer.nioBuffer(0, length.toInt).order(ByteOrder.nativeOrder())
}
