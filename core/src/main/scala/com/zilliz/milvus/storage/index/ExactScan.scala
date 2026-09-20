package com.zilliz.milvus.storage.index

import java.lang.{Float => JavaFloat}
import java.nio.ByteOrder

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}

import com.zilliz.milvus.jni.vector.NativeVectorSearch
import com.zilliz.milvus.storage.read.exec.SegmentVectors

/** Searches a segment by computing every distance, one native call per batch.
  *
  * The batches, their exclusion bitmaps and their row offsets come from the
  * Milvus format side; this only calls Knowhere and turns what comes back into
  * candidates (docs/design/architecture/vector-search.html section 2.3).
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
    val parameters = s"""{"metric_type":"$metric"}"""
    val dtype = KnowhereBuffers.dtypeOf(queries.layout)
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
      collect(current, queries.queries, count, ids, scores, segmentId, merger)
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

  private def collect(
      batch: SegmentVectors.Batch,
      queries: Int,
      count: Int,
      ids: ArrowBuf,
      scores: ArrowBuf,
      segmentId: Long,
      merger: TopKMerger
  ): Unit = {
    var query = 0
    while (query < queries) {
      var slot = 0
      while (slot < count) {
        val position = query.toLong * count + slot
        val id = ids.getLong(position * 8L)
        if (id != -1L) {
          require(
            id >= 0 && id < batch.rows,
            s"Knowhere returned row $id of a batch with ${batch.rows} rows"
          )
          require(
            !batch.excluded.get(id.toInt),
            s"Knowhere returned excluded row $id"
          )
          val score = scores.getFloat(position * 4L)
          require(
            JavaFloat.isFinite(score),
            s"Knowhere returned a score that is not finite for row $id"
          )
          merger.add(
            Candidate(
              query,
              segmentId,
              batch.firstRow + id,
              score.toDouble
            )
          )
        }
        slot += 1
      }
      query += 1
    }
  }

  private def bytes(buffer: ArrowBuf, length: Long) =
    buffer.nioBuffer(0, length.toInt).order(ByteOrder.nativeOrder())
}
