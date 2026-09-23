package com.zilliz.milvus.storage.index

import java.lang.{Float => JavaFloat}
import java.nio.ByteOrder
import java.util.{Arrays, BitSet}
import scala.util.Try

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}

import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle

/** Searches a segment through the index the Milvus format side opened.
  *
  * One call answers the whole query group. A query that comes back with fewer
  * visible hits than it asked for is retried with a wider `ef`, up to eight
  * times, which is what section 2.4 fixes.
  */
object IndexProbe {

  private val MaxAttempts = 8

  /** How wide the HNSW search is: `ef` if the caller named it, otherwise
    * `max(64, k)`. It is the only search parameter this connector accepts, and
    * it cannot be smaller than k (docs/design/architecture/vector-search.html
    * section 2.4).
    */
  def searchEf(topK: Int, parameters: Map[String, String]): Int = {
    require(
      parameters != null && parameters.keySet.subsetOf(Set("ef")),
      "HNSW search supports only the ef parameter"
    )
    val ef = parameters
      .get("ef")
      .map { value =>
        require(
          value != null && value.matches("[0-9]+"),
          "HNSW ef must be a positive integer"
        )
        val parsed = Try(value.toInt)
          .getOrElse(
            throw new IllegalArgumentException(
              "HNSW ef exceeds the supported integer range"
            )
          )
        require(parsed > 0, "HNSW ef must be positive")
        parsed
      }
      .getOrElse(math.max(64, topK))
    require(ef >= topK, "HNSW ef must be at least topK")
    ef
  }

  /** Adds this segment's candidates to `merger`, counted the way the group
    * counts its queries. `excluded` covers the segment's rows; a set bit is a
    * row the search must not return. A nullable column's index holds only the
    * rows that have a value, so both the mask and the returned labels go
    * through the index's row mapping (section 2.4).
    */
  def run(
      handle: SegmentIndexHandle,
      queries: QueryMatrix,
      excluded: BitSet,
      k: Int,
      parameters: Map[String, String],
      allocator: BufferAllocator,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit = _ => ()
  ): Unit = {
    require(k > 0, s"topK must be positive: $k")
    require(
      queries.dimension == handle.dimension,
      s"Queries have ${queries.dimension} dimensions; the index has ${handle.dimension}"
    )
    val rows = handle.rows
    require(
      rows <= 256L * 1024 * 1024 * 8,
      "Segment exclusion bitmap exceeds the supported size"
    )
    val labels = handle.mapping.labelsOf(excluded)
    val visible = rows - labels.cardinality()
    if (visible <= 0) return
    val count = math.min(k.toLong, visible).toInt
    val maskBytes = (rows + 7L) / 8L
    val mask = allocator.buffer(maskBytes)
    val ids = allocator.buffer(queries.queries.toLong * count * 8L)
    val scores = allocator.buffer(queries.queries.toLong * count * 4L)
    try {
      writeMask(labels, rows, mask)
      var width = searchWidth(handle.family, count, parameters)
      var attempt = 0
      var complete = false
      // One query's ids, sorted to find a duplicate. Reused by every query of
      // every attempt: what the check needs is a scratch array, not a set of
      // boxed longs for each of the group's candidates.
      val scratch = new Array[Long](count)
      while (!complete) {
        val started = System.nanoTime()
        handle.index.search(
          queries.buffer,
          queries.queries.toLong,
          count,
          bytes(mask, maskBytes),
          bytes(ids, queries.queries.toLong * count * 8L),
          bytes(scores, queries.queries.toLong * count * 4L),
          searchParameters(handle, width)
        )
        // Knowhere does not report how many rows a probe visited, so a probe
        // counts its progress in calls rather than in compared pairs.
        onProgress(
          SegmentSearch.Progress(1, System.nanoTime() - started, 0L, 0)
        )
        val short = checked(
          queries.queries,
          count,
          ids,
          scores,
          labels,
          rows,
          handle,
          handle.metric,
          scratch
        )
        attempt += 1
        if (!short) complete = true
        else if (width.exists(_.toLong < rows) && attempt < MaxAttempts)
          width = width.map(value =>
            math
              .min(rows, math.min(Int.MaxValue.toLong, value.toLong * 2L))
              .toInt
          )
        else
          throw new IllegalArgumentException(
            s"Segment ${handle.segmentId}: ${handle.indexType} returned fewer than $count of $visible visible rows for a query after $attempt attempts"
          )
      }
      collect(queries.queries, count, ids, scores, handle, merger)
    } finally {
      scores.close()
      ids.close()
      mask.close()
    }
  }

  /** How wide the search starts: `ef` for the HNSW family, `nprobe` for the IVF
    * family, and nothing to widen for a flat index, which already looks at
    * every row.
    */
  private[index] def searchWidth(
      family: String,
      topK: Int,
      parameters: Map[String, String]
  ): Option[Int] = family match {
    case "HNSW" => Some(searchEf(topK, parameters))
    case "IVF"  => Some(searchNprobe(parameters))
    case _ =>
      require(
        parameters.isEmpty,
        s"A flat index takes no search parameters: ${parameters.keySet.toSeq.sorted.mkString(", ")}"
      )
      None
  }

  private def searchParameters(
      handle: SegmentIndexHandle,
      width: Option[Int]
  ): String = {
    val name = if (handle.family == "HNSW") "ef" else "nprobe"
    width match {
      case Some(value) =>
        s"""{"metric_type":"${handle.metric}","$name":$value}"""
      case None => s"""{"metric_type":"${handle.metric}"}"""
    }
  }

  /** How many inverted lists an IVF search visits: `nprobe` if the caller named
    * it, otherwise 16, which is what Milvus searches by default.
    */
  private[index] def searchNprobe(parameters: Map[String, String]): Int = {
    require(
      parameters != null && parameters.keySet.subsetOf(Set("nprobe")),
      "IVF search supports only the nprobe parameter"
    )
    parameters
      .get("nprobe")
      .map { value =>
        require(
          value != null && value.matches("[0-9]+"),
          "IVF nprobe must be a positive integer"
        )
        val parsed = Try(value.toInt).getOrElse(
          throw new IllegalArgumentException(
            "IVF nprobe exceeds the supported integer range"
          )
        )
        require(parsed > 0, "IVF nprobe must be positive")
        parsed
      }
      .getOrElse(16)
  }

  private def writeMask(excluded: BitSet, rows: Long, mask: ArrowBuf): Unit = {
    mask.setZero(0, mask.capacity())
    var row = excluded.nextSetBit(0)
    while (row >= 0 && row < rows) {
      val index = row.toLong / 8L
      mask.setByte(index, mask.getByte(index) | (1 << (row % 8)))
      row = excluded.nextSetBit(row + 1)
    }
  }

  /** Checks what the index returned and says whether any query came back with
    * fewer than `count` hits, without building a candidate: an answer that is
    * short is searched again with a wider `ef`, and anything this pass built
    * would be thrown away. Reading the buffers twice costs two passes over
    * native memory; building the candidates first cost one object per hit and a
    * regrouping of all of them (section 2.4).
    */
  private def checked(
      queries: Int,
      count: Int,
      ids: ArrowBuf,
      scores: ArrowBuf,
      excluded: BitSet,
      rows: Long,
      handle: SegmentIndexHandle,
      metric: String,
      scratch: Array[Long]
  ): Boolean = {
    var short = false
    var query = 0
    while (query < queries) {
      var slot = 0
      var hits = 0
      while (slot < count) {
        val position = query.toLong * count + slot
        val id = ids.getLong(position * 8L)
        if (id != -1L) {
          require(
            id >= 0 && id < rows,
            s"Segment ${handle.segmentId}: the index returned row $id outside its $rows rows"
          )
          require(
            !excluded.get(id.toInt),
            s"Segment ${handle.segmentId}: the index returned excluded row $id"
          )
          val score = scores.getFloat(position * 4L)
          require(
            JavaFloat.isFinite(score) && (metric != "L2" || score >= 0),
            s"Segment ${handle.segmentId}: the index returned an invalid score for row $id"
          )
          scratch(hits) = id
          hits += 1
        }
        slot += 1
      }
      // A row the index returned twice for one query ends up beside itself.
      Arrays.sort(scratch, 0, hits)
      var at = 1
      while (at < hits) {
        require(
          scratch(at) != scratch(at - 1),
          s"Segment ${handle.segmentId}: the index returned row ${scratch(at)} twice"
        )
        at += 1
      }
      if (hits < count) short = true
      query += 1
    }
    short
  }

  /** The hits of an answer [[checked]] accepted, added to the merger. The
    * labels the index returned go through the handle's row mapping, which is
    * what a nullable column's index needs (section 2.4).
    */
  private def collect(
      queries: Int,
      count: Int,
      ids: ArrowBuf,
      scores: ArrowBuf,
      handle: SegmentIndexHandle,
      merger: TopKMerger
  ): Unit = {
    var query = 0
    while (query < queries) {
      var slot = 0
      while (slot < count) {
        val position = query.toLong * count + slot
        val id = ids.getLong(position * 8L)
        if (id != -1L)
          merger.add(
            Candidate(
              query,
              handle.segmentId,
              handle.mapping.rowOf(id),
              scores.getFloat(position * 4L).toDouble
            )
          )
        slot += 1
      }
      query += 1
    }
  }

  private def bytes(buffer: ArrowBuf, length: Long) =
    buffer.nioBuffer(0, length.toInt).order(ByteOrder.nativeOrder())
}
