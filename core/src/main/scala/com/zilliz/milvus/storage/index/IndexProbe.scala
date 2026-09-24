package com.zilliz.milvus.storage.index

import java.lang.{Float => JavaFloat}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.{
  ExecutionException,
  Executors,
  Future,
  ThreadFactory
}
import java.util.BitSet
import scala.util.Try

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}

import com.zilliz.milvus.storage.read.exec.{IndexRowMapping, SegmentIndexHandle}

/** Searches a segment through the index the Milvus format side opened.
  *
  * One call answers a whole query group. A query that comes back with fewer
  * visible hits than it asked for is retried with a wider `ef`, up to eight
  * times, which is what section 2.4 fixes.
  *
  * A segment's groups go through a [[Pipeline]]: the task thread hands the
  * index one group after another, and a second thread checks and collects each
  * answer while the index is already searching the next group. On the P3 chunk
  * that was profiled a group's search took 238 ms of native time and its
  * checking and collecting 30 ms of Java time, one after the other on the task
  * thread while the other cores waited; pipelined, the Java time hides behind
  * the next search (docs/design/architecture/vector-search.html section 2.1,
  * 2026-09-24 decision).
  */
object IndexProbe {

  private val MaxAttempts = 8

  /** The searchable side of an index handle: what a probe needs of it, and no
    * more, so that a pipeline can be driven by something other than a loaded
    * index in tests.
    */
  trait Target {
    def segmentId: Long
    def rows: Long
    def dimension: Int
    def metric: String
    def indexType: String
    def family: String
    def mapping: IndexRowMapping

    /** Knowhere's search: `queryRows` queries of `dimension` values in
      * `queries`, the top `topK` of the rows not set in `excluded`, into `ids`
      * (int64, -1 pads a short answer) and `scores` (float32). Safe to call
      * from one thread at a time per buffer pair; two threads may search the
      * same index at once.
      */
    def search(
        queries: ByteBuffer,
        queryRows: Long,
        topK: Int,
        excluded: ByteBuffer,
        ids: ByteBuffer,
        scores: ByteBuffer,
        parameters: String
    ): Unit
  }

  /** A loaded index as a [[Target]]. */
  def target(handle: SegmentIndexHandle): Target = new Target {
    def segmentId: Long = handle.segmentId
    def rows: Long = handle.rows
    def dimension: Int = handle.dimension
    def metric: String = handle.metric
    def indexType: String = handle.indexType
    def family: String = handle.family
    def mapping: IndexRowMapping = handle.mapping
    def search(
        queries: ByteBuffer,
        queryRows: Long,
        topK: Int,
        excluded: ByteBuffer,
        ids: ByteBuffer,
        scores: ByteBuffer,
        parameters: String
    ): Unit =
      handle.index.search(
        queries,
        queryRows,
        topK,
        excluded,
        ids,
        scores,
        parameters
      )
  }

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

  /** Searches one group on one index and adds the hits to the merger, waiting
    * for the answer to be checked and collected before returning: a
    * [[Pipeline]] of one group.
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
    val pipeline = new Pipeline(
      target(handle),
      excluded,
      k,
      parameters,
      allocator,
      queries.queries
    )
    try {
      pipeline.run(queries, merger, onProgress)
      pipeline.finish(onProgress)
    } finally pipeline.close()
  }

  /** One segment's index answering query groups, the search of a group
    * overlapping the checking and collecting of the one before.
    *
    * The task thread calls [[run]] once per group: it searches into one of two
    * buffer pairs and hands the answer to a single worker thread, which checks
    * it (and, for a short answer, searches again wider on the same buffers
    * before it collects), then adds the hits to that group's merger. The two
    * buffer pairs mean a search can start while the previous answer is still
    * being read; a third group waits for the first's worker to finish, which on
    * the profiled chunk it never does, the worker being eight times faster than
    * the search. Every group's merger is touched by the worker alone until
    * [[finish]] returns, after which the task thread owns them again.
    *
    * `maxQueries` sizes the buffers once for the largest group; the exclusion
    * mask is written once for the segment rather than once per group.
    */
  final class Pipeline(
      target: Target,
      excluded: BitSet,
      k: Int,
      parameters: Map[String, String],
      allocator: BufferAllocator,
      maxQueries: Int
  ) extends AutoCloseable {
    require(k > 0, s"topK must be positive: $k")
    require(
      maxQueries > 0,
      s"A pipeline serves at least one query: $maxQueries"
    )
    private val rows = target.rows
    require(
      rows <= 256L * 1024 * 1024 * 8,
      "Segment exclusion bitmap exceeds the supported size"
    )
    private val labels = target.mapping.labelsOf(excluded)
    private val visible = rows - labels.cardinality()

    /** Hits asked of the index per query: k, or every visible row when the
      * segment has fewer.
      */
    val count: Int = math.max(0, math.min(k.toLong, visible)).toInt
    private val maskBytes = (rows + 7L) / 8L
    private val mask = allocator.buffer(maskBytes)
    private final class Slot {
      val ids: ArrowBuf = allocator.buffer(maxQueries.toLong * count * 8L)
      val scores: ArrowBuf = allocator.buffer(maxQueries.toLong * count * 4L)
      var pending: Option[Future[Unit]] = None
      def close(): Unit = {
        scores.close()
        ids.close()
      }
    }
    private val slots: Array[Slot] = Array(new Slot, new Slot)
    private var turn = 0
    private val worker = Executors.newSingleThreadExecutor(new ThreadFactory {
      def newThread(runnable: Runnable): Thread = {
        val thread =
          new Thread(runnable, s"index-probe-collect-${target.segmentId}")
        thread.setDaemon(true)
        thread
      }
    })
    // Scratch of the worker thread alone: where one query's ids land, to
    // find a duplicate, cleared by unsetting only the bits that query set.
    // `java.util.BitSet.clear` rescans for its highest set word on every call,
    // which over a 1.35M-row segment is a walk of 21,000 words for each of a
    // query's 100 ids; a plain word array clears in one store.
    private val seen = new Array[Long](((rows + 63L) >>> 6).toInt)
    private val touched = new Array[Int](math.max(count, 1))
    private var retriedCalls = 0
    private var retriedNanos = 0L
    if (count > 0) writeMask(labels, rows, mask)

    private def bytes(buffer: ArrowBuf, length: Long): ByteBuffer =
      buffer.nioBuffer(0, length.toInt).order(ByteOrder.nativeOrder())

    private def search(
        queries: QueryMatrix,
        slot: Slot,
        width: Option[Int]
    ): Long = {
      val started = System.nanoTime()
      target.search(
        queries.buffer,
        queries.queries.toLong,
        count,
        bytes(mask, maskBytes),
        bytes(slot.ids, queries.queries.toLong * count * 8L),
        bytes(slot.scores, queries.queries.toLong * count * 4L),
        searchParameters(target, width)
      )
      System.nanoTime() - started
    }

    /** Fails with the worker's exception if the slot's last answer failed. */
    private def await(slot: Slot): Unit = {
      slot.pending.foreach { future =>
        try future.get()
        catch {
          case wrapped: ExecutionException =>
            throw Option(wrapped.getCause).getOrElse(wrapped)
        }
      }
      slot.pending = None
    }

    /** Searches one group and hands its answer to the worker. Progress carries
      * the search alone; the worker's retries are reported by [[finish]].
      */
    def run(
        queries: QueryMatrix,
        merger: TopKMerger,
        onProgress: SegmentSearch.Progress => Unit
    ): Unit = {
      require(
        queries.dimension == target.dimension,
        s"Queries have ${queries.dimension} dimensions; the index has ${target.dimension}"
      )
      require(
        queries.queries <= maxQueries,
        s"A group of ${queries.queries} queries exceeds the ${maxQueries} this pipeline was sized for"
      )
      if (count == 0) return
      val slot = slots(turn)
      await(slot)
      val width = searchWidth(target.family, count, parameters)
      val nanos = search(queries, slot, width)
      onProgress(SegmentSearch.Progress(1, nanos, 0L, 0))
      slot.pending = Some(worker.submit[Unit] { () =>
        collectAnswer(queries, slot, merger, width)
      })
      turn ^= 1
    }

    /** Worker side: checks the answer, widens and searches again while any
      * query is short, then adds the hits to the merger.
      */
    private def collectAnswer(
        queries: QueryMatrix,
        slot: Slot,
        merger: TopKMerger,
        initialWidth: Option[Int]
    ): Unit = {
      var width = initialWidth
      var attempt = 0
      var complete = false
      while (!complete) {
        val short = checked(
          queries.queries,
          count,
          slot.ids,
          slot.scores,
          labels,
          rows,
          target,
          seen,
          touched
        )
        attempt += 1
        if (!short) complete = true
        else if (width.exists(_.toLong < rows) && attempt < MaxAttempts) {
          width = width.map(value =>
            math
              .min(rows, math.min(Int.MaxValue.toLong, value.toLong * 2L))
              .toInt
          )
          val nanos = search(queries, slot, width)
          synchronized {
            retriedCalls += 1
            retriedNanos += nanos
          }
        } else
          throw new IllegalArgumentException(
            s"Segment ${target.segmentId}: ${target.indexType} returned fewer than $count of $visible visible rows for a query after $attempt attempts"
          )
      }
      collect(queries.queries, count, slot.ids, slot.scores, target, merger)
    }

    /** Waits for every answer to be collected and reports the worker's own
      * searches. After this the mergers hold every group's hits.
      */
    def finish(onProgress: SegmentSearch.Progress => Unit): Unit = {
      slots.foreach(await)
      val (calls, nanos) = synchronized {
        val out = (retriedCalls, retriedNanos)
        retriedCalls = 0
        retriedNanos = 0L
        out
      }
      if (calls > 0) onProgress(SegmentSearch.Progress(calls, nanos, 0L, 0))
    }

    override def close(): Unit = {
      worker.shutdownNow()
      try slots.foreach(slot => Try(slot.pending.foreach(_.cancel(true))))
      finally {
        slots.foreach(_.close())
        mask.close()
      }
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

  private def searchParameters(target: Target, width: Option[Int]): String = {
    val name = if (target.family == "HNSW") "ef" else "nprobe"
    width match {
      case Some(value) =>
        s"""{"metric_type":"${target.metric}","$name":$value}"""
      case None => s"""{"metric_type":"${target.metric}"}"""
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
      target: Target,
      seen: Array[Long],
      touched: Array[Int]
  ): Boolean = {
    val metric = target.metric
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
            s"Segment ${target.segmentId}: the index returned row $id outside its $rows rows"
          )
          require(
            !excluded.get(id.toInt),
            s"Segment ${target.segmentId}: the index returned excluded row $id"
          )
          val score = scores.getFloat(position * 4L)
          require(
            JavaFloat.isFinite(score) && (metric != "L2" || score >= 0),
            s"Segment ${target.segmentId}: the index returned an invalid score for row $id"
          )
          // A row the index returned twice for one query has its bit set
          // already; the bits this query set are cleared below, so the next
          // query starts from an empty bitmap without clearing all the rows.
          val at = id.toInt
          val word = at >>> 6
          val bit = 1L << (at & 63)
          require(
            (seen(word) & bit) == 0L,
            s"Segment ${target.segmentId}: the index returned row $id twice"
          )
          seen(word) |= bit
          touched(hits) = at
          hits += 1
        }
        slot += 1
      }
      var cleared = 0
      while (cleared < hits) {
        val at = touched(cleared)
        seen(at >>> 6) &= ~(1L << (at & 63))
        cleared += 1
      }
      if (hits < count) short = true
      query += 1
    }
    short
  }

  /** Offers this segment's answer to the merger, three primitives at a time.
    *
    * Nothing here builds a `Candidate`: the merger keeps its runs in primitive
    * columns, so a candidate that cannot be kept costs one comparison and no
    * allocation. That is what most candidates of most segments are once a query
    * has seen one segment, and on the profiled P3 chunk the object-and-heap
    * version of this loop was 46% of the executor's Java samples. The labels
    * the index returned go through the target's row mapping, which is what a
    * nullable column's index needs (section 2.4).
    */
  private def collect(
      queries: Int,
      count: Int,
      ids: ArrowBuf,
      scores: ArrowBuf,
      target: Target,
      merger: TopKMerger
  ): Unit = {
    val segmentId = target.segmentId
    val mapping = target.mapping
    var query = 0
    while (query < queries) {
      var slot = 0
      while (slot < count) {
        val position = query.toLong * count + slot
        val id = ids.getLong(position * 8L)
        if (id != -1L) {
          val score = scores.getFloat(position * 4L).toDouble
          if (!merger.rejectsScore(query, score))
            merger.add(query, segmentId, mapping.rowOf(id), score)
        }
        slot += 1
      }
      query += 1
    }
  }
}
