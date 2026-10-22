package com.zilliz.milvus.storage.vector

import java.lang.{Float => JavaFloat}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{Arrays, BitSet}
import java.util.concurrent.ExecutorService

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}

import com.zilliz.milvus.jni.vector.NativeVectorSearch
import com.zilliz.milvus.storage.schema.MetricType

import io.knowhere.DType

/** Searches a unit by computing every distance, one native call per batch.
  *
  * The batches, their exclusion bitmaps and their row offsets come from the
  * input: a Milvus segment's from the format side, a DataFrame input's from its
  * rows ([[VectorBatch]]). This only calls the engine and turns what comes back
  * into candidates, a candidate's place being the unit's id and the row's
  * position in it (design document architecture/vector-search.html section
  * 2.3).
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
  * (design document architecture/dataframe-api.html section 2).
  *
  * Under L2 the batched entry computes |x|^2 + |y|^2 - 2x.y in float32, which
  * errs by as much as the squared norms are large, whatever the distance, and
  * picks the wrong rows when vectors lie far from the origin and near each
  * other. With a function, its candidates are only nominations: its rescored
  * variant computes again, pair by pair, those that can still be kept, a bound
  * on the entry's error shows that the rows it did not name cannot beat them,
  * and a query it cannot show that for is searched over the batch again pair by
  * pair (decision 37, design document
  * architecture/exact-search-and-index-build-internals.html section 1.8).
  */
object ExactScan {

  /** How many candidates an L2 query asks the batched entry for when they are
    * nominations: enough that the rows the entry leaves out can be shown not to
    * beat the query's k. Once the batch is merged, about k times its rows over
    * all the rows scanned are among the best k (all k on a first batch); the
    * count is that many with three standard deviations and 8 to spare, never
    * fewer than k and never more than k + min(k, 64), because a wider heap
    * slows faiss on every batch that asks for one. A count that falls short
    * costs a second search of the query, never a wrong answer. The entry's
    * output, 16 bytes a candidate, lives for one call, as its 12 bytes a
    * candidate did before.
    */
  def nominated(k: Int, batchRows: Int, scannedRows: Long): Int = {
    require(k > 0, s"topK must be positive: $k")
    require(batchRows > 0, s"A batch of $batchRows rows")
    require(scannedRows >= 0L, s"$scannedRows rows scanned")
    val most = k + math.min(k, 64)
    val expected = k.toDouble * batchRows / (scannedRows + batchRows)
    val wanted = math.ceil(expected + 3.0 * math.sqrt(expected)).toLong + 8L
    math.min(most.toLong, math.max(k.toLong, wanted)).toInt
  }

  /** float32's unit roundoff, 2^-24. */
  private final val UnitRoundoff = 1.0 / (1L << 24)

  /** How far the batched entry's L2 for a query and any row of a batch can be
    * from their squared distance: the query's squared norm, the row's and their
    * dot product each err by at most `gamma` of the norms' product, whatever
    * the order of the sums and with or without fused multiply-adds, and the sum
    * and the difference by one rounding each; the factor of 2 is margin.
    */
  private def engineError(
      querySquaredNorm: Double,
      largestSquaredNorm: Double,
      gamma: Double
  ): Double = {
    val reach = math.sqrt(querySquaredNorm) + math.sqrt(largestSquaredNorm)
    2.0 * gamma * reach * reach
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
      ranked: Option[EngineRange.Batch] = None,
      collector: Option[ExecutorService] = None,
      threads: Int = 1
  ): Unit = {
    require(k > 0, s"topK must be positive: $k")
    require(threads > 0, s"A scan collects on at least one thread: $threads")
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
          onProgress,
          ranked,
          collector,
          threads
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
          onProgress,
          collector,
          threads
        )
    }
    ranked.foreach(
      scoreByFunction(
        current,
        _,
        queries,
        segmentId,
        merger,
        onProgress,
        collector,
        threads
      )
    )
    SearchThreads.collect(collector) {
      merger.countScanned(current.visibleRows)
    }
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
      onProgress: SegmentSearch.Progress => Unit,
      collector: Option[ExecutorService],
      threads: Int
  ): Unit = if (ranked.rankedRows.nonEmpty) {
    // Keep function evaluation on the caller, handing over at most this many
    // scores at once. The buffer is reused only after collection has returned.
    val capacity =
      math.min(4096L, queries.queries.toLong * ranked.rankedRows.length).toInt
    val queryIds = new Array[Int](capacity)
    val rowIds = new Array[Long](capacity)
    val scores = new Array[Float](capacity)
    var used = 0
    def flush(): Unit = if (used > 0) {
      val count = used
      SearchThreads.collect(collector) {
        val firstQuery = queryIds(0)
        SearchThreads.shards(queryIds(count - 1) - firstQuery + 1, threads) {
          (_, from, until) =>
            val first = firstQuery + from
            val end = firstQuery + until
            // Candidates are in query order. A shard owns whole queries,
            // including when one query fills the entire temporary buffer.
            var low = 0
            var high = count
            while (low < high) {
              val middle = low + (high - low) / 2
              if (queryIds(middle) < first) low = middle + 1
              else high = middle
            }
            var slot = low
            while (slot < count && queryIds(slot) < end) {
              val at = queryIds(slot)
              val score = scores(slot)
              if (!merger.rejectsScore(at, score))
                merger.add(at, segmentId, rowIds(slot), score)
              slot += 1
            }
        }
      }
      used = 0
    }
    val query = new Array[Float](queries.dimension)
    var at = 0
    while (at < queries.queries) {
      EngineRange.decode(queries.buffer, at, queries.layout, query)
      var row = 0
      while (row < ranked.rankedRows.length) {
        val score = ranked.function.score(query, ranked.rankedValues(row))
        if (score != null) {
          queryIds(used) = at
          rowIds(used) = current.firstRow + ranked.rankedRows(row)
          scores(used) = score.floatValue()
          used += 1
          if (used == capacity) flush()
        }
        row += 1
      }
      at += 1
    }
    flush()
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
  final class Compacted private[vector] (
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
  private[vector] def compact(
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
      onProgress: SegmentSearch.Progress => Unit,
      ranked: Option[EngineRange.Batch],
      collector: Option[ExecutorService],
      threads: Int
  ): Unit = {
    val parameters = s"""{"metric_type":"${metric.name}"}"""
    // Under L2 the entry's candidates are only nominations (decision 37).
    val nominating = ranked.filter(_ => metric == MetricType.L2)
    val count = math.min(
      nominating.fold(k)(_ =>
        SearchThreads.collect(collector) {
          nominated(k, visible, merger.scannedRows)
        }
      ),
      visible
    )
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
    // The queries a nomination could not answer for, and the rows each was
    // given already: they are searched again once these buffers are released.
    val (again, offered) =
      try {
        val (base, rows) = compacted match {
          case Some(c) => (bytes(c.buffer, c.buffer.capacity()), c.rows)
          case None    => (current.base.buffer, current.rows)
        }
        nominating match {
          case Some(batch) =>
            nominate(
              current,
              batch,
              visible,
              base,
              rows,
              queries,
              count,
              ids,
              scores,
              segmentId,
              allocator,
              merger,
              compacted.map(_.batchRows),
              parameters,
              onProgress,
              collector,
              threads
            )
          case None =>
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
            SearchThreads.collect(collector) {
              SearchThreads.shards(queries.queries, threads) {
                (_, from, until) =>
                  collect(
                    excluded,
                    from,
                    until,
                    count,
                    ids,
                    scores,
                    segmentId,
                    current.firstRow,
                    merger,
                    rows,
                    compacted.map(_.batchRows)
                  )
              }
            }
            (Array.emptyIntArray, Array.empty[Array[Int]])
        }
      } finally {
        scores.close()
        ids.close()
        compacted.foreach(_.close())
      }
    nominating.foreach(batch =>
      if (again.nonEmpty)
        rescan(
          current,
          batch,
          visible,
          queries,
          again,
          offered,
          segmentId,
          k,
          metric,
          allocator,
          merger,
          onProgress,
          collector,
          threads
        )
    )
  }

  /** L2 over the batched entry, whose ||x||^2 + ||y||^2 - 2x.y errs by as much
    * as the squared norms are large (decision 37). Its candidates are only
    * nominations: the rescored entry computes again, pair by pair, every
    * candidate of a query that keeps fewer than k, and of a full query those
    * whose estimate less the entry's error could still beat its worst, and the
    * merger takes those distances. When the entry named as many rows as it was
    * asked for, the rest of the batch estimates no nearer than its last; if
    * even that row's bound could still be kept, the query is to be searched
    * over the batch again pair by pair. Returns those queries in ascending
    * order, with the rows each was given here, sorted.
    */
  private def nominate(
      current: VectorBatch,
      ranked: EngineRange.Batch,
      visible: Int,
      base: ByteBuffer,
      rows: Int,
      queries: QueryMatrix,
      count: Int,
      ids: ArrowBuf,
      scores: ArrowBuf,
      segmentId: Long,
      allocator: BufferAllocator,
      merger: TopKMerger,
      batchRows: Option[Array[Int]],
      parameters: String,
      onProgress: SegmentSearch.Progress => Unit,
      collector: Option[ExecutorService],
      threads: Int
  ): (Array[Int], Array[Array[Int]]) = {
    val nq = queries.queries
    val gamma = (queries.dimension + 4) * UnitRoundoff
    // A distance computed pair by pair, or by the ranking function, is within
    // its rounding of the squared distance: at least this share of it.
    val shrink = 1.0 - 2.0 * gamma
    val errors = Array.tabulate(nq)(at =>
      engineError(queries.squaredNorms(at), ranked.largestSquaredNorm, gamma)
    )
    val cutoffs = allocator.buffer(nq.toLong * 4L)
    val direct = allocator.buffer(nq.toLong * count * 4L)
    val counts = allocator.buffer(nq.toLong * 4L)
    try {
      SearchThreads.collect(collector) {
        SearchThreads.shards(nq, threads) { (_, from, until) =>
          var at = from
          while (at < until) {
            // A full query keeps a row only when (estimate - error) * shrink can
            // beat its worst, that is when the estimate is at most this. A worst of
            // NaN, a row the function scored, ranks after every number.
            val cutoff =
              if (!merger.isFull(at)) Float.PositiveInfinity
              else {
                val worst = merger.worstScore(at)
                if (worst.isNaN) Float.PositiveInfinity
                else floatAbove(worst / shrink + errors(at))
              }
            cutoffs.setFloat(at.toLong * 4L, cutoff)
            at += 1
          }
        }
      }
      val started = System.nanoTime()
      NativeVectorSearch.bruteForceBatchedL2Rescored(
        queries.layout.dtype,
        base,
        rows.toLong,
        queries.buffer,
        nq.toLong,
        queries.dimension,
        count,
        bytes(cutoffs, nq.toLong * 4L),
        bytes(ids, nq.toLong * count * 8L),
        bytes(scores, nq.toLong * count * 4L),
        bytes(direct, nq.toLong * count * 4L),
        bytes(counts, nq.toLong * 4L),
        parameters
      )
      onProgress(
        SegmentSearch.Progress(
          1,
          System.nanoTime() - started,
          nq.toLong * visible.toLong,
          0
        )
      )
      val collected = SearchThreads.collect(collector) {
        SearchThreads.shards(nq, threads) { (_, from, until) =>
          val offered = new Array[Int](count)
          val again = Array.newBuilder[Int]
          // The rows each query searched again was given already: the merger
          // keeps a row once.
          val alreadyOffered = Array.newBuilder[Array[Int]]
          var at = from
          while (at < until) {
            val computed = counts.getInt(at.toLong * 4L)
            var slot = 0
            while (slot < computed) {
              val position = at.toLong * count + slot
              val batchRow =
                rowOf(
                  ids.getLong(position * 8L),
                  rows,
                  batchRows,
                  ranked.engineExcluded
                )
              val distance = finite(direct.getFloat(position * 4L), batchRow)
              if (!merger.rejectsScore(at, distance))
                merger.add(at, segmentId, current.firstRow + batchRow, distance)
              offered(slot) = batchRow
              slot += 1
            }
            if (count < visible) {
              val position = at.toLong * count + count - 1
              val last = finite(
                scores.getFloat(position * 4L),
                rowOf(
                  ids.getLong(position * 8L),
                  rows,
                  batchRows,
                  ranked.engineExcluded
                )
              )
              // Keep the lower-bound arithmetic in Double, then compare on the
              // merger's Float scale. Rounding is monotone; an equal score
              // remains eligible for the address tie-break.
              val lowerBound = ((last.toDouble - errors(at)) * shrink).toFloat
              if (!merger.rejectsScore(at, lowerBound)) {
                again += at
                val sorted = Arrays.copyOf(offered, computed)
                Arrays.sort(sorted)
                alreadyOffered += sorted
              }
            }
            at += 1
          }
          (again.result(), alreadyOffered.result())
        }
      }
      (collected.flatMap(_._1).toArray, collected.flatMap(_._2).toArray)
    } finally {
      counts.close()
      direct.close()
      cutoffs.close()
    }
  }

  /** The smallest float above `value`, which a float compared below it is not
    * above.
    */
  private def floatAbove(value: Double): Float = {
    val rounded = value.toFloat
    if (rounded.toDouble > value) rounded else Math.nextUp(rounded)
  }

  /** Searches the batch again for the queries `again` names, in ascending
    * order, through the per-query entry, which computes each distance pair by
    * pair and errs only as much as the distance is large; the merger takes
    * those distances, apart from the rows each query was given already, sorted
    * in `offered`. Consecutive queries go in one call, read where they lie in
    * the matrix: a search that has to look again copies no query.
    */
  private def rescan(
      current: VectorBatch,
      ranked: EngineRange.Batch,
      visible: Int,
      queries: QueryMatrix,
      again: Array[Int],
      offered: Array[Array[Int]],
      segmentId: Long,
      k: Int,
      metric: MetricType,
      allocator: BufferAllocator,
      merger: TopKMerger,
      onProgress: SegmentSearch.Progress => Unit,
      collector: Option[ExecutorService],
      threads: Int
  ): Unit = {
    val layout = queries.layout
    val parameters = s"""{"metric_type":"${metric.name}"}"""
    val count = math.min(k, visible)
    val mask = allocator.buffer((current.rows.toLong + 7L) / 8L)
    try {
      writeMask(ranked.engineExcluded, current.rows, mask)
      var from = 0
      while (from < again.length) {
        var until = from + 1
        while (until < again.length && again(until) == again(until - 1) + 1)
          until += 1
        val run = until - from
        val ids = allocator.buffer(run.toLong * count * 8L)
        val scores = allocator.buffer(run.toLong * count * 4L)
        try {
          val started = System.nanoTime()
          NativeVectorSearch.bruteForce(
            layout.dtype,
            current.base.buffer,
            current.rows.toLong,
            rowsOf(queries, again(from), run),
            run.toLong,
            layout.dimension,
            count,
            bytes(mask, mask.capacity()),
            bytes(ids, run.toLong * count * 8L),
            bytes(scores, run.toLong * count * 4L),
            parameters
          )
          onProgress(
            SegmentSearch.Progress(
              1,
              System.nanoTime() - started,
              0L,
              0,
              run.toLong
            )
          )
          SearchThreads.collect(collector) {
            SearchThreads.shards(run, threads) { (_, shardFrom, shardUntil) =>
              var query = from + shardFrom
              while (query < from + shardUntil) {
                // In ascending order: past the first the merger rejects, so is
                // the rest.
                val at = again(query)
                var slot = 0
                var open = true
                while (open && slot < count) {
                  val position = (query - from).toLong * count + slot
                  val id = ids.getLong(position * 8L)
                  if (id == -1L) open = false
                  else {
                    val batchRow =
                      rowOf(id, current.rows, None, ranked.engineExcluded)
                    val distance =
                      finite(scores.getFloat(position * 4L), batchRow)
                    if (merger.rejectsScore(at, distance)) open = false
                    else if (Arrays.binarySearch(offered(query), batchRow) < 0)
                      merger.add(
                        at,
                        segmentId,
                        current.firstRow + batchRow,
                        distance
                      )
                  }
                  slot += 1
                }
                query += 1
              }
            }
          }
        } finally {
          scores.close()
          ids.close()
        }
        from = until
      }
    } finally mask.close()
  }

  /** `run` queries of the matrix from `first` on, where they lie. */
  private def rowsOf(queries: QueryMatrix, first: Int, run: Int): ByteBuffer = {
    val rowBytes = queries.layout.rowBytes
    val view = queries.buffer.duplicate()
    view.position(first * rowBytes)
    view.limit((first + run) * rowBytes)
    view.slice().order(ByteOrder.nativeOrder())
  }

  /** The batch row an engine row number names: through `batchRows` when the
    * batch was compacted, and never one the engine was told to skip.
    */
  private def rowOf(
      id: Long,
      rows: Int,
      batchRows: Option[Array[Int]],
      excluded: BitSet
  ): Int = {
    require(
      id >= 0 && id < rows,
      s"The engine returned row $id of a batch with $rows rows"
    )
    val row = batchRows match {
      case Some(mapping) => mapping(id.toInt)
      case None          => id.toInt
    }
    require(!excluded.get(row), s"The engine returned excluded row $row")
    row
  }

  private def finite(score: Float, row: Int): Float = {
    require(
      JavaFloat.isFinite(score),
      s"The engine returned a score that is not finite for row $row"
    )
    score
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
      onProgress: SegmentSearch.Progress => Unit,
      collector: Option[ExecutorService],
      threads: Int
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
      SearchThreads.collect(collector) {
        SearchThreads.shards(queries.queries, threads) { (_, from, until) =>
          collect(
            excluded,
            from,
            until,
            count,
            ids,
            scores,
            segmentId,
            current.firstRow,
            merger,
            current.rows,
            None
          )
        }
      }
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
      fromQuery: Int,
      untilQuery: Int,
      count: Int,
      ids: ArrowBuf,
      scores: ArrowBuf,
      segmentId: Long,
      firstRow: Long,
      merger: TopKMerger,
      rows: Int,
      batchRows: Option[Array[Int]]
  ): Unit = {
    var query = fromQuery
    while (query < untilQuery) {
      var slot = 0
      while (slot < count) {
        val position = query.toLong * count + slot
        val id = ids.getLong(position * 8L)
        if (id != -1L) {
          val row = rowOf(id, rows, batchRows, excluded)
          val score = finite(scores.getFloat(position * 4L), row)
          merger.add(query, segmentId, firstRow + row, score)
        }
        slot += 1
      }
      query += 1
    }
  }

  private def bytes(buffer: ArrowBuf, length: Long) =
    buffer.nioBuffer(0, length.toInt).order(ByteOrder.nativeOrder())
}
