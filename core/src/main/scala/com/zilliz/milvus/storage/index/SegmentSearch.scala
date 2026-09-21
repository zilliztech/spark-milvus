package com.zilliz.milvus.storage.index

import java.util.BitSet

import org.apache.arrow.memory.BufferAllocator

import com.zilliz.milvus.storage.read.exec.{
  ReadMetrics,
  SegmentIndexHandle,
  SegmentVectors
}
import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.milvus.storage.Logging

/** Runs one query group over one segment set and merges what the segments find.
  *
  * The two strategies take the same input and give the same output: exact
  * scanning works on the vector batches a segment hands out, index probing on
  * the index handle it opens. Whichever runs, the task keeps each query's best
  * k and nothing else (docs/design/architecture/vector-search.html sections
  * 2.1, 2.3 and 2.4).
  */
object SegmentSearch extends Logging {

  /** What the format side offers for one segment. A segment carries either its
    * vectors or its index; `Exact` is also what an unindexed segment falls back
    * to when the search allows it.
    */
  sealed trait Source extends AutoCloseable {
    def segmentId: Long
  }

  final case class Exact(segmentId: Long, vectors: SegmentVectors)
      extends Source {
    override def close(): Unit = vectors.close()
  }

  final case class Index(
      segmentId: Long,
      handle: SegmentIndexHandle,
      excluded: BitSet
  ) extends Source {
    override def close(): Unit = handle.close()
  }

  /** One segment set, read into memory and kept there while query group after
    * query group runs on it.
    *
    * This is how a task answers a query set too large to fit one group: the
    * groups arrive one at a time, and what stays is the segment set, which the
    * planner sized against `milvus.search.vectors.max.bytes`. A search with one
    * query group uses [[run]] instead and holds one batch at a time
    * (docs/design/architecture/vector-search.html sections 2.1 and 2.3).
    */
  final class Held private[index] (
      private val sources: Seq[Source],
      private val batches: Map[Long, Seq[SegmentVectors.Batch]],
      val read: ReadMetrics = ReadMetrics.Zero
  ) extends AutoCloseable {

    def segments: Int = sources.size

    /** The bytes of vectors this task keeps. */
    def retainedBytes: Long =
      batches.values.flatten.map(_.base.buffer.capacity().toLong).sum

    /** One query group over the whole set. */
    def search(
        queries: QueryMatrix,
        k: Int,
        metric: String,
        parameters: Map[String, String],
        allocator: BufferAllocator,
        onProgress: Progress => Unit = _ => ()
    ): (TopKMerger, Counters) = {
      val merger = new TopKMerger(queries.queries, k, metric)
      var nativeCalls = 0
      var nativeNanos = 0L
      var compared = 0L
      def counted(step: Progress): Unit = {
        nativeCalls += step.nativeCalls
        nativeNanos += step.nativeNanos
        compared += step.compared
        onProgress(step)
      }
      sources.foreach { source =>
        source match {
          case Exact(id, _) =>
            batches(id).foreach(
              ExactScan
                .batch(_, queries, id, k, metric, allocator, merger, counted)
            )
          case Index(id, handle, excluded) =>
            require(
              handle.metric == metric,
              s"Segment $id has a $metric query on a ${handle.metric} index"
            )
            IndexProbe.run(
              handle,
              queries,
              excluded,
              k,
              parameters,
              allocator,
              merger,
              counted
            )
        }
        onProgress(Progress(0, 0L, 0L, 1))
      }
      (merger, Counters(sources.size, nativeCalls, nativeNanos, compared))
    }

    override def close(): Unit = {
      val failures = (batches.values.flatten ++ sources).flatMap(closeable =>
        scala.util.Try(closeable.close()).failed.toOption
      )
      failures.headOption.foreach(throw _)
    }
  }

  /** A set that did not fit: the segment whose batch went over, the bytes
    * retained by then, and the budget. Everything read so far is closed; the
    * caller streams the set instead, one batch at a time for every query group
    * (docs/design/architecture/search-resources.html section 3.3).
    */
  final case class Overflow(
      segmentId: Long,
      retainedBytes: Long,
      budgetBytes: Long,
      read: ReadMetrics
  )

  /** Reads the segment set into memory: every batch of an exact scan, every
    * index handle of an index probe. The planner sized the set to fit
    * `vectorsMaxBytes` from the row counts it had; this measures what was
    * actually read, and a set that goes over comes back as an [[Overflow]] with
    * nothing left open, rather than a failure.
    */
  def hold(
      segments: Seq[Long],
      open: Long => Source,
      vectorsMaxBytes: Long,
      layout: VectorLayout
  ): Either[Overflow, Held] = {
    require(segments != null, "A task must name its segments")
    val sources = Seq.newBuilder[Source]
    val batches = Map.newBuilder[Long, Seq[SegmentVectors.Batch]]
    var retained = 0L
    var read = ReadMetrics.Zero
    def opened = new Held(sources.result(), batches.result().toMap, read)
    try {
      segments.foreach { segmentId =>
        val source = open(segmentId)
        sources += source
        source match {
          case Exact(id, vectors) =>
            val held = Seq.newBuilder[SegmentVectors.Batch]
            var next = vectors.next()
            while (next.nonEmpty) {
              val batch = next.get
              retained += batch.base.buffer.capacity().toLong
              held += batch
              if (retained > vectorsMaxBytes) {
                batches += id -> held.result()
                read = read + vectors.metrics
                opened.close()
                logInfo(
                  s"Segment set not held: segment $id brings the vectors read to $retained bytes, " +
                    s"over the $vectorsMaxBytes a task keeps; the set streams instead, once per query group"
                )
                return Left(Overflow(id, retained, vectorsMaxBytes, read))
              }
              next = vectors.next()
            }
            batches += id -> held.result()
            read = read + vectors.metrics
          case Index(_, _, _) =>
        }
      }
      val result = opened
      logInfo(
        s"Segment set held: segments=${segments.size}, retainedBytes=${result.retainedBytes}, " +
          s"rowBytes=${layout.rowBytes}"
      )
      Right(result)
    } catch {
      case failure: Throwable =>
        try opened.close()
        catch { case closing: Throwable => failure.addSuppressed(closing) }
        throw failure
    }
  }

  /** Counts what one task did, for the accumulators section 1.3 lists.
    *
    * `read` is what the exact scan pulled through the reader; an index probe
    * reads its files when the handle opens, which the handle itself reports.
    */
  final case class Counters(
      segments: Int,
      nativeCalls: Int,
      nativeNanos: Long,
      compared: Long = 0L,
      read: ReadMetrics = ReadMetrics.Zero
  )

  /** One finished step of a search, handed to the caller as it happens.
    *
    * `Counters` is the same quantities summed, and a caller that only wants the
    * total can ignore this. A caller that has somewhere to publish them needs
    * them before the task ends: a search of one query group against one segment
    * set runs for as long as the vectors take, and a total reported at the end
    * says nothing while it runs.
    *
    * `compared` is the pairs a step measured distances for, queries times the
    * rows that survived the exclusion mask. An index probe does not compare
    * every row and Knowhere does not say how many it did, so a probe reports
    * zero and counts its progress in `nativeCalls`.
    */
  final case class Progress(
      nativeCalls: Int,
      nativeNanos: Long,
      compared: Long,
      segments: Int
  )

  /** Searches every segment of the set in turn. Sources are opened one at a
    * time by `open`, so a task holds one segment's data at once, and each is
    * closed before the next one opens.
    */
  def run(
      segments: Seq[Long],
      open: Long => Source,
      queries: QueryMatrix,
      k: Int,
      metric: String,
      parameters: Map[String, String],
      allocator: BufferAllocator,
      onProgress: Progress => Unit = _ => ()
  ): (TopKMerger, Counters) = {
    require(segments != null, "A task must name its segments")
    val merger = new TopKMerger(queries.queries, k, metric)
    var nativeCalls = 0
    var nativeNanos = 0L
    var compared = 0L
    def counted(step: Progress): Unit = {
      nativeCalls += step.nativeCalls
      nativeNanos += step.nativeNanos
      compared += step.compared
      onProgress(step)
    }
    var read = ReadMetrics.Zero
    segments.foreach { segmentId =>
      val source = open(segmentId)
      try
        source match {
          case Exact(id, vectors) =>
            ExactScan.run(
              vectors,
              queries,
              id,
              k,
              metric,
              allocator,
              merger,
              counted
            )
            read = read + vectors.metrics
          case Index(id, handle, excluded) =>
            require(
              handle.metric == metric,
              s"Segment $id has a $metric query on a ${handle.metric} index"
            )
            IndexProbe.run(
              handle,
              queries,
              excluded,
              k,
              parameters,
              allocator,
              merger,
              counted
            )
        }
      finally source.close()
      onProgress(Progress(0, 0L, 0L, 1))
    }
    logInfo(
      s"Segment set searched: segments=${segments.size}, queries=${queries.queries}, " +
        s"topK=$k, metric=$metric, candidates=${merger.size}, " +
        s"nativeSearchCalls=$nativeCalls, nativeMillis=${nativeNanos / 1000000L}"
    )
    (merger, Counters(segments.size, nativeCalls, nativeNanos, compared, read))
  }
}
