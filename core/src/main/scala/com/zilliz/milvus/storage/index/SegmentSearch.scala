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
        allocator: BufferAllocator
    ): (TopKMerger, Counters) = {
      val merger = new TopKMerger(queries.queries, k, metric)
      var nativeCalls = 0
      var nativeNanos = 0L
      def counted(nanos: Long): Unit = {
        nativeCalls += 1
        nativeNanos += nanos
      }
      sources.foreach {
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
      (merger, Counters(sources.size, nativeCalls, nativeNanos))
    }

    override def close(): Unit = {
      val failures = (batches.values.flatten ++ sources).flatMap(closeable =>
        scala.util.Try(closeable.close()).failed.toOption
      )
      failures.headOption.foreach(throw _)
    }
  }

  /** Reads the segment set into memory: every batch of an exact scan, every
    * index handle of an index probe. The vectors of one set fit
    * `vectorsMaxBytes` by construction, and this checks what it actually read
    * against that.
    */
  def hold(
      segments: Seq[Long],
      open: Long => Source,
      vectorsMaxBytes: Long,
      layout: VectorLayout
  ): Held = {
    require(segments != null, "A task must name its segments")
    val sources = Seq.newBuilder[Source]
    val batches = Map.newBuilder[Long, Seq[SegmentVectors.Batch]]
    var retained = 0L
    var read = ReadMetrics.Zero
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
              require(
                retained <= vectorsMaxBytes,
                s"Segment $id fills more than the $vectorsMaxBytes bytes of vectors a task keeps; " +
                  s"raise milvus.search.vectors.max.bytes or search fewer queries at a time"
              )
              held += batch
              next = vectors.next()
            }
            batches += id -> held.result()
            read = read + vectors.metrics
          case Index(_, _, _) =>
        }
      }
      val result = new Held(sources.result(), batches.result().toMap, read)
      logInfo(
        s"Segment set held: segments=${segments.size}, retainedBytes=${result.retainedBytes}, " +
          s"rowBytes=${layout.rowBytes}"
      )
      result
    } catch {
      case failure: Throwable =>
        val opened = new Held(sources.result(), batches.result().toMap, read)
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
      read: ReadMetrics = ReadMetrics.Zero
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
      allocator: BufferAllocator
  ): (TopKMerger, Counters) = {
    require(segments != null, "A task must name its segments")
    val merger = new TopKMerger(queries.queries, k, metric)
    var nativeCalls = 0
    var nativeNanos = 0L
    def counted(nanos: Long): Unit = {
      nativeCalls += 1
      nativeNanos += nanos
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
    }
    logInfo(
      s"Segment set searched: segments=${segments.size}, queries=${queries.queries}, " +
        s"topK=$k, metric=$metric, candidates=${merger.size}, " +
        s"nativeSearchCalls=$nativeCalls, nativeMillis=${nativeNanos / 1000000L}"
    )
    (merger, Counters(segments.size, nativeCalls, nativeNanos, read))
  }
}
