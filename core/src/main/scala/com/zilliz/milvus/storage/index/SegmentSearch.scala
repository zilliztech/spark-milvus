package com.zilliz.milvus.storage.index

import java.util.BitSet

import org.apache.arrow.memory.BufferAllocator

import com.zilliz.milvus.storage.read.exec.{SegmentIndexHandle, SegmentVectors}
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

  /** Counts what one task did, for the accumulators section 1.3 lists. */
  final case class Counters(
      segments: Int,
      nativeCalls: Int,
      nativeNanos: Long
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
    (merger, Counters(segments.size, nativeCalls, nativeNanos))
  }
}
