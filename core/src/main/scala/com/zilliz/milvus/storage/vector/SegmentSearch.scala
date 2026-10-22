package com.zilliz.milvus.storage.vector

import java.util.concurrent.{Executors, Future, ThreadFactory}
import java.util.BitSet
import scala.collection.mutable
import scala.util.Try

import org.apache.arrow.memory.BufferAllocator

import com.zilliz.milvus.storage.schema.{MetricType, VectorLayout}
import com.zilliz.milvus.storage.Logging

/** Runs query groups over one segment set and merges what the segments find.
  * The two strategies take the same input and give the same output: exact
  * scanning works on the vector batches a segment hands out, indexed search on
  * the index handle it opens. Whichever runs, the task keeps each query's best
  * k and nothing else.
  */
object SegmentSearch extends Logging {

  /** What the format side offers for one segment. A segment carries either its
    * vectors or its index; `Exact` is also what an unindexed segment falls back
    * to when the search allows it.
    */
  sealed trait Source extends AutoCloseable {
    def segmentId: Long
  }

  final case class Exact(segmentId: Long, vectors: VectorBatch.Stream)
      extends Source {
    override def close(): Unit = vectors.close()
  }

  final case class Index(
      segmentId: Long,
      handle: IndexSearch.Target,
      excluded: BitSet
  ) extends Source {
    override def close(): Unit = handle.close()
  }

  /** One segment set, read into memory and kept there while query group after
    * query group runs on it.
    *
    * This is how a task answers query groups it cannot keep: the groups arrive
    * one at a time, and what stays is the segment set, which the planner sized
    * against the task's segment budget. A task that can keep its queries uses
    * [[runGroups]] instead and holds one segment at a time.
    */
  final class Held private[vector] (
      private val sources: Seq[Source],
      private val batches: Map[Long, Seq[VectorBatch]],
      val read: VectorBatch.ReadCost = VectorBatch.ReadCost.Zero,
      private val ranked: Map[VectorBatch, EngineRange.Batch] = Map.empty,
      private val rankNonFinite: Boolean = false,
      // How many segments' index searches a group keeps in flight at once:
      // 1 is the sequential search; 2 lets the next segment's call fill the
      // tail of the current one and the collect of the current one run behind.
      private val callsInFlight: Int = 1
  ) extends AutoCloseable {
    require(
      callsInFlight >= 1,
      s"A held set searches at least one segment at a time: $callsInFlight"
    )

    private def daemon(name: String): ThreadFactory = new ThreadFactory {
      def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, name)
        thread.setDaemon(true)
        thread
      }
    }
    // One collector for every pipeline of the task: the collects of successive
    // segments run one after another on it, into the group's one merger, while
    // the searches of the following segments go on.
    private val collector =
      Executors.newSingleThreadExecutor(daemon("search-collect"))
    // The native searches, callsInFlight at a time.
    private val searchers =
      Executors.newFixedThreadPool(callsInFlight, daemon("index-search"))

    def segments: Int = sources.size

    /** The bytes this task keeps: the vector batches of an exact scan and the
      * persisted indexes of an indexed search.
      */
    def retainedBytes: Long =
      batches.values.flatten.map(_.base.buffer.capacity().toLong).sum +
        sources.collect { case Index(_, handle, _) => handle.bytes }.sum

    /** One query group over the whole set. */
    // Each index keeps its mask and mapping. Concurrent calls share bounded
    // result slots and checking scratch, independent of the number of indexes.
    private val pipelines = mutable.HashMap.empty[Long, IndexSearch.Pipeline]
    private var workspace: Option[IndexSearch.Workspace] = None

    private def prepareWorkspace(
        queries: Int,
        k: Int,
        threads: Int,
        allocator: BufferAllocator
    ): Unit = {
      if (workspace.forall(_.maxQueries < queries)) {
        pipelines.values.foreach(_.close())
        pipelines.clear()
        workspace.foreach(_.close())
        workspace = None
        val rows = sources.collect { case Index(_, handle, _) => handle.rows }
        if (rows.nonEmpty)
          workspace = Some(
            new IndexSearch.Workspace(
              queries,
              math.min(k.toLong, rows.max).toInt,
              rows.max,
              threads,
              callsInFlight + 1,
              allocator
            )
          )
      }
    }

    private def pipelineFor(
        source: Index,
        k: Int,
        parameters: Map[String, String],
        allocator: BufferAllocator,
        queries: Int,
        threads: Int
    ): IndexSearch.Pipeline = pipelines.get(source.segmentId) match {
      case Some(pipeline) if pipeline.maxQueries >= queries => pipeline
      case other =>
        other.foreach(_.close())
        val pipeline = new IndexSearch.Pipeline(
          source.handle,
          source.excluded,
          k,
          parameters,
          allocator,
          queries,
          threads,
          rankNonFinite,
          Some(collector),
          workspace
        )
        pipelines(source.segmentId) = pipeline
        pipeline
    }

    /** Runs one query group over the held set. `threads` is how many shards a
      * segment's answer is checked and collected in at once.
      */
    def search(
        queries: QueryMatrix,
        k: Int,
        metric: MetricType,
        parameters: Map[String, String],
        allocator: BufferAllocator,
        onProgress: Progress => Unit = _ => (),
        threads: Int = 1
    ): (TopKMerger, Counters) = {
      prepareWorkspace(queries.queries, k, threads, allocator)
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
      // Index segments: up to callsInFlight native searches in flight, each
      // collected (on the shared collector, in order) once its search returns,
      // so the collect of segment s runs while segments s+1.. search and the
      // tail of one call is filled by the next. Exact native calls stay on
      // this thread; their merger phases run on the same collector.
      val inFlight =
        mutable.Queue
          .empty[(IndexSearch.Pipeline, IndexSearch.Answer, Future[_])]
      def settle(): Unit = {
        val (pipeline, answer, searched) = inFlight.dequeue()
        SearchThreads.await(searched)
        pipeline.complete(answer, merger, counted)
        onProgress(Progress(0, 0L, 0L, 1))
      }
      try {
        sources.foreach { source =>
          source match {
            case Exact(id, _) =>
              while (inFlight.nonEmpty) settle()
              batches(id).foreach(batch =>
                ExactScan.batch(
                  batch,
                  queries,
                  id,
                  k,
                  metric,
                  allocator,
                  merger,
                  counted,
                  ranked.get(batch),
                  Some(collector),
                  threads
                )
              )
              onProgress(Progress(0, 0L, 0L, 1))
            case index @ Index(id, handle, _) =>
              require(
                handle.metric == metric,
                s"Segment $id has a $metric query on a ${handle.metric} index"
              )
              val pipeline = pipelineFor(
                index,
                k,
                parameters,
                allocator,
                queries.queries,
                threads
              )
              val answer = pipeline.begin(queries)
              val searched =
                searchers.submit[Unit](() => pipeline.search(answer))
              inFlight.enqueue((pipeline, answer, searched))
              if (inFlight.size >= callsInFlight) settle()
          }
        }
        while (inFlight.nonEmpty) settle()
        // Every collect of the group is done before the merger returns to the
        // task, including a failed collect whose buffer users must be drained.
        pipelines.values.foreach(_.finish(counted))
      } catch {
        case failure: Throwable =>
          def completed(body: => Unit): Unit = {
            try body
            catch {
              case later: Throwable if later ne failure =>
                failure.addSuppressed(later)
            }
          }
          // The caller releases the query matrix as soon as this throws.
          // Cancelled futures and Java interrupts do not stop native calls.
          completed(stop())
          pipelines.values.foreach(pipeline =>
            completed(pipeline.finish(_ => ()))
          )
          throw failure
      }
      (merger, Counters(sources.size, nativeCalls, nativeNanos, compared))
    }

    private def stop(): Unit = {
      try SearchThreads.shutdown(searchers)
      finally SearchThreads.shutdown(collector)
    }

    override def close(): Unit = {
      var failure: Throwable = null
      def completed(body: => Unit): Unit = {
        try body
        catch {
          case cause: Throwable =>
            if (failure == null) failure = cause
            else if (failure ne cause) failure.addSuppressed(cause)
        }
      }
      completed(stop())
      (pipelines.values ++ workspace ++ batches.values.flatten ++ sources)
        .foreach(closeable => completed(closeable.close()))
      pipelines.clear()
      if (failure != null) throw failure
    }
  }

  /** A set that did not fit: the segment whose batch went over, the bytes
    * retained by then, and the budget. Everything read so far is closed; the
    * caller streams the set instead, one batch at a time for every query group.
    */
  final case class Overflow(
      segmentId: Long,
      retainedBytes: Long,
      budgetBytes: Long,
      read: VectorBatch.ReadCost
  )

  /** Reads the segment set into memory: every batch of an exact scan, every
    * index handle of an indexed search. The planner sized the set to fit
    * `keptMaxBytes` from the sizes it had; this measures what was actually kept
    * -- the batches' buffers, the indexes' bytes -- and a set that goes over
    * comes back as an [[Overflow]] with nothing left open, rather than a
    * failure. With a ranking function each batch is classified as it is read
    * (see [[runGroups]]).
    */
  def hold(
      segments: Seq[Long],
      open: Long => Source,
      keptMaxBytes: Long,
      layout: VectorLayout,
      metric: MetricType,
      function: Option[RankingFunction] = None,
      callsInFlight: Int = 1
  ): Either[Overflow, Held] = {
    require(segments != null, "A task must name its segments")
    val sources = Seq.newBuilder[Source]
    val batches = mutable.Map.empty[Long, mutable.ArrayBuffer[VectorBatch]]
    val ranked = Map.newBuilder[VectorBatch, EngineRange.Batch]
    var retained = 0L
    var read = VectorBatch.ReadCost.Zero
    def opened = new Held(
      sources.result(),
      batches.iterator.map { case (id, held) => id -> held.toVector }.toMap,
      read,
      ranked.result(),
      function.nonEmpty,
      callsInFlight
    )
    try {
      val remaining = segments.iterator
      while (remaining.hasNext) {
        val segmentId = remaining.next()
        val source = open(segmentId)
        sources += source
        source match {
          case Exact(id, vectors) =>
            val held = mutable.ArrayBuffer.empty[VectorBatch]
            // A returned batch belongs to this task immediately, including
            // when sizing, classification or the following read fails.
            batches(id) = held
            var next = vectors.next()
            while (next.nonEmpty) {
              val batch = next.get
              held += batch
              retained += batch.base.buffer.capacity().toLong
              function.foreach(f =>
                ranked += batch -> EngineRange.Batch
                  .of(batch, layout, metric, f)
              )
              if (retained > keptMaxBytes) {
                read = read + vectors.readCost
                opened.close()
                logInfo(
                  s"Segment set not held: segment $id brings the vectors read to $retained bytes, " +
                    s"over the $keptMaxBytes a task keeps; the set streams instead, once per query group"
                )
                return Left(Overflow(id, retained, keptMaxBytes, read))
              }
              next = vectors.next()
            }
            read = read + vectors.readCost
          case Index(id, handle, _) =>
            retained += handle.bytes
            if (retained > keptMaxBytes) {
              opened.close()
              logInfo(
                s"Segment set not held: segment $id brings the indexes loaded to $retained bytes, " +
                  s"over the $keptMaxBytes a task keeps; the set streams instead, once per query group"
              )
              return Left(Overflow(id, retained, keptMaxBytes, read))
            }
        }
      }
      val result = opened
      logInfo(
        s"Segment set held: segments=${segments.size}, retainedBytes=${result.retainedBytes}, " +
          s"rowBytes=${layout.rowBytes}, callsInFlight=$callsInFlight"
      )
      Right(result)
    } catch {
      case failure: Throwable =>
        try opened.close()
        catch { case closing: Throwable => failure.addSuppressed(closing) }
        throw failure
    }
  }

  /** Counts what one task did, for the accumulators section 1.3 lists. `read`
    * is what the exact scan pulled through the reader; an indexed search reads
    * its files when the handle opens, which the handle itself reports.
    */
  final case class Counters(
      segments: Int,
      nativeCalls: Int,
      nativeNanos: Long,
      compared: Long = 0L,
      read: VectorBatch.ReadCost = VectorBatch.ReadCost.Zero
  )

  /** One finished step of a search, handed to the caller as it happens.
    * `Counters` is the same quantities summed, and a caller that only wants the
    * total can ignore this. A caller that has somewhere to publish them needs
    * them before the task ends: a search of one query group against one segment
    * set runs for as long as the vectors take, and a total reported at the end
    * says nothing while it runs.
    *
    * `compared` is the pairs a step measured distances for, queries times the
    * rows that survived the exclusion mask. An indexed search does not compare
    * every row and Knowhere does not say how many it did, so it reports zero
    * and counts its progress in `nativeCalls`. `rescans` is the queries an L2
    * exact scan searched a batch for again through the per-query entry, because
    * the batched entry's error could hide a nearer row; their pairs are not
    * counted again in `compared`.
    */
  final case class Progress(
      nativeCalls: Int,
      nativeNanos: Long,
      compared: Long,
      segments: Int,
      rescans: Long = 0L
  )

  /** Searches every segment of the set in turn for one query group: what
    * [[runGroups]] does for a single group.
    */
  def run(
      segments: Seq[Long],
      open: Long => Source,
      queries: QueryMatrix,
      k: Int,
      metric: MetricType,
      parameters: Map[String, String],
      allocator: BufferAllocator,
      onProgress: Progress => Unit = _ => (),
      function: Option[RankingFunction] = None
  ): (TopKMerger, Counters) = {
    val (mergers, counters) = runGroups(
      segments,
      open,
      Seq(queries),
      k,
      metric,
      parameters,
      allocator,
      onProgress,
      function = function
    )
    (mergers.head, counters)
  }

  /** Searches every segment of the set once for every query group. Sources are
    * opened one at a time by `open`, and each is closed before the next one
    * opens, so a task holds one segment's data at once while its query groups
    * stay: an exact scan hands every batch to every group before it reads the
    * next batch; indexed search uses the loaded index for every group. Each
    * group has its own merger, returned in the order the groups were given.
    *
    * A search ranked by `function` classifies each exact-scan batch once, for
    * all its groups ([[EngineRange.Batch]]), and ranks an index's NaN or
    * infinite scores rather than failing on them.
    */
  def runGroups(
      segments: Seq[Long],
      open: Long => Source,
      groups: Seq[QueryMatrix],
      k: Int,
      metric: MetricType,
      parameters: Map[String, String],
      allocator: BufferAllocator,
      onProgress: Progress => Unit = _ => (),
      prefetch: Boolean = false,
      threads: Int = 1,
      function: Option[RankingFunction] = None
  ): (Seq[TopKMerger], Counters) = {
    require(segments != null, "A task must name its segments")
    require(groups.nonEmpty, "A task searches at least one query group")
    val mergers =
      groups.map(queries => new TopKMerger(queries.queries, k, metric))
    val largestGroup = groups.iterator.map(_.queries).max
    var nativeCalls = 0
    var nativeNanos = 0L
    var compared = 0L
    def counted(step: Progress): Unit = {
      nativeCalls += step.nativeCalls
      nativeNanos += step.nativeNanos
      compared += step.compared
      onProgress(step)
    }
    var read = VectorBatch.ReadCost.Zero
    val sources = new Prefetcher[Source](segments, open, prefetch)
    try
      sources.foreach { source =>
        try
          source match {
            case Exact(id, vectors) =>
              var next = vectors.next()
              while (next.nonEmpty) {
                val batch = next.get
                try {
                  val ranked = function.map(
                    EngineRange.Batch.of(batch, groups.head.layout, metric, _)
                  )
                  groups.indices.foreach { group =>
                    ExactScan.batch(
                      batch,
                      groups(group),
                      id,
                      k,
                      metric,
                      allocator,
                      mergers(group),
                      counted,
                      ranked,
                      threads = threads
                    )
                  }
                } finally batch.close()
                next = vectors.next()
              }
              read = read + vectors.readCost
            case Index(id, handle, excluded) =>
              require(
                handle.metric == metric,
                s"Segment $id has a $metric query on a ${handle.metric} index"
              )
              // Every group through one pipeline: the index searches group g+1
              // while a worker checks and collects group g.
              val pipeline = new IndexSearch.Pipeline(
                handle,
                excluded,
                k,
                parameters,
                allocator,
                largestGroup,
                threads,
                rankNonFinite = function.nonEmpty
              )
              try {
                groups.indices.foreach { group =>
                  pipeline.run(groups(group), mergers(group), counted)
                }
                pipeline.finish(counted)
              } finally pipeline.close()
          }
        finally source.close()
        onProgress(Progress(0, 0L, 0L, groups.size))
      }
    finally sources.close()
    logInfo(
      s"Segment set searched: segments=${segments.size}, groups=${groups.size}, " +
        s"queries=${groups.map(_.queries).sum}, topK=$k, metric=$metric, " +
        s"candidates=${mergers.map(_.size).sum}, nativeSearchCalls=$nativeCalls, " +
        s"nativeMillis=${nativeNanos / 1000000L}"
    )
    (mergers, Counters(segments.size, nativeCalls, nativeNanos, compared, read))
  }

  /** The sources of a task's segments, opened one ahead when asked. With
    * `prefetch`, the source after the one being searched is opened on a thread
    * of its own while the search runs: for an index that is the read of its
    * files and Knowhere's deserialization, 0.6 s a segment on P3 that used to
    * sit between one segment's last group and the next segment's first. The
    * task holds at most two sources at once, which is what the caller checks
    * against its budget before asking for it. A failure to open surfaces where
    * the source would have been used; a source opened ahead but never used is
    * closed by [[close]].
    */
  private[vector] final class Prefetcher[A <: AutoCloseable](
      ids: Seq[Long],
      open: Long => A,
      prefetch: Boolean
  ) extends AutoCloseable {
    private val opener =
      if (prefetch && ids.size > 1)
        Some(Executors.newSingleThreadExecutor(new ThreadFactory {
          def newThread(runnable: Runnable): Thread = {
            val thread = new Thread(runnable, "segment-source-prefetch")
            thread.setDaemon(true)
            thread
          }
        }))
      else None
    private var ahead: Option[Future[A]] = None

    private def take(future: Future[A]): A =
      SearchThreads.take(future)(_.close())

    /** Runs `body` on every source in order; each is closed by the caller. */
    def foreach(body: A => Unit): Unit = {
      var index = 0
      while (index < ids.size) {
        val source = ahead match {
          case Some(future) =>
            try take(future)
            finally ahead = None
          case None => open(ids(index))
        }
        if (index + 1 < ids.size) {
          val nextId = ids(index + 1)
          ahead = opener.map(_.submit[A](() => open(nextId)))
        }
        body(source)
        index += 1
      }
    }

    override def close(): Unit = {
      try ahead.foreach(future => take(future).close())
      finally {
        ahead = None
        opener.foreach(SearchThreads.shutdown)
      }
    }
  }
}
