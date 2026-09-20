package com.zilliz.spark.connector.read

import java.util.concurrent.ConcurrentHashMap
import java.util.Locale
import scala.jdk.CollectionConverters._

import org.apache.spark.{Success => TaskSucceeded}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerExecutorMetricsUpdate,
  SparkListenerTaskEnd
}

import com.zilliz.spark.connector.metrics.SearchMetrics

/** Writes what a running search has counted to the driver log.
  *
  * A first-stage task runs for as long as its vectors take, and until it ends
  * Spark shows nothing of what it has done: executors put every one of a
  * running task's accumulators on the heartbeat, but `AppStatusListener` keeps
  * only the names under `internal.metrics.`, so the stage page and the REST API
  * hold no `milvus.search.*` value until the task is over. A listener of our
  * own reads the same heartbeat and reports it.
  *
  * The heartbeat carries each task's value so far, not what changed since the
  * last one, so a task's report replaces its previous one rather than adding to
  * it. It also carries only the tasks still running: a task that ends
  * disappears from the next heartbeat, so its final value is taken from the
  * task-end event and kept. A task that failed contributes nothing, which is
  * what Spark does with its accumulators.
  *
  * @param comparedTotal
  *   the pairs the whole search has to measure, from the plan, or zero when the
  *   mode does not count them. It is the denominator of
  *   `milvus.search.compared`.
  */
private[read] final class SearchProgress(
    comparedPairsTotal: Long,
    segmentSearchesTotal: Long
) extends SparkListener
    with Logging {

  /** Said once, so a reader meeting these lines knows what wrote them and why
    * they are here rather than on a Spark page.
    */
  def announce(): Unit = logInfo(
    "Search progress is written here while the search runs, because Spark's " +
      "stage page carries a task's own counters only once that task ends. " +
      "A line is one reading of every task's counters, taken from the " +
      "executor heartbeat: distance pairs are query vector against base " +
      "vector, a segment search is one query group over one segment, and an " +
      "engine call is one call into Knowhere over one batch of one segment."
  )

  private val running = new ConcurrentHashMap[Long, Map[String, Long]]()
  private val ended = new ConcurrentHashMap[Long, Map[String, Long]]()
  @volatile private var lastReport = 0L

  override def onExecutorMetricsUpdate(
      update: SparkListenerExecutorMetricsUpdate
  ): Unit = {
    var sawSearch = false
    update.accumUpdates.foreach { case (taskId, _, _, infos) =>
      val counted =
        SearchProgress.searchValues(infos.map(info => (info.name, info.update)))
      if (counted.nonEmpty) {
        running.put(taskId, counted)
        sawSearch = true
      }
    }
    if (sawSearch) report()
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val taskId = event.taskInfo.taskId
    running.remove(taskId)
    if (event.reason == TaskSucceeded) {
      val counted = SearchProgress.searchValues(
        event.taskInfo.accumulables.toSeq.map(info => (info.name, info.update))
      )
      if (counted.nonEmpty) ended.put(taskId, counted)
    }
  }

  /** What every task has counted, the ones still running included. */
  def totals: Map[String, Long] =
    (running.values.asScala.toSeq ++ ended.values.asScala.toSeq).flatten
      .groupBy(_._1)
      .map { case (name, values) => name -> values.map(_._2).sum }

  private def report(): Unit = {
    // The listener runs on the listener bus, so it does no work beyond adding
    // up a few numbers; a slow listener holds up every other one.
    val now = System.nanoTime()
    if (now - lastReport < SearchProgress.ReportNanos) return
    lastReport = now
    val counted = totals
    if (counted.isEmpty) return
    val pairs = counted.getOrElse(SearchMetrics.ComparedPairs, 0L)
    val searches = counted.getOrElse(SearchMetrics.SegmentSearches, 0L)
    val calls = counted.getOrElse(SearchMetrics.KnowhereCalls, 0L)
    val done =
      if (comparedPairsTotal > 0L) pairs
      else if (segmentSearchesTotal > 0L) searches
      else 0L
    val total =
      if (comparedPairsTotal > 0L) comparedPairsTotal else segmentSearchesTotal
    val headline =
      if (total <= 0L) "Search progress"
      else f"Search progress ${done * 100.0 / total}%.1f%%"
    val of = SearchProgress.grouped(_: Long)
    // Pairs per call is the queries of a group times the rows of a batch: the
    // shape of one distance computation, and the only place a batch size that
    // is not the configured one shows.
    val perCall =
      if (calls <= 0L) ""
      else s" at ${of(pairs / calls)} pairs each"
    logInfo(
      s"$headline: ${of(pairs)}" +
        (if (comparedPairsTotal > 0L) s" of ${of(comparedPairsTotal)}"
         else "") +
        " distance pairs, " +
        s"${of(searches)}" +
        (if (segmentSearchesTotal > 0L) s" of ${of(segmentSearchesTotal)}"
         else "") +
        " segment searches, " +
        s"${of(calls)} engine calls$perCall, " +
        s"engine time ${of(counted.getOrElse(SearchMetrics.KnowhereNanos, 0L) / 1000000000L)}s " +
        s"across ${running.size + ended.size} tasks " +
        s"(${running.size} running, ${ended.size} done)"
    )
  }
}

private[read] object SearchProgress {

  /** One line per heartbeat period; a shorter one would repeat the same
    * numbers, because the heartbeat is where they come from.
    */
  private val ReportNanos = 10L * 1000L * 1000L * 1000L

  /** A count at a glance. A search compares pairs in the billions, and eleven
    * digits in a row are counted rather than read; grouped, the size is seen
    * and the value is still exact. The grouping is the root locale's, so a log
    * reads the same wherever it was written.
    */
  private[read] def grouped(value: Long): String =
    String.format(Locale.ROOT, "%,d", java.lang.Long.valueOf(value))

  /** The `milvus.search.*` values of one report, as longs.
    *
    * An accumulable arrives with the name it was registered under and the value
    * as `Any`; anything that is not one of ours, or not a number, is not this
    * listener's business.
    */
  private[read] def searchValues(
      infos: Seq[(Option[String], Option[Any])]
  ): Map[String, Long] =
    infos.flatMap {
      case (Some(name), Some(value)) if name.startsWith("milvus.search.") =>
        value match {
          case number: java.lang.Long    => Some(name -> number.longValue())
          case number: java.lang.Integer => Some(name -> number.longValue())
          case _                         => None
        }
      case _ => None
    }.toMap
}
