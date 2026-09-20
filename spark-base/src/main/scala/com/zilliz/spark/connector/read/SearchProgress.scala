package com.zilliz.spark.connector.read

import java.util.concurrent.ConcurrentHashMap
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
private[read] final class SearchProgress(comparedTotal: Long)
    extends SparkListener
    with Logging {

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
    val compared = counted.getOrElse(SearchMetrics.Compared, 0L)
    val calls = counted.getOrElse(SearchMetrics.KnowhereCalls, 0L)
    val of =
      if (comparedTotal <= 0L) ""
      else
        f" of ${SearchProgress.brief(comparedTotal)}%s" +
          f" (${compared * 100.0 / comparedTotal}%.1f%%)"
    // Pairs per call is queries in the group times rows in the batch, which is
    // the shape of one distance computation and the only place the batch size
    // shows.
    val each =
      if (calls <= 0L) ""
      else s", ${SearchProgress.brief(compared / calls)} per call over $calls"
    logInfo(
      s"Search progress: compared=${SearchProgress.brief(compared)}$of$each, " +
        s"segments=${counted.getOrElse(SearchMetrics.Segments, 0L)}, " +
        s"knowhereMillis=${counted.getOrElse(SearchMetrics.KnowhereNanos, 0L) / 1000000L}, " +
        s"tasks=${running.size} running, ${ended.size} done"
    )
  }
}

private[read] object SearchProgress {

  /** One line per heartbeat period; a shorter one would repeat the same
    * numbers, because the heartbeat is where they come from.
    */
  private val ReportNanos = 10L * 1000L * 1000L * 1000L

  /** A count at a glance. A search compares pairs in the billions, and a number
    * that long is read digit by digit or not at all; two significant figures
    * are what a reader watching progress needs. Anything under ten thousand is
    * left alone, because there the digits are the answer.
    */
  private[read] def brief(value: Long): String =
    if (value < 10000L) value.toString else f"${value.toDouble}%.2e"

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
