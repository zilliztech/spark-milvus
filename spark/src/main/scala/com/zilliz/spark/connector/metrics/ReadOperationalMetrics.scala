package com.zilliz.spark.connector.metrics

import java.io.IOException
import java.util.IdentityHashMap

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}

/** Stable failure phases describe the boundary that failed, without parsing
  * exception messages or putting object paths into metric names.
  */
object ReadFailurePhase {
  val None = 0L
  val Open = 1L
  val DeleteLoad = 2L
  val Read = 3L
  val Filter = 4L
  val Convert = 5L
  val Close = 6L

  def name(value: Long): String = value match {
    case Open       => "open"
    case DeleteLoad => "delete_load"
    case Read       => "read"
    case Filter     => "filter"
    case Convert    => "convert"
    case Close      => "close"
    case _          => "none"
  }
}

object ReadEndReason {
  val Exhausted = "exhausted"
  val Limit = "limit"
  val ConsumerStop = "consumer_stop"
  val Cancelled = "cancelled"
  val Failed = "failed"
  val values: Seq[String] =
    Seq(Exhausted, Limit, ConsumerStop, Cancelled, Failed)
}

final class ReadPlannedSegmentsMetric
    extends SumMetric(
      ReadOperationalMetrics.PlannedSegments,
      "segments in the current read plan"
    )
final class ReadDecodedRowsMetric
    extends SumMetric(
      ReadOperationalMetrics.DecodedRows,
      "physical rows delivered by native batches, before filtering"
    )
final class ReadEmittedRowsMetric
    extends SumMetric(
      ReadOperationalMetrics.EmittedRows,
      "rows handed to Spark after deletes, predicates and partition limit"
    )
final class ReadDeletedRowsMetric
    extends SumMetric(
      ReadOperationalMetrics.DeletedRows,
      "evaluated rows excluded by deletes"
    )
final class ReadFilteredRowsMetric
    extends SumMetric(
      ReadOperationalMetrics.FilteredRows,
      "evaluated nondeleted rows excluded by pushed predicates"
    )
final class ReadCompletedSegmentsMetric
    extends SumMetric(
      ReadOperationalMetrics.CompletedSegments,
      "read partitions consumed successfully, including early stops"
    )
final class ReadExhaustedMetric
    extends SumMetric(
      ReadOperationalMetrics.endName(ReadEndReason.Exhausted),
      "readers that reached physical EOF"
    )
final class ReadLimitedMetric
    extends SumMetric(
      ReadOperationalMetrics.endName(ReadEndReason.Limit),
      "readers stopped by the pushed partition limit"
    )
final class ReadConsumerStopMetric
    extends SumMetric(
      ReadOperationalMetrics.endName(ReadEndReason.ConsumerStop),
      "readers closed before physical EOF by their consumer"
    )
final class ReadCancelledMetric
    extends SumMetric(
      ReadOperationalMetrics.endName(ReadEndReason.Cancelled),
      "readers cancelled"
    )
final class ReadFailedMetric
    extends SumMetric(
      ReadOperationalMetrics.endName(ReadEndReason.Failed),
      "readers that failed"
    )
final class ReadCleanupFailuresMetric
    extends SumMetric(
      ReadOperationalMetrics.CleanupFailures,
      "read resource cleanup failures"
    )

final class ReadFailurePhaseMetric extends CustomMetric {
  override def name(): String = ReadOperationalMetrics.FailurePhase
  override def description(): String =
    "first failure phase of reported read attempts"
  override def aggregateTaskMetrics(values: Array[Long]): String = {
    val failures = values.filter(_ != ReadFailurePhase.None)
    if (failures.isEmpty) "none"
    else
      failures
        .groupBy(identity)
        .toSeq
        .sortBy(_._1)
        .map { case (phase, occurrences) =>
          s"${ReadFailurePhase.name(phase)}: ${occurrences.length}"
        }
        .mkString(", ")
  }
}

/** One reader's counters are shared with its outer limit and output wrappers.
  * Decode and filtering belong to the physical reader; only the outer wrapper
  * counts rows handed to Spark. Successful early stops count as consumed
  * partitions, while their end reason remains distinct from physical EOF.
  *
  * SQL gets a current snapshot, including a provisional EOF/limit. The bridge
  * receives the terminal snapshot only after resource cleanup, so a close
  * failure cannot first be published as a successfully consumed partition.
  */
final class ReadProgress(
    observation: Option[TaskObservation] = None,
    isCancelled: () => Boolean = () =>
      Option(TaskContext.get()).exists(_.isInterrupted())
) {
  private var decoded = 0L
  private var emitted = 0L
  private var deleted = 0L
  private var filtered = 0L
  private var endReason: Option[String] = None
  private var failurePhase = ReadFailurePhase.None
  private var failureCategory = "none"
  private val cleanupErrors =
    new IdentityHashMap[Throwable, java.lang.Boolean]()
  private var closed = false

  def decodedRows(count: Long): Unit = synchronized {
    decoded += count
    observation.foreach { task =>
      task.set(ReadOperationalMetrics.DecodedRows, decoded)
      if (count > 0L) task.progress()
    }
  }

  def emittedRows(count: Long): Unit = synchronized {
    emitted += count
    observation.foreach { task =>
      task.set(ReadOperationalMetrics.EmittedRows, emitted)
      if (count > 0L) task.progress()
    }
  }

  def excludedRows(deletedCount: Long, filteredCount: Long): Unit =
    synchronized {
      deleted += deletedCount
      filtered += filteredCount
      observation.foreach { task =>
        task.set(ReadOperationalMetrics.DeletedRows, deleted)
        task.set(ReadOperationalMetrics.FilteredRows, filtered)
        if (deletedCount > 0L || filteredCount > 0L) task.progress()
      }
    }

  def inPhase[T](phase: Long)(body: => T): T =
    try body
    catch {
      case failure: Throwable =>
        failed(phase, failure)
        throw failure
    }

  def failed(phase: Long, failure: Throwable): Unit = synchronized {
    if (phase == ReadFailurePhase.Close)
      cleanupErrors.put(failure, java.lang.Boolean.TRUE)
    if (failurePhase == ReadFailurePhase.None) {
      failurePhase = phase
      failureCategory = failure match {
        case _: IllegalArgumentException      => "validation"
        case _: UnsupportedOperationException => "unsupported"
        case _: IOException                   => "storage"
        case _                                => "other"
      }
      if (failure.isInstanceOf[TaskKilledException] || isCancelled()) {
        endReason = Some(ReadEndReason.Cancelled)
        failureCategory = "cancelled"
      } else endReason = Some(ReadEndReason.Failed)
    }
  }

  def end(reason: String): Unit = synchronized {
    require(
      ReadEndReason.values.contains(reason),
      s"unknown read end reason $reason"
    )
    if (endReason.isEmpty) endReason = Some(reason)
  }

  def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      if (isCancelled() && !endReason.contains(ReadEndReason.Failed))
        endReason = Some(ReadEndReason.Cancelled)
      if (endReason.isEmpty) endReason = Some(ReadEndReason.ConsumerStop)
      observation.foreach { task =>
        values
          .filter { case (name, _) =>
            ReadOperationalMetrics.BridgeCounters.contains(name)
          }
          .foreach { case (name, value) => task.set(name, value) }
        if (
          failurePhase != ReadFailurePhase.None && failurePhase != ReadFailurePhase.Close
        )
          task.failure(ReadFailurePhase.name(failurePhase), failureCategory)
        var cleanup = 0
        while (cleanup < cleanupErrors.size()) {
          task.failure("close", "cleanup")
          cleanup += 1
        }
        task.end(endReason.get)
        task.close()
      }
    }
  }

  def values: Map[String, Long] = synchronized {
    import ReadOperationalMetrics._
    val completed = endReason.exists(reason =>
      reason == ReadEndReason.Exhausted || reason == ReadEndReason.Limit ||
        reason == ReadEndReason.ConsumerStop
    )
    Map(
      DecodedRows -> decoded,
      EmittedRows -> emitted,
      DeletedRows -> deleted,
      FilteredRows -> filtered,
      CompletedSegments -> (if (completed) 1L else 0L),
      FailurePhase -> failurePhase,
      CleanupFailures -> cleanupErrors.size().toLong
    ) ++ ReadEndReason.values.map(reason =>
      endName(reason) -> (if (endReason.contains(reason)) 1L else 0L)
    )
  }

  def taskValues: Array[CustomTaskMetric] =
    values.toSeq
      .sortBy(_._1)
      .map { case (name, value) =>
        new TaskMetric(name, value): CustomTaskMetric
      }
      .toArray
}

object ReadOperationalMetrics {
  val PlannedSegments = "milvus.read.segments.planned"
  val DecodedRows = "milvus.read.rows.decoded"
  val EmittedRows = "milvus.read.rows.emitted"
  val DeletedRows = "milvus.read.rows.deleted"
  val FilteredRows = "milvus.read.rows.filtered"
  val CompletedSegments = "milvus.read.segments.completed"
  val FailurePhase = "milvus.read.failure.phase"
  val CleanupFailures = "milvus.read.cleanup.failures"
  private[metrics] val BridgeCounters = Set(
    DecodedRows,
    EmittedRows,
    DeletedRows,
    FilteredRows,
    CompletedSegments
  )

  def endName(reason: String): String = s"milvus.read.ends.$reason"

  def supported: Array[CustomMetric] = Array(
    new ReadPlannedSegmentsMetric,
    new ReadDecodedRowsMetric,
    new ReadEmittedRowsMetric,
    new ReadDeletedRowsMetric,
    new ReadFilteredRowsMetric,
    new ReadCompletedSegmentsMetric,
    new ReadExhaustedMetric,
    new ReadLimitedMetric,
    new ReadConsumerStopMetric,
    new ReadCancelledMetric,
    new ReadFailedMetric,
    new ReadCleanupFailuresMetric,
    new ReadFailurePhaseMetric
  )

  def planned(segments: Long): Array[CustomTaskMetric] =
    Array(new TaskMetric(PlannedSegments, segments))

  def withTaskValues(
      existing: Array[CustomTaskMetric],
      progress: ReadProgress
  ): Array[CustomTaskMetric] = {
    val current = progress.taskValues
    val names = current.iterator.map(_.name()).toSet
    existing.filterNot(metric => names.contains(metric.name())) ++ current
  }
}
