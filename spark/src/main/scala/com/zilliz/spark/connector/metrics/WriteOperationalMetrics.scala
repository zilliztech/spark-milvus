package com.zilliz.spark.connector.metrics

import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}

import com.zilliz.spark.connector.metrics.WriteOperationalMetrics._

/** Driver phases have stable numeric values for SQL metrics. A failed write
  * retains the phase in which its first failure was observed separately.
  */
object WritePhase {
  val Prepared = 0L
  val Claiming = 1L
  val Writing = 2L
  val Validating = 3L
  val CommitPlan = 4L
  val Committing = 5L
  val Publishing = 6L
  val Succeeded = 7L
  val Failed = 8L

  def name(value: Long): String = value match {
    case Prepared   => "prepared"
    case Claiming   => "claiming"
    case Writing    => "writing"
    case Validating => "validating"
    case CommitPlan => "commit_plan"
    case Committing => "committing"
    case Publishing => "publishing"
    case Succeeded  => "succeeded"
    case Failed     => "failed"
    case _          => "unknown"
  }
}

object WriteFailureCategory {
  val None = 0L
  val Validation = 1L
  val Storage = 2L
  val Unsupported = 3L
  val Commit = 4L
  val PublicationUnconfirmed = 5L
  val Task = 6L
  val Other = 7L

  def of(failure: Throwable, phase: Long): Long =
    if (phase == WritePhase.Validating) Validation
    else if (phase == WritePhase.Publishing) PublicationUnconfirmed
    else if (phase == WritePhase.Committing) Commit
    else
      failure match {
        case _: IllegalArgumentException      => Validation
        case _: UnsupportedOperationException => Unsupported
        case _: IOException                   => Storage
        case _                                => Other
      }

  def name(value: Long): String = value match {
    case None                   => "none"
    case Validation             => "validation"
    case Storage                => "storage"
    case Unsupported            => "unsupported"
    case Commit                 => "commit"
    case PublicationUnconfirmed => "publication_unconfirmed"
    case Task                   => "task"
    case _                      => "other"
  }
}

final class WriteExpectedRowsMetric
    extends SumMetric(
      WriteOperationalMetrics.ExpectedRows,
      "stored rows expected, deleted rows included"
    )
final class WriteExpectedSegmentsMetric
    extends SumMetric(
      WriteOperationalMetrics.ExpectedSegments,
      "nonempty segments expected"
    )
final class WriteAcceptedRowsMetric
    extends SumMetric(
      WriteOperationalMetrics.AcceptedRows,
      "rows accepted by reported task attempts, not committed"
    )
final class WriteReadyRowsMetric
    extends SumMetric(
      WriteOperationalMetrics.ReadyRows,
      "rows in complete files from adopted attempts"
    )
final class WriteReadyFilesMetric
    extends SumMetric(
      WriteOperationalMetrics.ReadyFiles,
      "complete files from adopted attempts"
    )
final class WriteCommittedRowsMetric
    extends SumMetric(
      WriteOperationalMetrics.CommittedRows,
      "rows in confirmed committed segment manifests"
    )
final class WriteCommittedSegmentsMetric
    extends SumMetric(
      WriteOperationalMetrics.CommittedSegments,
      "confirmed committed segment manifests"
    )
final class WriteUnknownSegmentsMetric
    extends SumMetric(
      WriteOperationalMetrics.UnknownSegments,
      "segment manifest outcomes that could not be confirmed"
    )
final class WriteNotCommittedSegmentsMetric
    extends SumMetric(
      WriteOperationalMetrics.NotCommittedSegments,
      "segment manifests confirmed not committed or not attempted"
    )
final class WriteReceiptPublishedMetric
    extends SumMetric(
      WriteOperationalMetrics.ReceiptPublished,
      "receipt publication confirmed (0 or 1)"
    )
final class WriteCleanupFailuresMetric
    extends SumMetric(
      WriteOperationalMetrics.CleanupFailures,
      "driver cleanup failures"
    )
final class WriteTaskCleanupFailuresMetric
    extends SumMetric(
      WriteOperationalMetrics.TaskCleanupFailures,
      "reported task attempt cleanup failures"
    )
final class WriteTaskFailuresMetric
    extends SumMetric(
      WriteOperationalMetrics.TaskFailures,
      "reported task attempts that failed"
    )
final class WriteValidationFailuresMetric
    extends SumMetric(
      WriteOperationalMetrics.ValidationFailures,
      "reported task validation failures, not invalid row count"
    )
final class WriteProgressMetric
    extends SumMetric(
      WriteOperationalMetrics.LastProgress,
      "last confirmed driver progress (Unix seconds)"
    )
final class WritePhaseMetric extends CustomMetric {
  override def name(): String = WriteOperationalMetrics.Phase
  override def description(): String = "write phase"
  override def aggregateTaskMetrics(values: Array[Long]): String =
    WritePhase.name(if (values.isEmpty) WritePhase.Prepared else values.max)
}
final class WriteFailurePhaseMetric extends CustomMetric {
  override def name(): String = WriteOperationalMetrics.FailurePhase
  override def description(): String = "phase of first write failure"
  override def aggregateTaskMetrics(values: Array[Long]): String = {
    val phase = if (values.isEmpty) WritePhase.Prepared else values.max
    if (phase == WritePhase.Prepared) "none" else WritePhase.name(phase)
  }
}
final class WriteFailureCategoryMetric extends CustomMetric {
  override def name(): String = WriteOperationalMetrics.FailureCategory
  override def description(): String = "first write failure category"
  override def aggregateTaskMetrics(values: Array[Long]): String =
    WriteFailureCategory.name(
      if (values.isEmpty) WriteFailureCategory.None else values.max
    )
}

/** Only the driver records adopted files and confirmed commits. Task attempt
  * input counters cannot advance either milestone. Repeated outcome callbacks
  * replace one segment's state, never add its rows again.
  */
final class WriteOperationalMetrics(
    expected: Map[Long, Long],
    clockMillis: () => Long = () => System.currentTimeMillis()
) {
  private var phase = WritePhase.Prepared
  private var failurePhase = WritePhase.Prepared
  private var failureCategory = WriteFailureCategory.None
  private var readyRows = 0L
  private var readyFiles = 0L
  private var receiptPublished = false
  private var cleanupFailures = 0L
  private var lastProgress = clockMillis() / 1000L
  private var outcomes = Map.empty[Long, String]

  def enter(next: Long): Unit = synchronized {
    if (
      phase != next && phase != WritePhase.Failed && phase != WritePhase.Succeeded
    ) {
      phase = next
      progress()
    }
  }

  def filesReady(rows: Long, files: Long): Unit = synchronized {
    require(
      rows == expected.values.sum,
      "adopted files must cover every expected row"
    )
    require(files >= 0L, "the number of adopted files cannot be negative")
    if (readyRows != rows || readyFiles != files) {
      readyRows = rows
      readyFiles = files
      progress()
    }
  }

  def segmentOutcome(segmentId: Long, state: String): Unit = synchronized {
    require(expected.contains(segmentId), s"unknown planned segment $segmentId")
    require(
      Set("committed", "not_committed", "unknown").contains(state),
      s"unknown segment outcome $state"
    )
    if (!outcomes.get(segmentId).contains(state)) {
      outcomes += segmentId -> state
      progress()
    }
  }

  def published(): Unit = synchronized {
    require(
      expected.keys.forall(id => outcomes.get(id).contains("committed")),
      "a receipt needs every segment manifest to be confirmed committed"
    )
    receiptPublished = true
    phase = WritePhase.Succeeded
    progress()
  }

  def fail(failure: Throwable): Unit = synchronized {
    failCategory(WriteFailureCategory.of(failure, phase))
  }

  def taskFailed(): Unit = synchronized {
    failCategory(WriteFailureCategory.Task)
  }

  private def failCategory(category: Long): Unit = {
    if (phase != WritePhase.Failed && phase != WritePhase.Succeeded) {
      failurePhase = phase
      failureCategory = category
      phase = WritePhase.Failed
    }
  }

  def cleanupFailed(): Unit = synchronized { cleanupFailures += 1L }

  private def progress(): Unit = { lastProgress = clockMillis() / 1000L }

  def values: Map[String, Long] = synchronized {
    val committed = outcomes.collect { case (id, "committed") => id }.toSeq
    Map(
      ExpectedRows -> expected.values.sum,
      ExpectedSegments -> expected.size.toLong,
      ReadyRows -> readyRows,
      ReadyFiles -> readyFiles,
      CommittedRows -> committed.map(expected).sum,
      CommittedSegments -> committed.size.toLong,
      UnknownSegments -> outcomes.values.count(_ == "unknown").toLong,
      NotCommittedSegments -> outcomes.values
        .count(_ == "not_committed")
        .toLong,
      ReceiptPublished -> (if (receiptPublished) 1L else 0L),
      CleanupFailures -> cleanupFailures,
      LastProgress -> lastProgress,
      Phase -> phase,
      FailurePhase -> failurePhase,
      FailureCategory -> failureCategory
    )
  }

  def driverValues: Array[CustomTaskMetric] =
    values.toSeq
      .sortBy(_._1)
      .map { case (name, value) =>
        new TaskMetric(name, value): CustomTaskMetric
      }
      .toArray

  def operationalValues: Map[String, Long] =
    values -- Set(Phase, FailurePhase, FailureCategory, LastProgress)
}

/** Attempt-local input progress is deliberately independent of commit state. */
final class WriteTaskMetrics {
  private val accepted = new AtomicLong()
  private val failures = new AtomicLong()
  private val validationFailures = new AtomicLong()
  private val cleanupFailures = new AtomicLong()

  def rowAccepted(): Unit = { accepted.incrementAndGet(); () }
  def failed(failure: Throwable): Boolean = {
    val first = failures.compareAndSet(0L, 1L)
    if (first && failure.isInstanceOf[IllegalArgumentException]) {
      validationFailures.incrementAndGet()
    }
    first
  }
  def cleanupFailed(): Unit = { cleanupFailures.incrementAndGet(); () }

  def values: Map[String, Long] = Map(
    WriteOperationalMetrics.AcceptedRows -> accepted.get(),
    WriteOperationalMetrics.TaskFailures -> failures.get(),
    WriteOperationalMetrics.ValidationFailures -> validationFailures.get(),
    WriteOperationalMetrics.TaskCleanupFailures -> cleanupFailures.get()
  )

  def taskValues: Array[CustomTaskMetric] =
    values.toSeq
      .sortBy(_._1)
      .map { case (name, value) =>
        new TaskMetric(name, value): CustomTaskMetric
      }
      .toArray
}

object WriteOperationalMetrics {
  val ExpectedRows = "milvus.write.rows.expected"
  val ExpectedSegments = "milvus.write.segments.expected"
  val AcceptedRows = "milvus.write.rows.accepted"
  val ReadyRows = "milvus.write.rows.files_ready"
  val ReadyFiles = "milvus.write.files.ready"
  val CommittedRows = "milvus.write.rows.manifest_committed"
  val CommittedSegments = "milvus.write.segments.manifest_committed"
  val UnknownSegments = "milvus.write.segments.manifest_unknown"
  val NotCommittedSegments = "milvus.write.segments.not_committed"
  val ReceiptPublished = "milvus.write.receipt.published"
  val CleanupFailures = "milvus.write.cleanup.failures"
  val TaskCleanupFailures = "milvus.write.task.cleanup.failures"
  val TaskFailures = "milvus.write.task.failures"
  val ValidationFailures = "milvus.write.task.validation.failures"
  val LastProgress = "milvus.write.last_progress.timestamp_seconds"
  val Phase = "milvus.write.phase"
  val FailurePhase = "milvus.write.failure.phase"
  val FailureCategory = "milvus.write.failure.category"

  def supported: Array[CustomMetric] = Array(
    new WriteExpectedRowsMetric,
    new WriteExpectedSegmentsMetric,
    new WriteAcceptedRowsMetric,
    new WriteReadyRowsMetric,
    new WriteReadyFilesMetric,
    new WriteCommittedRowsMetric,
    new WriteCommittedSegmentsMetric,
    new WriteUnknownSegmentsMetric,
    new WriteNotCommittedSegmentsMetric,
    new WriteReceiptPublishedMetric,
    new WriteCleanupFailuresMetric,
    new WriteTaskCleanupFailuresMetric,
    new WriteTaskFailuresMetric,
    new WriteValidationFailuresMetric,
    new WriteProgressMetric,
    new WritePhaseMetric,
    new WriteFailurePhaseMetric,
    new WriteFailureCategoryMetric
  )
}
