package com.zilliz.spark.connector.metrics

import java.util.UUID
import scala.collection.mutable

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.{SparkContext, TaskContext}

/** Internal correlation identity; never exported as a metric label. */
final case class OperationToken(
    id: String,
    kind: String,
    executionId: Option[String]
) extends Serializable

/** Optional operational reporting. SQL task metrics do not depend on this
  * bridge being installed. The driver plugin owns its lifetime.
  */
object OperationalMetrics {
  @volatile private[metrics] var driver
      : Option[(SparkContext, OperationalState)] = None
  @volatile private[metrics] var reporter: Option[TaskReporter] = None

  def begin(
      kind: String,
      planned: Map[String, Long] = Map.empty,
      id: String = UUID.randomUUID().toString
  ): OperationToken = {
    require(
      OperationalNames.Kinds.contains(kind),
      s"Unknown operation kind: $kind"
    )
    val token = OperationToken(
      id,
      kind,
      driver.flatMap { case (context, _) =>
        Option(context.getLocalProperty("spark.sql.execution.id"))
      }
    )
    driver.foreach { case (_, state) => state.begin(token, planned) }
    token
  }

  def phase(token: OperationToken, phase: String): Unit =
    driver.foreach { case (_, state) => state.phase(token, phase) }

  def set(token: OperationToken, name: String, value: Long): Unit =
    driver.foreach { case (_, state) => state.set(token, name, value) }

  def failure(token: OperationToken, phase: String, reason: String): Unit =
    driver.foreach { case (_, state) => state.failure(token, phase, reason) }

  def finish(token: OperationToken, outcome: String): Unit =
    driver.foreach { case (_, state) => state.finish(token, outcome) }

  def task(
      token: OperationToken,
      phase: String,
      logicalPartition: Option[String] = None
  ): TaskObservation = {
    val context = TaskContext.get()
    (reporter, Option(context)) match {
      case (Some(transport), Some(task)) =>
        transport.observe(token, task, phase, logicalPartition)
      case _ => new TaskObservation(None, None, phase)
    }
  }
}

private[metrics] object OperationalNames {
  val Kinds = Seq("read", "write")
  val Phases = Seq(
    "planning",
    "claiming",
    "reading",
    "writing",
    "validation",
    "commit",
    "publish",
    "open",
    "delete_load",
    "read",
    "filter",
    "convert",
    "close",
    "other"
  )
  val Reasons = Seq(
    "identity",
    "coverage",
    "manifest_conflict",
    "storage",
    "receipt",
    "cleanup",
    "executor_lost",
    "task_failed",
    "cancelled",
    "validation",
    "unsupported",
    "commit",
    "publication_unconfirmed",
    "task",
    "other"
  )
  val Outcomes = Seq("success", "failed", "cancelled", "unknown")
  val Ends = Seq(
    "exhausted",
    "limit",
    "consumer_stop",
    "cancelled",
    "failed",
    "files_ready",
    "aborted"
  )
  val Work = Set(
    "milvus.read.rows.decoded",
    "milvus.read.rows.emitted",
    "milvus.read.rows.deleted",
    "milvus.read.rows.filtered",
    "milvus.write.rows.accepted",
    "milvus.write.task.failures",
    "milvus.write.task.validation.failures",
    "milvus.write.task.cleanup.failures"
  )
  val DriverGauges = Set(
    "milvus.read.segments.planned",
    "milvus.read.segments.completed",
    "milvus.write.rows.expected",
    "milvus.write.segments.expected",
    "milvus.write.rows.files_ready",
    "milvus.write.files.ready",
    "milvus.write.segments.manifest_committed",
    "milvus.write.rows.manifest_committed",
    "milvus.write.segments.manifest_unknown",
    "milvus.write.receipt.published",
    "milvus.write.segments.not_committed",
    "milvus.write.cleanup.failures"
  )
  val TaskValues = Work + "milvus.read.segments.completed"
  val DriverTotals = DriverGauges.filter(name =>
    name.startsWith("milvus.write.") && !name.endsWith("expected")
  )
  def phase(value: String): String =
    if (Phases.contains(value)) value else "other"
  def reason(value: String): String =
    if (Reasons.contains(value)) value else "other"
  def counterName(name: String): String = s"${name.replace('.', '_')}_total"
  def gaugeName(name: String): String = name.replace('.', '_')
}

private[metrics] final case class TaskIdentity(
    operation: OperationToken,
    taskId: Long,
    stageId: Int,
    partition: Int,
    executor: String,
    observation: String,
    logicalPartition: Option[String] = None
) extends Serializable

private[metrics] final case class TaskSnapshot(
    identity: TaskIdentity,
    sequence: Long,
    values: Map[String, Long],
    failures: Map[(String, String), Long],
    progress: Long,
    end: Option[String]
) extends Serializable

/** Cumulative task-local values. A timer sends snapshots without inventing
  * progress. Multiple readers in one task have distinct observation IDs.
  */
final class TaskObservation private[metrics] (
    transport: Option[TaskReporter],
    identity: Option[TaskIdentity],
    initialPhase: String
) extends AutoCloseable {
  private val values = mutable.Map.empty[String, Long]
  private val failures = mutable.Map.empty[(String, String), Long]
  private var progressVersion = 0L
  private var sequence = 0L
  private var ending: Option[String] = None
  private var closed = false

  def set(name: String, value: Long): Unit = synchronized {
    if (identity.nonEmpty && !closed) {
      require(
        OperationalNames.TaskValues.contains(name),
        s"Unknown task metric: $name"
      )
      require(value >= 0L, s"Negative task metric: $name")
      if (value > values.getOrElse(name, 0L) && !name.endsWith("failures"))
        progressVersion += 1L
      values.update(name, value)
    }
  }

  def progress(): Unit = synchronized { if (!closed) progressVersion += 1L }

  def failure(reason: String): Unit = failure(initialPhase, reason)

  def failure(phase: String, reason: String): Unit = synchronized {
    if (identity.nonEmpty && !closed) {
      val key = OperationalNames.phase(phase) -> OperationalNames.reason(reason)
      failures.update(key, failures.getOrElse(key, 0L) + 1L)
    }
  }

  def end(reason: String): Unit = synchronized {
    if (!closed && ending.isEmpty) {
      require(
        OperationalNames.Ends.contains(reason),
        s"Unknown task end: $reason"
      )
      ending = Some(reason)
      if (
        Set("exhausted", "limit", "consumer_stop", "files_ready").contains(
          reason
        )
      ) progressVersion += 1L
    }
  }

  private[metrics] def snapshot(): Option[TaskSnapshot] = synchronized {
    if (closed) None
    else
      identity.map { id =>
        sequence += 1L
        TaskSnapshot(
          id,
          sequence,
          values.toMap,
          failures.toMap,
          progressVersion,
          ending
        )
      }
  }

  private[metrics] def taskId: Option[Long] = identity.map(_.taskId)

  override def close(): Unit = synchronized {
    if (!closed) {
      transport.foreach(_.complete(this))
      closed = true
    }
  }
}

/** Driver aggregation independent of RPC and Spark listeners, so ordering,
  * retries, and operation retirement can be tested without a Spark context.
  */
private[metrics] final class OperationalState(
    val registry: MetricRegistry,
    nowMillis: () => Long = () => System.currentTimeMillis(),
    maxOperations: Int = 1024,
    maxTrackedTasks: Int = 100000
) {
  require(maxOperations > 0 && maxTrackedTasks > 0)
  private final class Active(val token: OperationToken) {
    var phase = "planning"
    var lastProgress = nowMillis()
    var incomplete = false
    var outcome: Option[String] = None
    val gauges = mutable.Map.empty[String, Long]
    val highWater = mutable.Map.empty[String, Long]
    val snapshots = mutable.Map.empty[String, TaskSnapshot]
    val acceptedPartitions = mutable.Set.empty[String]
    val taskResults = mutable.Map.empty[Long, Boolean]
  }
  private val active = mutable.Map.empty[String, Active]

  OperationalNames.Work.foreach(name =>
    registry.counter(OperationalNames.counterName(name))
  )
  OperationalNames.DriverTotals.foreach(name =>
    registry.counter(OperationalNames.counterName(name))
  )
  OperationalNames.DriverGauges.foreach { name =>
    registerGauge(OperationalNames.gaugeName(name)) {
      active.valuesIterator.map(_.gauges.getOrElse(name, 0L)).sum
    }
  }
  registry.counter("milvus_read_segments_completed_total")
  registry.counter("milvus_telemetry_dropped_total")
  registry.counter("milvus_cleanup_failures_total")
  OperationalNames.Kinds.foreach { kind =>
    OperationalNames.Outcomes.foreach(outcome =>
      registry.counter(s"milvus_${kind}_operations_${outcome}_total")
    )
    OperationalNames.Ends.foreach(end =>
      registry.counter(s"milvus_${kind}_ends_${end}_total")
    )
    OperationalNames.Phases.foreach { phase =>
      registerGauge(s"milvus_${kind}_${phase}_active") {
        active.valuesIterator
          .count(op =>
            op.outcome.isEmpty && op.token.kind == kind && op.phase == phase
          )
          .toLong
      }
      registerGauge(
        s"milvus_${kind}_${phase}_oldest_progress_timestamp_seconds"
      ) {
        val times = active.valuesIterator
          .filter(op =>
            op.outcome.isEmpty && op.token.kind == kind && op.phase == phase
          )
          .map(_.lastProgress)
          .toSeq
        if (times.isEmpty) 0L else times.min / 1000L
      }
      OperationalNames.Reasons.foreach { reason =>
        registry.counter(s"milvus_${kind}_failures_${phase}_${reason}_total")
      }
    }
  }
  registerGauge("milvus_telemetry_incomplete_operations") {
    active.valuesIterator.count(_.incomplete).toLong
  }

  private def registerGauge(name: String)(value: => Long): Unit =
    registry.register(
      name,
      new Gauge[Long] {
        override def getValue: Long = OperationalState.this.synchronized(value)
      }
    )

  def begin(token: OperationToken, planned: Map[String, Long]): Unit =
    synchronized {
      if (!active.contains(token.id)) {
        if (active.size >= maxOperations)
          registry.counter("milvus_telemetry_dropped_total").inc()
        else {
          active.update(token.id, new Active(token))
          planned.foreach { case (name, value) => set(token, name, value) }
        }
      }
    }

  def phase(token: OperationToken, value: String): Unit = synchronized {
    active.get(token.id).foreach { operation =>
      val next = OperationalNames.phase(value)
      if (operation.phase != next) {
        operation.phase = next
        operation.lastProgress = nowMillis()
      }
    }
  }

  def set(token: OperationToken, name: String, value: Long): Unit =
    synchronized {
      require(
        OperationalNames.DriverGauges.contains(name),
        s"Unknown driver metric: $name"
      )
      require(value >= 0L, s"Negative driver metric: $name")
      active.get(token.id).foreach { operation =>
        if (
          operation.gauges.get(name) != Some(value) && !name
            .endsWith("failures")
        ) operation.lastProgress = nowMillis()
        operation.gauges.update(name, value)
        if (OperationalNames.DriverTotals.contains(name)) {
          val previous = operation.highWater.getOrElse(name, 0L)
          val delta = math.max(0L, value - previous)
          registry.counter(OperationalNames.counterName(name)).inc(delta)
          if (name == "milvus.write.cleanup.failures")
            addFailure(token.kind, "close", "cleanup", delta)
          operation.highWater.update(name, math.max(previous, value))
        }
      }
    }

  def failure(token: OperationToken, phase: String, reason: String): Unit =
    synchronized {
      active
        .get(token.id)
        .foreach(_ => addFailure(token.kind, phase, reason, 1L))
    }

  private def addFailure(
      kind: String,
      phase: String,
      reason: String,
      count: Long
  ): Unit = {
    val boundedReason = OperationalNames.reason(reason)
    registry
      .counter(
        s"milvus_${kind}_failures_${OperationalNames.phase(phase)}_${boundedReason}_total"
      )
      .inc(count)
    if (boundedReason == "cleanup")
      registry.counter("milvus_cleanup_failures_total").inc(count)
  }

  def receive(snapshot: TaskSnapshot): Unit = synchronized {
    active.get(snapshot.identity.operation.id).foreach { operation =>
      val key = snapshot.identity.observation
      val previous = operation.snapshots.get(key)
      if (previous.isEmpty && operation.snapshots.size >= maxTrackedTasks) {
        operation.incomplete = true
        registry.counter("milvus_telemetry_dropped_total").inc()
      } else if (previous.forall(_.sequence < snapshot.sequence)) {
        snapshot.values.foreach { case (name, value) =>
          if (OperationalNames.Work.contains(name)) {
            val prior = previous.flatMap(_.values.get(name)).getOrElse(0L)
            val delta = math.max(0L, value - prior)
            registry.counter(OperationalNames.counterName(name)).inc(delta)
            if (name == "milvus.write.task.cleanup.failures")
              addFailure(operation.token.kind, "close", "cleanup", delta)
          }
        }
        snapshot.failures.foreach { case ((phase, reason), count) =>
          val prior =
            previous.flatMap(_.failures.get(phase -> reason)).getOrElse(0L)
          addFailure(
            operation.token.kind,
            phase,
            reason,
            math.max(0L, count - prior)
          )
        }
        if (snapshot.progress > previous.map(_.progress).getOrElse(0L))
          operation.lastProgress = nowMillis()
        if (previous.flatMap(_.end).isEmpty) snapshot.end.foreach { end =>
          if (OperationalNames.Ends.contains(end))
            registry
              .counter(s"milvus_${operation.token.kind}_ends_${end}_total")
              .inc()
        }
        operation.snapshots.update(key, snapshot)
        accept(operation, snapshot)
      }
    }
  }

  /** A successful attempt is accepted only after Spark reports TaskEnd Success.
    * Partition identity deduplicates speculative and recomputed stage attempts.
    */
  private def accept(operation: Active, snapshot: TaskSnapshot): Unit = {
    val partition = snapshot.identity.logicalPartition.getOrElse(
      snapshot.identity.partition.toString
    )
    if (
      operation.token.kind == "read" &&
      operation.taskResults.get(snapshot.identity.taskId).contains(true) &&
      snapshot.end.exists(Set("exhausted", "limit", "consumer_stop")) &&
      !operation.acceptedPartitions.contains(partition)
    ) {
      val count =
        snapshot.values.getOrElse("milvus.read.segments.completed", 0L)
      if (count > 0L) {
        operation.acceptedPartitions += partition
        val name = "milvus.read.segments.completed"
        operation.gauges.update(
          name,
          operation.gauges.getOrElse(name, 0L) + count
        )
        registry.counter("milvus_read_segments_completed_total").inc(count)
      }
    }
  }

  def taskEnded(
      executionId: Option[String],
      taskId: Long,
      success: Boolean
  ): Unit = synchronized {
    active.valuesIterator
      .filter(op => executionId.nonEmpty && op.token.executionId == executionId)
      .foreach { operation =>
        if (
          operation.taskResults.contains(
            taskId
          ) || operation.taskResults.size < maxTrackedTasks
        ) {
          operation.taskResults.update(taskId, success)
          operation.snapshots.valuesIterator
            .filter(_.identity.taskId == taskId)
            .foreach(accept(operation, _))
        } else {
          operation.incomplete = true
          registry.counter("milvus_telemetry_dropped_total").inc()
        }
      }
  }

  def executorLost(executor: String): Unit = synchronized {
    active.valuesIterator.foreach { operation =>
      val lost = operation.snapshots.valuesIterator
        .filter { snapshot =>
          snapshot.identity.executor == executor && !operation.taskResults
            .contains(snapshot.identity.taskId)
        }
        .map(_.identity.taskId)
        .toSet
      if (lost.nonEmpty) {
        operation.incomplete = true
        lost.foreach(id => operation.taskResults.update(id, false))
        addFailure(operation.token.kind, operation.phase, "executor_lost", 1L)
      }
    }
  }

  def executionEnded(executionId: String, failed: Boolean): Unit =
    synchronized {
      active.valuesIterator
        .filter(_.token.executionId.contains(executionId))
        .toVector
        .foreach { operation =>
          val outcome =
            if (failed) "failed"
            else if (operation.incomplete || operation.token.kind == "write")
              "unknown"
            else "success"
          finish(operation.token, outcome)
          active.remove(operation.token.id)
        }
    }

  def finish(token: OperationToken, outcome: String): Unit = synchronized {
    require(
      OperationalNames.Outcomes.contains(outcome),
      s"Unknown outcome: $outcome"
    )
    active.get(token.id).foreach { operation =>
      if (operation.outcome.isEmpty) {
        operation.outcome = Some(outcome)
        registry
          .counter(s"milvus_${token.kind}_operations_${outcome}_total")
          .inc()
      }
      if (token.executionId.isEmpty) active.remove(token.id)
    }
  }

  def shutdown(): Unit = synchronized {
    active.valuesIterator.map(_.token).toVector.foreach(finish(_, "unknown"))
    active.clear()
  }

  private[metrics] def activeCount: Int = synchronized(active.size)
  private[metrics] def trackedCount: Int = synchronized(
    active.valuesIterator.map(_.snapshots.size).sum
  )
}
