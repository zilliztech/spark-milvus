package com.zilliz.spark.connector.metrics

import java.util.{Collections, Map => JavaMap, UUID}
import java.util.concurrent.{
  ConcurrentHashMap,
  Executors,
  ScheduledExecutorService,
  ThreadFactory,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SparkContext, Success, TaskContext}
import org.apache.spark.api.plugin.{
  DriverPlugin,
  ExecutorPlugin,
  PluginContext,
  SparkPlugin
}
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerApplicationEnd,
  SparkListenerEvent,
  SparkListenerExecutorRemoved,
  SparkListenerJobStart,
  SparkListenerTaskEnd
}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import com.zilliz.milvus.storage.Logging

/** Enable with spark.plugins. Driver counters and gauges use Spark's existing
  * metrics sinks; no executor HTTP port or separate HTTP server is required.
  */
final class MilvusMetricsPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new MilvusDriverMetricsPlugin
  override def executorPlugin(): ExecutorPlugin =
    new MilvusExecutorMetricsPlugin
}

private[metrics] final class MilvusDriverMetricsPlugin extends DriverPlugin {
  private var state: OperationalState = _
  private var context: SparkContext = _
  private var listener: SparkListener = _

  override def init(
      sc: SparkContext,
      plugin: PluginContext
  ): JavaMap[String, String] = {
    context = sc
    state = new OperationalState(
      plugin.metricRegistry(),
      maxOperations =
        sc.getConf.getInt("spark.milvus.metrics.maxOperations", 1024),
      maxTrackedTasks =
        sc.getConf.getInt("spark.milvus.metrics.maxTrackedTasks", 100000)
    )
    OperationalMetrics.driver = Some(sc -> state)
    listener = new SparkListener {
      private val executions = mutable.Map.empty[Int, String]

      override def onJobStart(event: SparkListenerJobStart): Unit = {
        Option(event.properties)
          .flatMap(properties =>
            Option(properties.getProperty("spark.sql.execution.id"))
          )
          .foreach(id =>
            event.stageIds.foreach(stage => executions.update(stage, id))
          )
      }

      override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
        state.taskEnded(
          executions.get(event.stageId),
          event.taskInfo.taskId,
          event.reason == Success
        )

      override def onExecutorRemoved(
          event: SparkListenerExecutorRemoved
      ): Unit =
        state.executorLost(event.executorId)

      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case end: SparkListenerSQLExecutionEnd =>
          val id = end.executionId.toString
          // Spark emits Some("") for a successful execution; None is retained
          // for old event logs and is not a failure either.
          state.executionEnded(id, end.errorMessage.exists(_.nonEmpty))
          executions.keysIterator
            .filter(stage => executions(stage) == id)
            .toVector
            .foreach(executions.remove)
        case _ => ()
      }

      override def onApplicationEnd(
          event: SparkListenerApplicationEnd
      ): Unit = {
        state.shutdown()
        executions.clear()
      }
    }
    sc.addSparkListener(listener)
    Collections.emptyMap[String, String]()
  }

  override def receive(message: AnyRef): AnyRef = {
    message match {
      case snapshot: TaskSnapshot => state.receive(snapshot)
      case _ =>
        throw new IllegalArgumentException("Unknown Milvus metrics message")
    }
    java.lang.Boolean.TRUE
  }

  override def shutdown(): Unit = {
    if (state != null) state.shutdown()
    if (context != null && listener != null)
      context.removeSparkListener(listener)
    OperationalMetrics.driver = None
  }
}

private[metrics] final class MilvusExecutorMetricsPlugin
    extends ExecutorPlugin {
  private var transport: TaskReporter = _

  override def init(
      context: PluginContext,
      extraConf: JavaMap[String, String]
  ): Unit = {
    transport = new TaskReporter(context)
    OperationalMetrics.reporter = Some(transport)
  }

  override def shutdown(): Unit = {
    if (transport != null) transport.close()
    OperationalMetrics.reporter = None
  }
}

/** Periodic reporting is independent of task activity, so a stalled native call
  * continues to expose its last actual progress. Final snapshots are
  * acknowledged before the task completes, preventing SQL execution-end from
  * overtaking them. Telemetry transport failures are visible through a counter
  * and rate-limited warnings; they never change a storage operation's outcome.
  */
private[metrics] final class TaskReporter(context: PluginContext)
    extends AutoCloseable
    with Logging {
  private val observations =
    new ConcurrentHashMap[TaskObservation, java.lang.Boolean]()
  private val lastWarning = new AtomicLong(0L)
  private val failures =
    context.metricRegistry().counter("milvus_telemetry_send_failures_total")
  private val maximum =
    context.conf().getInt("spark.milvus.metrics.maxTrackedTasks", 100000)
  private val dropped =
    context.metricRegistry().counter("milvus_telemetry_dropped_total")
  private val intervalMillis =
    context.conf().getLong("spark.milvus.metrics.reportIntervalMillis", 10000L)
  require(intervalMillis > 0L && maximum > 0)

  private val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, "milvus-metrics-reporter")
        thread.setDaemon(true)
        thread
      }
    })
  scheduler.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit =
        observations.keySet().asScala.foreach(report(_, terminal = false))
    },
    intervalMillis,
    intervalMillis,
    TimeUnit.MILLISECONDS
  )

  def observe(
      token: OperationToken,
      task: TaskContext,
      phase: String,
      logicalPartition: Option[String]
  ): TaskObservation = {
    if (observations.size() >= maximum) {
      dropped.inc()
      new TaskObservation(None, None, phase)
    } else {
      val identity = TaskIdentity(
        token,
        task.taskAttemptId(),
        task.stageId(),
        task.partitionId(),
        context.executorID(),
        UUID.randomUUID().toString,
        logicalPartition
      )
      val observation = new TaskObservation(Some(this), Some(identity), phase)
      observations.put(observation, java.lang.Boolean.TRUE)
      task.addTaskCompletionListener[Unit](_ => observation.close())
      observation
    }
  }

  private def report(observation: TaskObservation, terminal: Boolean): Unit =
    observation.snapshot().foreach { snapshot =>
      try {
        if (terminal) context.ask(snapshot) else context.send(snapshot)
      } catch {
        case NonFatal(error) =>
          failures.inc()
          val now = System.currentTimeMillis()
          val previous = lastWarning.get()
          if (
            now - previous >= 60000L && lastWarning.compareAndSet(previous, now)
          ) {
            logWarning(
              "Milvus operational metrics could not reach the driver; monitoring data is incomplete",
              error
            )
          }
      }
    }

  def complete(observation: TaskObservation): Unit = {
    report(observation, terminal = true)
    observations.remove(observation)
  }

  override def close(): Unit = {
    scheduler.shutdownNow()
    observations.clear()
  }
}
