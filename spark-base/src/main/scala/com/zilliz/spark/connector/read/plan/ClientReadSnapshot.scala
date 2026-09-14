package com.zilliz.spark.connector.read.plan

import java.util.concurrent.{ConcurrentHashMap, Executors}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
// Spark does not expose a public SQL-execution-end hook; validate when upgrading Spark.
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.client.api.MilvusClient
import com.zilliz.spark.connector.options.{MilvusOption, StorageOptions}

/** Lifecycle of the snapshot a client-mode read creates on the Milvus service.
  *
  * Naming, the compaction-protection window, registration of the cleanup that
  * drops the snapshot when the Spark SQL execution ends, the retrying drop
  * itself, the executor those drops run on, and the shutdown-hook drain.
  */
object ClientReadSnapshot extends Logging {
  private[read] case class SnapshotCleanupRegistration(
      session: SparkSession,
      executionId: Long
  )

  private val SnapshotCleanupDrainTimeout = 2.seconds
  private val InitialDropRetryDelayMillis = 200L
  private val DropRetryMaxAttempts = 7
  private val DefaultClientSnapshotCompactionProtectionSeconds = 86400L
  private val MaxClientSnapshotCompactionProtectionSeconds =
    7L * 24L * 60L * 60L
  private val MaxGeneratedSnapshotNameLength = 255

  private val CleanupRpcExecutor = Executors.newFixedThreadPool(
    2,
    (r: Runnable) => {
      val thread = new Thread(r, "milvus-snapshot-cleanup-rpc")
      thread.setDaemon(true)
      thread
    }
  )

  private implicit val CleanupExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(CleanupRpcExecutor)

  private val PendingCleanupFutures =
    ConcurrentHashMap.newKeySet[Future[Unit]]()
  private val CleanupDraining = new AtomicBoolean(false)
  private val CleanupSubmissionLock = new Object

  private[read] def drainPendingCleanupFutures(
      timeout: FiniteDuration = SnapshotCleanupDrainTimeout
  ): Unit = {
    CleanupSubmissionLock.synchronized {
      CleanupDraining.set(true)
    }
    val deadline = timeout.fromNow
    var keepDraining = true
    while (keepDraining && deadline.timeLeft.length > 0) {
      val snapshot = PendingCleanupFutures.asScala.toSeq
      if (snapshot.isEmpty) {
        keepDraining = false
      } else {
        snapshot.foreach { future =>
          val remaining = deadline.timeLeft
          if (remaining.length > 0) {
            try Await.ready(future, remaining)
            catch {
              case NonFatal(e) =>
                logWarning(
                  "Timed out waiting for client snapshot cleanup",
                  e
                )
            }
          }
        }
      }
    }
  }

  Try {
    val drain: Runnable = () => drainPendingCleanupFutures()
    Runtime.getRuntime.addShutdownHook(
      new Thread(drain, "milvus-snapshot-cleanup-shutdown-drain")
    )
  }.failed.foreach { e =>
    logWarning("Failed to register client snapshot cleanup shutdown hook", e)
  }

  private[read] def generatedClientSnapshotName(
      collectionName: String,
      currentTimeMillis: Long = System.currentTimeMillis(),
      uuid: String = UUID.randomUUID().toString.replace("-", "")
  ): String = {
    val suffix = s"_${currentTimeMillis}_$uuid"
    val prefix = "spark_read_"
    val maxCollectionNameLength =
      MaxGeneratedSnapshotNameLength - prefix.length - suffix.length
    val sanitizedCollectionName =
      collectionName.replaceAll("[^A-Za-z0-9_]", "_")
    val safeCollectionName = sanitizedCollectionName.take(
      maxCollectionNameLength.max(0)
    )
    s"$prefix$safeCollectionName$suffix"
  }

  private[read] def parseClientSnapshotCompactionProtectionSeconds(
      options: CaseInsensitiveStringMap
  ): Long = {
    val value = StorageOptions.parsePositiveLongOption(
      options,
      MilvusOption.ClientSnapshotCompactionProtectionSeconds,
      DefaultClientSnapshotCompactionProtectionSeconds
    )
    if (value > MaxClientSnapshotCompactionProtectionSeconds) {
      throw new IllegalArgumentException(
        s"Option '${MilvusOption.ClientSnapshotCompactionProtectionSeconds}' must be <= " +
          s"$MaxClientSnapshotCompactionProtectionSeconds seconds, got $value"
      )
    }
    if (value > DefaultClientSnapshotCompactionProtectionSeconds) {
      logWarning(
        s"Client snapshot compaction protection is set to $value seconds; " +
          "long protection windows can delay Milvus compaction."
      )
    }
    value
  }

  private[read] def activeCleanupRegistration()
      : Option[SnapshotCleanupRegistration] = {
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession) match {
      case Some(session) =>
        Option(
          session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        ).flatMap { raw =>
          try {
            val executionId = raw.trim.toLong
            Some(SnapshotCleanupRegistration(session, executionId))
          } catch {
            case _: NumberFormatException =>
              logWarning(
                s"Ignoring non-numeric ${SQLExecution.EXECUTION_ID_KEY}: $raw"
              )
              None
          }
        }
      case None => None
    }
  }

  private[read] def dropClientReadSnapshot(
      client: MilvusClient,
      databaseName: String,
      collectionName: String,
      snapshotName: String,
      reason: String,
      maxAttempts: Int = DropRetryMaxAttempts
  ): Try[Unit] = {
    var lastFailure = Option.empty[Throwable]
    (1 to maxAttempts).foreach { attempt =>
      client.dropSnapshot(databaseName, collectionName, snapshotName) match {
        case Success(_) =>
          logInfo(s"Dropped client read snapshot $snapshotName after $reason")
          return Success(())
        case Failure(e) if MilvusClient.isSnapshotAlreadyDropped(e) =>
          logInfo(
            s"Client read snapshot $snapshotName was already dropped after $reason"
          )
          return Success(())
        case Failure(e) if MilvusClient.isTerminalSnapshotDropError(e) =>
          logWarning(
            s"Not retrying terminal failure while dropping client read snapshot $snapshotName after $reason",
            e
          )
          return Failure(e)
        case Failure(e) =>
          lastFailure = Some(e)
          logWarning(
            s"Failed to drop client read snapshot $snapshotName after $reason " +
              s"(attempt $attempt/$maxAttempts)",
            e
          )
          if (attempt < maxAttempts) {
            val delayMillis = InitialDropRetryDelayMillis << (attempt - 1)
            Thread.sleep(delayMillis)
          }
      }
    }
    Failure(
      lastFailure.getOrElse(
        new RuntimeException(
          s"Failed to drop client read snapshot $snapshotName after $reason"
        )
      )
    )
  }

  private[read] def preserveResultWhenCloseFails(
      result: Try[Unit],
      close: => Unit,
      closeDescription: String
  ): Try[Unit] = {
    Try(close).failed.foreach { e =>
      logWarning(s"Failed to close $closeDescription", e)
    }
    result
  }

  private def dropClientReadSnapshotWithNewClient(
      baseOptions: Map[String, String],
      databaseName: String,
      collectionName: String,
      snapshotName: String,
      reason: String
  ): Try[Unit] = {
    Try(MilvusClient(MilvusOption(baseOptions).connectionParams)).flatMap {
      client =>
        val dropResult = dropClientReadSnapshot(
          client,
          databaseName,
          collectionName,
          snapshotName,
          reason
        )
        preserveResultWhenCloseFails(
          dropResult,
          client.close(),
          s"Milvus client after dropping client read snapshot $snapshotName"
        )
    }
  }

  private[read] def submitClientSnapshotCleanup(
      baseOptions: Map[String, String],
      databaseName: String,
      collectionName: String,
      snapshotName: String,
      reason: String
  ): Unit = {
    CleanupSubmissionLock.synchronized {
      if (CleanupDraining.get()) {
        logWarning(
          s"Skipping client read snapshot cleanup submission for $snapshotName after $reason because shutdown drain has started"
        )
        return
      }
      val cleanupFuture = Future {
        dropClientReadSnapshotWithNewClient(
          baseOptions,
          databaseName,
          collectionName,
          snapshotName,
          reason
        ) match {
          case Success(_) =>
          case Failure(e) =>
            logError(
              s"Failed to drop client read snapshot $snapshotName after $reason",
              e
            )
        }
      }
      PendingCleanupFutures.add(cleanupFuture)
      cleanupFuture.onComplete(_ => PendingCleanupFutures.remove(cleanupFuture))
    }
  }

  private[read] def registerClientSnapshotCleanup(
      registration: SnapshotCleanupRegistration,
      baseOptions: Map[String, String],
      databaseName: String,
      collectionName: String,
      snapshotName: String,
      autoCleanup: Boolean = true
  ): Boolean = {
    val cleanupTriggered = new AtomicBoolean(false)
    val session = registration.session
    val executionId = registration.executionId

    if (!autoCleanup) {
      logWarning(
        s"Client read snapshot $snapshotName will be preserved after Spark SQL execution ends because ${MilvusOption.ClientSnapshotAutoCleanup}=false"
      )
      true
    } else {
      def submitCleanup(reason: String): Unit = {
        submitClientSnapshotCleanup(
          baseOptions,
          databaseName,
          collectionName,
          snapshotName,
          reason
        )
      }

      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: SparkListenerSQLExecutionEnd
                if e.executionId == executionId && cleanupTriggered
                  .compareAndSet(false, true) =>
              try submitCleanup(s"Spark SQL execution $executionId ended")
              finally session.sparkContext.removeSparkListener(this)
            case _ =>
          }
        }
      }

      Try(session.sparkContext.addSparkListener(listener)) match {
        case Success(_) => true
        case Failure(e) =>
          logError(
            s"Failed to register cleanup listener for client read snapshot $snapshotName",
            e
          )
          false
      }
    }
  }
}
