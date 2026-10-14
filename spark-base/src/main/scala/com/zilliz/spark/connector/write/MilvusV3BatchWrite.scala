package com.zilliz.spark.connector.write

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory}
import java.util.concurrent.TimeUnit
import java.util.UUID
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriterFactory,
  PhysicalWriteInfo,
  Write,
  WriteBuilder,
  WriterCommitMessage
}
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.write.commit.{
  CommitOutcome,
  CommittedSegment,
  Committer,
  JobDescriptor,
  JobOwner,
  JobWriteMode
}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.metrics.WriteMetricsReport
import com.zilliz.spark.connector.options.{
  HadoopStorageKeys,
  MilvusOption,
  OptionParsing
}
import io.milvus.grpc.schema.CollectionSchema

/** The write builder `MilvusTable.newWriteBuilder` returns: `build()` checks
  * the DataFrame's schema against the collection's (`WriteSchema`) and the
  * write goes out as `storage_version = 3` segments, parquet column groups
  * under a manifest written through milvus-storage's `loon_*` writer. V2 and V3
  * in this repository always mean the snapshot's `storage_version`;
  * milvus-storage calls the same manifest format its "format v2".
  *
  * @param dataFrameSchema
  *   what Spark passed in `LogicalWriteInfo`: the DataFrame's own schema
  * @param collection
  *   the collection schema of the snapshot the table was resolved from
  * @param resolveStorage
  *   resolves the native `fs.*` bag on the driver
  *   (`StorageOptions.writeStorageProperties`); every task and the committer
  *   use the result as it is. Called after the schema check, so a DataFrame
  *   that does not fit the collection is reported first.
  */
class MilvusV3WriteBuilder(
    dataFrameSchema: StructType,
    collection: CollectionSchema,
    milvusOption: MilvusOption,
    resolveStorage: () => Map[String, String]
) extends WriteBuilder
    with Logging {

  override def build(): Write = {
    val schema =
      WriteSchema.resolve(dataFrameSchema, collection, WriteSchema.Mode.Append)
    new MilvusV3Write(schema, milvusOption, resolveStorage())
  }
}

/** The V3 write. `schema` is the resolved write schema: every column carries
  * its Milvus type, field id and, for dense vectors, dimension.
  */
class MilvusV3Write(
    schema: StructType,
    milvusOption: MilvusOption,
    storage: Map[String, String]
) extends Write
    with Logging {

  override def toBatch: BatchWrite = {
    new MilvusV3BatchWrite(schema, milvusOption, storage)
  }

  override def supportedCustomMetrics(): Array[CustomMetric] =
    WriteMetricsReport.supported
}

/** The V3 batch write: one job id, one writer factory, and the job-level commit
  * through `core.write.commit.Committer`: the job manifest and the marker under
  * the staging prefix on commit, the prefix's files deleted on abort.
  */
class MilvusV3BatchWrite(
    schema: StructType,
    milvusOption: MilvusOption,
    private[connector] val storage: Map[String, String]
) extends BatchWrite
    with Logging {

  /** One id per write job: every task writes under `StagingLayout(root, jobId)`
    * unless `milvus.writer.customPath` names the segment directory itself
    * (backfill).
    */
  val jobId: String = UUID.randomUUID().toString

  private val layout = StagingLayout(MilvusV3Writer.stagingRoot(storage), jobId)

  private val writeMode: JobWriteMode =
    if (
      OptionParsing
        .value(milvusOption.options, MilvusOption.WriterCustomPath)
        .flatMap(Option(_))
        .exists(_.trim.nonEmpty) ||
      OptionParsing
        .value(milvusOption.options, MilvusOption.WriterCommitType)
        .flatMap(Option(_))
        .exists(_.trim.equalsIgnoreCase("addfield"))
    ) JobWriteMode.Backfill
    else JobWriteMode.Append

  private val descriptor: Option[JobDescriptor] =
    Option(milvusOption.collectionName)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { collection =>
        val database = Option(milvusOption.databaseName)
          .map(_.trim)
          .filter(_.nonEmpty)
          .getOrElse("default")
        JobDescriptor(JobOwner(database, collection), writeMode)
      }

  @transient private var heartbeatExecutor: ScheduledExecutorService = null
  @transient private var ownershipStarted = false

  override def createBatchWriterFactory(
      info: PhysicalWriteInfo
  ): DataWriterFactory = {
    startHeartbeat()
    new MilvusV3WriterFactory(schema, milvusOption, jobId, storage)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    stopHeartbeat()
    val segments = messages.toSeq.map {
      case msg: MilvusV3CommitMessage =>
        CommittedSegment(
          msg.partitionId,
          msg.manifestPath,
          msg.committedVersion,
          msg.recordCount
        )
      case other =>
        throw new IllegalStateException(
          s"unexpected commit message ${other.getClass.getName} in job $jobId"
        )
    }
    withCommitter(_.commit(segments, descriptor = descriptor)) match {
      case CommitOutcome.Committed =>
        logInfo(
          s"Job $jobId committed: ${segments.size} segments, ${segments.map(_.rowCount).sum} rows, manifest ${layout.manifest}"
        )
      case CommitOutcome.AlreadyCommitted =>
        logInfo(s"Job $jobId was already committed; nothing written")
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    stopHeartbeat()
    val deleted = withCommitter(_.abort())
    logWarning(
      s"Job $jobId aborted: ${messages.length} task messages, $deleted files deleted under ${layout.prefix}"
    )
  }

  private def withCommitter[A](f: Committer => A): A = {
    val store = HadoopStorageKeys.storeFrom(storage)
    try f(new Committer(store, layout))
    finally store.close()
  }

  /** Every named job publishes ownership before task writers start. Append jobs
    * refresh driver liveness until Spark calls commit or abort. Backfill writes
    * into existing segments and is never an automatic cleanup candidate.
    */
  private def startHeartbeat(): Unit = synchronized {
    if (descriptor.isEmpty || ownershipStarted) return
    withCommitter(_.start(descriptor.get))
    ownershipStarted = true
    if (writeMode != JobWriteMode.Append) return
    val executor = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactory {
        override def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(runnable, s"milvus-staging-heartbeat-$jobId")
          thread.setDaemon(true)
          thread
        }
      }
    )
    heartbeatExecutor = executor
    try
      executor.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit =
            try withCommitter(_.heartbeat())
            catch {
              case NonFatal(failure) =>
                logError(
                  s"Failed to refresh staging heartbeat for job $jobId",
                  failure
                )
            }
        },
        MilvusV3BatchWrite.HeartbeatSeconds,
        MilvusV3BatchWrite.HeartbeatSeconds,
        TimeUnit.SECONDS
      )
    catch {
      case NonFatal(failure) =>
        heartbeatExecutor = null
        ownershipStarted = false
        executor.shutdownNow()
        throw failure
    }
  }

  private def stopHeartbeat(): Unit = {
    val executor = synchronized {
      val current = heartbeatExecutor
      heartbeatExecutor = null
      current
    }
    if (executor != null) {
      executor.shutdownNow()
      try {
        if (!executor.awaitTermination(10L, TimeUnit.SECONDS)) {
          logWarning(
            s"Staging heartbeat for job $jobId did not stop within 10 seconds; a late heartbeat can only postpone cleanup"
          )
        }
      } catch {
        case interrupted: InterruptedException =>
          Thread.currentThread().interrupt()
          logWarning(
            s"Interrupted while waiting for staging heartbeat of job $jobId to stop; continuing commit or abort",
            interrupted
          )
      }
    }
  }
}

object MilvusV3BatchWrite {
  private val HeartbeatSeconds = 60L
}
