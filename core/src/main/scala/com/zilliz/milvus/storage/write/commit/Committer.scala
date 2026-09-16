package com.zilliz.milvus.storage.write.commit

import java.nio.charset.StandardCharsets

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.write.exec.StagingLayout

sealed trait CommitOutcome

object CommitOutcome {

  /** The manifest and the marker were written by this call. */
  case object Committed extends CommitOutcome

  /** The marker already existed: an earlier run of the same job committed, and
    * nothing was written.
    */
  case object AlreadyCommitted extends CommitOutcome
}

/** The job-level commit of a write: one job manifest, one marker file,
  * idempotent. Runs on the driver after every task has committed its own
  * segment manifest.
  *
  * `commit` writes `manifest.json` under the job's staging prefix, then the
  * marker `_committed` holding the job id. The marker is the signal that the
  * job is complete: a rerun of the same job that finds it does nothing, so the
  * manifest a registration may already have read never changes underneath it.
  * The manifest is written first so the marker never exists without it.
  *
  * `abort` deletes every file under the staging prefix and leaves the directory
  * entries: the loon C API behind `ObjectStore` has no directory delete and its
  * `delete_file` refuses a directory, so on S3 the zero-byte marker objects
  * milvus-storage's writer created (`…/_data/`, `…/_metadata/`) stay behind,
  * and on a local filesystem the empty directories do. A backfill writes into
  * existing segments outside the prefix, and their manifest versions have
  * already advanced by the time the driver aborts; abort does not undo those.
  *
  * Registration is not this class's job (capability A4): it stops at the
  * manifest, and the core layer never calls Milvus.
  */
final class Committer(store: ObjectStore, layout: StagingLayout) {

  def isCommitted: Boolean = store.exists(layout.marker)

  def isRegistered: Boolean = store.exists(layout.registered)

  /** Writes immutable ownership before executor tasks start, then records a
    * liveness heartbeat. Repeating it for the same owner is safe; changing the
    * owner or write mode of an existing job is refused.
    */
  def start(
      descriptor: JobDescriptor,
      nowMillis: Long = System.currentTimeMillis()
  ): Unit = {
    require(nowMillis >= 0L, "job creation time must be non-negative")
    store.createDir(layout.prefix, recursive = true)
    val expected = JobOwnerManifest(
      JobOwnerManifest.CurrentVersion,
      layout.jobId,
      nowMillis,
      descriptor.owner,
      descriptor.writeMode.name
    )
    val owner =
      if (store.exists(layout.owner)) {
        val existing = ownerManifest()
        if (
          existing.formatVersion != JobOwnerManifest.CurrentVersion ||
          existing.jobId != layout.jobId ||
          existing.owner != descriptor.owner ||
          existing.writeMode != descriptor.writeMode.name
        ) {
          throw new IllegalStateException(
            s"${layout.owner} does not describe ${descriptor.owner.database}.${descriptor.owner.collection} " +
              s"${descriptor.writeMode.name} job ${layout.jobId}"
          )
        }
        existing
      } else {
        store.write(
          layout.owner,
          expected.toJson.getBytes(StandardCharsets.UTF_8)
        )
        expected
      }
    writeHeartbeat(owner, nowMillis)
  }

  /** Refreshes driver liveness for a job that has already written ownership. */
  def heartbeat(nowMillis: Long = System.currentTimeMillis()): Unit =
    writeHeartbeat(ownerManifest(), nowMillis)

  def ownerManifest(): JobOwnerManifest =
    JobOwnerManifest
      .fromJson(
        new String(store.readAll(layout.owner), StandardCharsets.UTF_8)
      )
      .fold(
        e =>
          throw new IllegalStateException(
            s"cannot read ${layout.owner}: ${e.getMessage}",
            e
          ),
        identity
      )

  /** The job manifest this job committed. */
  def manifest(): JobManifest =
    JobManifest
      .fromJson(
        new String(store.readAll(layout.manifest), StandardCharsets.UTF_8)
      )
      .fold(
        e =>
          throw new IllegalStateException(
            s"cannot read ${layout.manifest}: ${e.getMessage}",
            e
          ),
        identity
      )

  /** Records that Milvus has registered the job's segments; a second
    * registration of the job then does nothing.
    */
  def markRegistered(nowMillis: Long = System.currentTimeMillis()): Unit =
    store.write(
      layout.registered,
      nowMillis.toString.getBytes(StandardCharsets.UTF_8)
    )

  def commit(
      segments: Seq[CommittedSegment],
      nowMillis: Long = System.currentTimeMillis(),
      descriptor: Option[JobDescriptor] = None
  ): CommitOutcome = {
    if (store.exists(layout.marker)) {
      val marked =
        new String(store.readAll(layout.marker), StandardCharsets.UTF_8).trim
      if (marked != layout.jobId) {
        throw new IllegalStateException(
          s"${layout.marker} marks job '$marked', not job '${layout.jobId}'"
        )
      }
      return CommitOutcome.AlreadyCommitted
    }
    val owner = descriptor.map { value =>
      start(value, nowMillis)
      ownerManifest()
    }
    val manifest = owner match {
      case Some(value) =>
        JobManifest(
          layout.jobId,
          value.createdAtMillis,
          segments,
          formatVersion = Some(JobManifest.CurrentVersion),
          owner = Some(value.owner),
          writeMode = Some(value.writeMode)
        )
      case None =>
        // Keep the legacy shape for callers that cannot name an owner. It is
        // still registrable, but cleanup deliberately refuses to infer its
        // collection or write mode.
        JobManifest(layout.jobId, nowMillis, segments)
    }
    store.createDir(layout.prefix, recursive = true)
    store.write(
      layout.manifest,
      manifest.toJson.getBytes(StandardCharsets.UTF_8)
    )
    store.write(layout.marker, layout.jobId.getBytes(StandardCharsets.UTF_8))
    CommitOutcome.Committed
  }

  /** Deletes every file under the staging prefix; returns how many. */
  def abort(): Int = {
    val files =
      store.list(layout.prefix, recursive = true).filterNot(_.isDirectory)
    files.foreach(f => store.delete(f.path))
    files.size
  }

  private def writeHeartbeat(
      owner: JobOwnerManifest,
      nowMillis: Long
  ): Unit = {
    if (owner.jobId != layout.jobId) {
      throw new IllegalStateException(
        s"${layout.owner} belongs to job '${owner.jobId}', not '${layout.jobId}'"
      )
    }
    if (nowMillis < owner.createdAtMillis) {
      throw new IllegalArgumentException(
        s"heartbeat $nowMillis precedes job creation ${owner.createdAtMillis}"
      )
    }
    val heartbeat = JobHeartbeat(
      JobHeartbeat.CurrentVersion,
      layout.jobId,
      nowMillis
    )
    store.write(
      layout.heartbeat,
      heartbeat.toJson.getBytes(StandardCharsets.UTF_8)
    )
  }
}
