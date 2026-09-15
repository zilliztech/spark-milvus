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

  def commit(
      segments: Seq[CommittedSegment],
      nowMillis: Long = System.currentTimeMillis()
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
    val manifest = JobManifest(layout.jobId, nowMillis, segments)
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
}
