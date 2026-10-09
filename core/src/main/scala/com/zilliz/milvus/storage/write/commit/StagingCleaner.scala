package com.zilliz.milvus.storage.write.commit

import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

import com.zilliz.milvus.storage.io.{FileInfo, ObjectStore}
import com.zilliz.milvus.storage.write.exec.StagingLayout

object CleanupAction {
  val Preserved = "preserved"
  val WouldDeleteFiles = "would_delete_files"
  val FilesDeleted = "files_deleted"
  val DeleteFailed = "delete_failed"
}

/** One audited staging child. `prefixDeleted` remains false until the native
  * filesystem exposes recursive directory deletion.
  */
final case class StagingCleanupResult(
    jobId: String,
    owner: Option[JobOwner],
    writeMode: Option[String],
    action: String,
    reason: String,
    lastHeartbeatMillis: Option[Long],
    candidateFiles: Int,
    deletedFiles: Int,
    directoriesRemaining: Int,
    prefixDeleted: Boolean = false
)

/** Audits collection-owned staging jobs and deletes only their file objects.
  *
  * Missing, legacy or inconsistent ownership is never inferred. A registered
  * job and every backfill job are always preserved. An append job becomes a
  * candidate only when its driver heartbeat and every file modification time
  * are older than the retention cutoff. The same state is read a second time
  * immediately before deletion.
  *
  * The current native binding has no directory-delete operation. Directory
  * entries are counted and reported, but never passed to `deleteFile` and the
  * prefix is never reported as deleted.
  */
final class StagingCleaner(store: ObjectStore, rootPath: String) {

  def clean(
      target: JobOwner,
      retentionMillis: Long,
      dryRun: Boolean,
      nowMillis: Long = System.currentTimeMillis()
  ): Seq[StagingCleanupResult] = {
    require(retentionMillis > 0L, "retentionMillis must be positive")
    require(nowMillis >= 0L, "nowMillis must be non-negative")
    val jobsRoot = StagingLayout.jobsRoot(rootPath)
    if (!store.exists(jobsRoot)) return Seq.empty

    val cutoffMillis = nowMillis - retentionMillis
    store
      .list(jobsRoot, recursive = false)
      .groupBy(entryName)
      .toSeq
      .sortBy(_._1)
      .map { case (jobId, entries) =>
        if (
          !StagingLayout.isSafeJobId(jobId) || entries.size != 1 ||
          !entries.head.isDirectory
        ) {
          preserved(
            jobId,
            s"${entries.headOption.map(_.path).getOrElse(jobsRoot)} is not one unambiguous job directory"
          )
        } else {
          val layout = StagingLayout(rootPath, jobId)
          val observedJobPath = entries.head.path.stripSuffix("/")
          inspectSafely(
            layout,
            observedJobPath,
            target,
            cutoffMillis,
            nowMillis
          ) match {
            case Left(result) => result
            case Right(candidate) if dryRun =>
              candidate.result(
                CleanupAction.WouldDeleteFiles,
                "eligible unregistered append files; dry run made no changes",
                deletedFiles = 0
              )
            case Right(first) =>
              delete(
                layout,
                observedJobPath,
                target,
                cutoffMillis,
                nowMillis,
                first
              )
          }
        }
      }
  }

  private def delete(
      layout: StagingLayout,
      observedJobPath: String,
      target: JobOwner,
      cutoffMillis: Long,
      nowMillis: Long,
      first: Candidate
  ): StagingCleanupResult =
    inspectSafely(
      layout,
      observedJobPath,
      target,
      cutoffMillis,
      nowMillis
    ) match {
      case Left(result) => result
      case Right(second) if first.fingerprint != second.fingerprint =>
        second.result(
          CleanupAction.Preserved,
          "prefix changed between cleanup checks",
          deletedFiles = 0
        )
      case Right(second) =>
        var deleted = 0
        val ordered = second.files.sortBy(file => deleteRank(file.path, layout))
        try {
          ordered.foreach { file =>
            store.delete(file.path)
            deleted += 1
          }
          second.result(
            CleanupAction.FilesDeleted,
            "eligible unregistered append files deleted; native directory deletion is unavailable",
            deletedFiles = deleted
          )
        } catch {
          case NonFatal(failure) =>
            second.result(
              CleanupAction.DeleteFailed,
              s"file deletion failed after $deleted file(s): ${failure.getMessage}",
              deletedFiles = deleted
            )
        }
    }

  private def inspectSafely(
      layout: StagingLayout,
      observedJobPath: String,
      target: JobOwner,
      cutoffMillis: Long,
      nowMillis: Long
  ): Either[StagingCleanupResult, Candidate] =
    try
      inspect(
        layout,
        observedJobPath,
        target,
        cutoffMillis,
        nowMillis
      )
    catch {
      case NonFatal(failure) =>
        Left(
          preserved(
            layout.jobId,
            s"cannot validate job metadata: ${failure.getMessage}"
          )
        )
    }

  private def inspect(
      layout: StagingLayout,
      observedJobPath: String,
      target: JobOwner,
      cutoffMillis: Long,
      nowMillis: Long
  ): Either[StagingCleanupResult, Candidate] = {
    if (store.exists(layout.registered)) {
      return Left(preserved(layout.jobId, "job has a _registered marker"))
    }
    if (!store.exists(layout.owner)) {
      return Left(preserved(layout.jobId, "owner.json is missing"))
    }
    val owner = parseOwner(layout)
    val mode = JobWriteMode.fromName(owner.writeMode)
    if (owner.formatVersion != JobOwnerManifest.CurrentVersion) {
      return Left(
        preserved(
          layout.jobId,
          s"unsupported owner format version ${owner.formatVersion}",
          Some(owner.owner),
          Some(owner.writeMode)
        )
      )
    }
    if (owner.jobId != layout.jobId) {
      return Left(
        preserved(
          layout.jobId,
          s"owner job id '${owner.jobId}' does not match the directory",
          Some(owner.owner),
          Some(owner.writeMode)
        )
      )
    }
    if (owner.owner != target) {
      return Left(
        preserved(
          layout.jobId,
          s"owner is ${owner.owner.database}.${owner.owner.collection}, not ${target.database}.${target.collection}",
          Some(owner.owner),
          Some(owner.writeMode)
        )
      )
    }
    if (
      !mode.contains(
        JobWriteMode.Append
      ) || owner.writeMode != JobWriteMode.Append.name
    ) {
      return Left(
        preserved(
          layout.jobId,
          s"write mode '${owner.writeMode}' is never automatically cleaned",
          Some(owner.owner),
          Some(owner.writeMode)
        )
      )
    }
    if (owner.createdAtMillis < 0L || owner.createdAtMillis > nowMillis) {
      return Left(
        preserved(
          layout.jobId,
          s"owner creation time ${owner.createdAtMillis} is invalid at $nowMillis",
          Some(owner.owner),
          Some(owner.writeMode)
        )
      )
    }
    if (!store.exists(layout.heartbeat)) {
      return Left(
        preserved(
          layout.jobId,
          "_heartbeat is missing",
          Some(owner.owner),
          Some(owner.writeMode)
        )
      )
    }
    val heartbeat = parseHeartbeat(layout)
    if (
      heartbeat.formatVersion != JobHeartbeat.CurrentVersion ||
      heartbeat.jobId != layout.jobId ||
      heartbeat.updatedAtMillis < owner.createdAtMillis ||
      heartbeat.updatedAtMillis > nowMillis
    ) {
      return Left(
        preserved(
          layout.jobId,
          s"heartbeat metadata is inconsistent for job ${layout.jobId}",
          Some(owner.owner),
          Some(owner.writeMode),
          Some(heartbeat.updatedAtMillis)
        )
      )
    }
    if (heartbeat.updatedAtMillis > cutoffMillis) {
      return Left(
        preserved(
          layout.jobId,
          s"last heartbeat ${heartbeat.updatedAtMillis} is newer than retention cutoff $cutoffMillis",
          Some(owner.owner),
          Some(owner.writeMode),
          Some(heartbeat.updatedAtMillis)
        )
      )
    }

    val hasManifest = store.exists(layout.manifest)
    val hasMarker = store.exists(layout.marker)
    if (hasMarker) {
      val marked = text(layout.marker).trim
      if (marked != layout.jobId) {
        return Left(
          preserved(
            layout.jobId,
            s"_committed marks job '$marked'",
            Some(owner.owner),
            Some(owner.writeMode),
            Some(heartbeat.updatedAtMillis)
          )
        )
      }
      if (!hasManifest) {
        return Left(
          preserved(
            layout.jobId,
            "_committed exists but manifest.json is missing",
            Some(owner.owner),
            Some(owner.writeMode),
            Some(heartbeat.updatedAtMillis)
          )
        )
      }
    }
    if (hasManifest) {
      val manifest = parseManifest(layout)
      val matches =
        manifest.formatVersion.contains(JobManifest.CurrentVersion) &&
          manifest.jobId == layout.jobId &&
          manifest.createdAtMillis == owner.createdAtMillis &&
          manifest.owner.contains(owner.owner) &&
          manifest.writeMode.contains(owner.writeMode) &&
          manifest.segments.forall(_.segmentId.isEmpty)
      if (!matches) {
        return Left(
          preserved(
            layout.jobId,
            "manifest.json is legacy or inconsistent with append ownership",
            Some(owner.owner),
            Some(owner.writeMode),
            Some(heartbeat.updatedAtMillis)
          )
        )
      }
    }

    val entries = store.list(layout.prefix, recursive = true)
    val outsideJob =
      entries.filterNot(info => isWithinObservedJob(info.path, observedJobPath))
    if (outsideJob.nonEmpty) {
      return Left(
        preserved(
          layout.jobId,
          s"storage listing returned ${outsideJob.size} path(s) outside the exact job prefix",
          Some(owner.owner),
          Some(owner.writeMode),
          Some(heartbeat.updatedAtMillis),
          entries.count(info => !info.isDirectory),
          entries.count(_.isDirectory) + 1
        )
      )
    }
    val files = entries.filterNot(_.isDirectory)
    val unknownTimes = files.filter(_.modifiedNanos <= 0L)
    if (unknownTimes.nonEmpty) {
      return Left(
        preserved(
          layout.jobId,
          s"${unknownTimes.size} file(s) have no usable modification time",
          Some(owner.owner),
          Some(owner.writeMode),
          Some(heartbeat.updatedAtMillis),
          files.size,
          entries.count(_.isDirectory) + 1
        )
      )
    }
    val latestModifiedNanos = files
      .map(_.modifiedNanos)
      .foldLeft(0L)((left, right) => math.max(left, right))
    if (laterThanMillis(latestModifiedNanos, nowMillis)) {
      return Left(
        preserved(
          layout.jobId,
          s"prefix contains a file modified in the future at $latestModifiedNanos nanoseconds",
          Some(owner.owner),
          Some(owner.writeMode),
          Some(heartbeat.updatedAtMillis),
          files.size,
          entries.count(_.isDirectory) + 1
        )
      )
    }
    if (laterThanMillis(latestModifiedNanos, cutoffMillis)) {
      return Left(
        preserved(
          layout.jobId,
          s"prefix contains a file modified after retention cutoff $cutoffMillis",
          Some(owner.owner),
          Some(owner.writeMode),
          Some(heartbeat.updatedAtMillis),
          files.size,
          entries.count(_.isDirectory) + 1
        )
      )
    }
    Right(
      new Candidate(
        layout.jobId,
        owner,
        heartbeat,
        files,
        entries.count(_.isDirectory) + 1,
        hasManifest,
        hasMarker
      )
    )
  }

  private def parseOwner(layout: StagingLayout): JobOwnerManifest =
    JobOwnerManifest
      .fromJson(text(layout.owner))
      .fold(error => throw invalid(layout.owner, error), identity)

  private def parseHeartbeat(layout: StagingLayout): JobHeartbeat =
    JobHeartbeat
      .fromJson(text(layout.heartbeat))
      .fold(error => throw invalid(layout.heartbeat, error), identity)

  private def parseManifest(layout: StagingLayout): JobManifest =
    JobManifest
      .fromJson(text(layout.manifest))
      .fold(error => throw invalid(layout.manifest, error), identity)

  private def text(path: String): String =
    new String(store.readAll(path), StandardCharsets.UTF_8)

  private def invalid(path: String, error: Throwable): IllegalStateException =
    new IllegalStateException(s"cannot read $path: ${error.getMessage}", error)

  private def entryName(info: FileInfo): String =
    info.path.stripSuffix("/").split("/").lastOption.getOrElse("")

  private def deleteRank(path: String, layout: StagingLayout): Int =
    if (sameKey(path, layout.owner)) 4
    else if (sameKey(path, layout.heartbeat)) 3
    else if (sameKey(path, layout.manifest) || sameKey(path, layout.marker)) 2
    else 1

  private def sameKey(listedPath: String, key: String): Boolean =
    listedPath == key || listedPath.endsWith("/" + key)

  private def isWithinObservedJob(
      path: String,
      observedJobPath: String
  ): Boolean =
    path == observedJobPath || path.startsWith(observedJobPath + "/")

  private def laterThanMillis(nanos: Long, millis: Long): Boolean = {
    val wholeMillis = nanos / 1000000L
    wholeMillis > millis ||
    (wholeMillis == millis && nanos % 1000000L > 0L)
  }

  private def preserved(
      jobId: String,
      reason: String,
      owner: Option[JobOwner] = None,
      writeMode: Option[String] = None,
      heartbeat: Option[Long] = None,
      candidateFiles: Int = 0,
      directoriesRemaining: Int = 0
  ): StagingCleanupResult =
    StagingCleanupResult(
      jobId,
      owner,
      writeMode,
      CleanupAction.Preserved,
      reason,
      heartbeat,
      candidateFiles,
      deletedFiles = 0,
      directoriesRemaining
    )

  private final class Candidate(
      val jobId: String,
      val ownerManifest: JobOwnerManifest,
      val heartbeat: JobHeartbeat,
      val files: Seq[FileInfo],
      val directoriesRemaining: Int,
      val hasManifest: Boolean,
      val hasMarker: Boolean
  ) {
    def fingerprint: (
        JobOwnerManifest,
        JobHeartbeat,
        Seq[(String, Long, Long)],
        Boolean,
        Boolean
    ) =
      (
        ownerManifest,
        heartbeat,
        files
          .map(file => (file.path, file.size, file.modifiedNanos))
          .sortBy(_._1),
        hasManifest,
        hasMarker
      )

    def result(
        action: String,
        reason: String,
        deletedFiles: Int
    ): StagingCleanupResult =
      StagingCleanupResult(
        jobId,
        Some(ownerManifest.owner),
        Some(ownerManifest.writeMode),
        action,
        reason,
        Some(heartbeat.updatedAtMillis),
        files.size,
        deletedFiles,
        directoriesRemaining
      )
  }
}
