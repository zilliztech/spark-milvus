package com.zilliz.milvus.storage.write.exec

/** Where a write job puts its files before it is committed and registered:
  * `{root}/staging/{job}/`, as keys relative to the bucket.
  *
  * The prefix is outside `insert_log/` on purpose: DataCoord reclaims an
  * unregistered segment directory under `insert_log/{coll}/{part}/{seg}` after
  * `dataCoord.gc.missingTolerance` (86400 seconds by default), and a job may
  * legitimately take longer than that between writing and registering. The job
  * manifest (`core.write.commit`) lives at [[manifest]].
  */
final case class StagingLayout(rootPath: String, jobId: String) {
  require(StagingLayout.isSafeJobId(jobId), s"bad job id '$jobId'")

  private val root = Option(rootPath).map(_.trim.stripSuffix("/")).getOrElse("")

  /** `{root}/staging/{job}` */
  def prefix: String =
    (if (root.isEmpty) "" else root + "/") + s"staging/$jobId"

  /** One task's segment directory:
    * `{prefix}/{partition}/task_{partition}_{task}`.
    */
  def segment(partitionId: Int, taskId: Long): String =
    s"$prefix/$partitionId/task_${partitionId}_$taskId"

  /** The job manifest the committer writes. */
  def manifest: String = s"$prefix/manifest.json"

  /** Immutable collection ownership, written before executor tasks start. */
  def owner: String = s"$prefix/owner.json"

  /** Driver liveness, refreshed while executor tasks may still be writing. */
  def heartbeat: String = s"$prefix/_heartbeat"

  /** The marker the committer writes last; its presence means committed. */
  def marker: String = s"$prefix/_committed"

  /** The marker registration writes once Milvus has taken the segments. */
  def registered: String = s"$prefix/_registered"
}

object StagingLayout {

  private val SafeJobId = "[A-Za-z0-9][A-Za-z0-9._-]*".r

  /** Job ids are one storage-key component, not arbitrary paths. This accepts
    * the UUID, Spark application and backfill ids used by the connector while
    * refusing dot segments, separators, whitespace and control characters.
    */
  def isSafeJobId(jobId: String): Boolean =
    Option(jobId).exists(value => SafeJobId.pattern.matcher(value).matches())

  /** `{root}/staging`, whose immediate children are job ids. */
  def jobsRoot(rootPath: String): String = {
    val root = Option(rootPath).map(_.trim.stripSuffix("/")).getOrElse("")
    (if (root.isEmpty) "" else root + "/") + "staging"
  }
}
