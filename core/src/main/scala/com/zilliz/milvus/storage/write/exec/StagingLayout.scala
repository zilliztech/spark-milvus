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
  require(jobId.nonEmpty && !jobId.contains('/'), s"bad job id '$jobId'")

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

  /** The marker the committer writes last; its presence means committed. */
  def marker: String = s"$prefix/_committed"

  /** The marker registration writes once Milvus has taken the segments. */
  def registered: String = s"$prefix/_registered"
}
