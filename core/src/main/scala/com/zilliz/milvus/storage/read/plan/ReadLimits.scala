package com.zilliz.milvus.storage.read.plan

/** Resource limits carried by every segment read task.
  *
  * The driver parses user options into this type before it creates a plan. The
  * executor maps the two batch limits to milvus-storage reader properties and
  * uses `arrowMaxBytes` as the hard limit of the task-owned Arrow child
  * allocator. Keeping these values out of the raw `fs.*` property bag makes a
  * malformed resource option fail once during planning instead of separately in
  * every native reader.
  */
final case class ReadLimits(
    batchMaxRows: Int,
    batchMaxBytes: Long,
    arrowMaxBytes: Long
) extends Serializable {
  require(batchMaxRows > 0, s"batchMaxRows must be positive, got $batchMaxRows")
  require(
    batchMaxBytes > 0L,
    s"batchMaxBytes must be positive, got $batchMaxBytes"
  )
  require(
    batchMaxBytes <= ReadLimits.MaxBatchBytes,
    s"batchMaxBytes must be at most ${ReadLimits.MaxBatchBytes}, got $batchMaxBytes"
  )
  require(
    arrowMaxBytes > 0L,
    s"arrowMaxBytes must be positive, got $arrowMaxBytes"
  )
}

object ReadLimits {
  val DefaultBatchMaxRows: Int = 8192
  val DefaultBatchMaxBytes: Long = 32L * 1024L * 1024L

  /** The upstream property currently accepts at most 4 GiB. */
  val MaxBatchBytes: Long = 4L * 1024L * 1024L * 1024L

  /** Unset preserves the connector's previous unbounded allocator behavior. */
  val DefaultArrowMaxBytes: Long = Long.MaxValue

  val Default: ReadLimits = ReadLimits(
    DefaultBatchMaxRows,
    DefaultBatchMaxBytes,
    DefaultArrowMaxBytes
  )
}
