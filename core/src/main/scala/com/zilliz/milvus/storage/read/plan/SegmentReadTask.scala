package com.zilliz.milvus.storage.read.plan

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.snapshot.SegmentLayout

/** Where the rows deleted from a segment come from.
  *
  * Two cases, and the difference matters. [[Materialized]] is what the driver
  * produces today: it reads every delta log and ships the resulting primary-key
  * map inside each partition, which is one of the costs section 1 of
  * docs/design/README.md lists against 1.x. [[Files]] names the delta logs and
  * leaves the reading to the executor, which is where this is going.
  *
  * Modelling both keeps the current behaviour from being the only expressible
  * one; moving over is then a change of what the planner emits, not a change to
  * this type.
  */
sealed trait DeleteSource extends Serializable {

  /** The plan to apply, for a reader that evaluates deletes itself rather than
    * reading the delta logs.
    *
    * [[DeleteSource.Files]] throws here on purpose. Answering with an empty
    * plan would let a reader that cannot read delta logs return deleted rows
    * with no exception and no warning, which is the failure the delete path
    * already had once.
    */
  def materializedPlan: DeletePlan
}

object DeleteSource {

  /** Deletes are off for this read (`milvus.read.apply.deletes=false`), or
    * nothing was deleted from this segment.
    */
  case object None extends DeleteSource {
    override def materializedPlan: DeletePlan = DeletePlan.empty
  }

  /** The plan the driver already built. */
  final case class Materialized(plan: DeletePlan) extends DeleteSource {
    override def materializedPlan: DeletePlan = plan
  }

  /** Delta logs to read on the executor. `entryCounts` is parallel to `paths`
    * and lets a reader size its bitset before opening anything.
    */
  final case class Files(paths: Seq[String], entryCounts: Seq[Long])
      extends DeleteSource {
    require(
      paths.size == entryCounts.size,
      s"${paths.size} delta log paths but ${entryCounts.size} entry counts"
    )

    override def materializedPlan: DeletePlan =
      throw new UnsupportedOperationException(
        s"these ${paths.size} delta log(s) have not been read; a reader that " +
          "cannot read them itself needs a materialized plan from the planner"
      )
  }
}

/** One segment, read by one task.
  *
  * Descriptions only: no native handle, no Hadoop `Configuration`, no open
  * resource. A handle is a pointer inside one process and this object is built
  * on the driver and shipped, so anything that cannot survive serialization
  * cannot be a field here. The reader opens what it needs on the executor from
  * what this says.
  *
  * @param properties
  *   the `fs.*` map from `core.credential.StorageProperties`, already parsed
  *   and validated. Paths below are bucket-relative keys, because the C
  *   filesystem roots itself at `fs.bucket_name` and appends what it is given.
  * @param neededFieldIds
  *   Milvus field ids the caller wants, empty for all of them. Field ids rather
  *   than column names, because that is what a snapshot and a parquet footer
  *   agree on; resolving them to Arrow column names needs the schema.
  */
final case class SegmentReadTask(
    segmentId: Long,
    partitionId: Long,
    layout: SegmentLayout,
    schemaBytes: Array[Byte],
    properties: Map[String, String],
    neededFieldIds: Seq[Long] = Seq.empty,
    deletes: DeleteSource = DeleteSource.None
) extends Serializable {

  /** True when this partition has to evaluate deletes at all. A reader checks
    * this before pulling the primary-key and timestamp columns it would
    * otherwise not need, which is what makes deletes cost column pruning.
    */
  def appliesDeletes: Boolean = deletes match {
    case DeleteSource.None               => false
    case DeleteSource.Materialized(plan) => !plan.isEmpty
    case DeleteSource.Files(paths, _)    => paths.nonEmpty
  }

  /** The delete plan this partition applies. Shorthand for
    * `deletes.materializedPlan`, which is what every reader needs today.
    */
  def deletePlan: DeletePlan = deletes.materializedPlan

  /** The manifest version this partition was pinned to, or -1 when the layout
    * names none (a column-group layout has no manifest at all).
    */
  def readVersionOrLatest: Long = layout match {
    case SegmentLayout.Manifest(_, version) => version
    case SegmentLayout.ColumnGroups(_)      => -1L
  }

  /** Every data file this partition will open, for logging and for the plan's
    * own accounting. Empty for a manifest layout: the file list is inside the
    * manifest and only the native library has read it.
    */
  def dataFiles: Seq[String] = layout match {
    case SegmentLayout.Manifest(_, _)   => Seq.empty
    case SegmentLayout.ColumnGroups(gs) => gs.flatMap(_.filePaths)
  }

  /** Rows this partition is expected to deliver, when the layout says.
    *
    * All column groups of a segment carry the same row total, so one group's
    * sum is the segment's. A reader compares this against what it actually
    * delivered: a short read has to be an error, not a short DataFrame.
    */
  def expectedRows: Option[Long] = layout match {
    case SegmentLayout.Manifest(_, _) => scala.None
    case SegmentLayout.ColumnGroups(gs) =>
      gs.headOption.map(_.fileRowCounts.sum)
  }
}
