package com.zilliz.milvus.storage.read.plan

import com.zilliz.milvus.storage.delete.MilvusDeletePlan
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup

/** Where a segment's column groups come from.
  *
  * This is the only thing that differs between the two segment layouts, which
  * is why it is the only thing modelled as a choice. Everything else a reader
  * needs is the same for both and lives flat in [[InputSpec]].
  */
sealed trait SegmentLayout extends Serializable

object SegmentLayout {

  /** `storage_version = 3`: the native library reads the layout out of the
    * manifest at `basePath`.
    *
    * @param readVersion
    *   the manifest version to read, or -1 for the latest. A planner that
    *   resolved a version pins it, so every task in the job reads the same one
    *   even if a compaction commits a newer manifest meanwhile.
    */
  final case class Manifest(basePath: String, readVersion: Long = -1L)
      extends SegmentLayout

  /** `storage_version = 2`: no manifest exists, so the layout was recovered on
    * the driver from the snapshot AVRO plus each parquet footer's
    * `group_field_id_list` and is carried here already materialized.
    */
  final case class ColumnGroups(groups: Seq[V2ColumnGroup])
      extends SegmentLayout
}

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
sealed trait DeleteSource extends Serializable

object DeleteSource {

  /** Deletes are off for this read (`milvus.read.apply.deletes=false`). */
  case object None extends DeleteSource

  /** The plan the driver already built. */
  final case class Materialized(plan: MilvusDeletePlan) extends DeleteSource

  /** Delta logs to read on the executor. `entryCounts` is parallel to `paths`
    * and lets a reader size its bitset before opening anything.
    */
  final case class Files(paths: Seq[String], entryCounts: Seq[Long])
      extends DeleteSource {
    require(
      paths.size == entryCounts.size,
      s"${paths.size} delta log paths but ${entryCounts.size} entry counts"
    )
  }
}

/** What one Spark partition reads.
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
final case class InputSpec(
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

/** A whole read: one [[InputSpec]] per partition, plus what planning already
  * knows about the total.
  *
  * The totals are what a Spark `Statistics` reports (capability R13). They are
  * options because a manifest layout does not reveal its row count on the
  * driver without opening the manifest, and guessing a number that feeds the
  * optimizer is worse than admitting there is none.
  */
final case class ReadPlan(specs: Seq[InputSpec]) extends Serializable {

  def isEmpty: Boolean = specs.isEmpty

  /** Sum of the per-partition expectations, or nothing when any partition
    * cannot state one. A partial sum would read as the table's size.
    */
  def totalRows: Option[Long] =
    if (specs.isEmpty) Some(0L)
    else {
      val counts = specs.map(_.expectedRows)
      if (counts.forall(_.isDefined)) Some(counts.flatten.sum) else scala.None
    }

  def partitionsApplyingDeletes: Int = specs.count(_.appliesDeletes)
}
