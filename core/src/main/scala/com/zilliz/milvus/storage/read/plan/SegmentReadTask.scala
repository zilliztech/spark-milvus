package com.zilliz.milvus.storage.read.plan

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.snapshot.{
  DeltaLogFile,
  SegmentIndexes,
  SegmentLayout
}

/** Where the rows deleted from a segment come from.
  *
  * [[Files]] is what the planner emits: the delete files that apply to the
  * segment, its own and the partition's L0 ones, read on the executor by
  * `core.read.exec.DeletePlans`. [[Materialized]] carries a plan already read;
  * nothing on the driver produces it any more (it used to ship every
  * primary-key map inside every partition, one of the costs section 1 of
  * docs/design/README.md lists against 1.x), but a caller that has a plan in
  * hand, a test above all, can still hand it over.
  */
sealed trait DeleteSource extends Serializable {

  /** The plan, for a source that already holds one.
    *
    * [[DeleteSource.Files]] throws here on purpose. Answering with an empty
    * plan would let a reader that cannot read delta logs return deleted rows
    * with no exception and no warning, which is the failure the delete path
    * already had once. The executor resolves files through
    * `core.read.exec.DeletePlans`.
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

  /** A plan already read. */
  final case class Materialized(plan: DeletePlan) extends DeleteSource {
    override def materializedPlan: DeletePlan = plan
  }

  /** The delete files to read on the executor, each with its entry count so a
    * reader can size what it builds before opening anything.
    */
  final case class Files(files: Seq[DeltaLogFile]) extends DeleteSource {
    override def materializedPlan: DeletePlan =
      throw new UnsupportedOperationException(
        s"these ${files.size} delete file(s) have not been read; " +
          "core.read.exec.DeletePlans reads them on the executor"
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
    deletes: DeleteSource = DeleteSource.None,
    indexes: SegmentIndexes = SegmentIndexes.Unknown,
    snapshotRows: Option[Long] = None,
    limits: ReadLimits = ReadLimits.Default
) extends Serializable {

  /** True when this partition has to evaluate deletes at all. A reader checks
    * this before pulling the primary-key and timestamp columns it would
    * otherwise not need, which is what makes deletes cost column pruning.
    */
  def appliesDeletes: Boolean = deletes match {
    case DeleteSource.None               => false
    case DeleteSource.Materialized(plan) => !plan.isEmpty
    case DeleteSource.Files(files)       => files.nonEmpty
  }

  /** The plan of a source that already holds one; see
    * [[DeleteSource.materializedPlan]]. A reader goes through
    * `core.read.exec.DeletePlans`, which also reads files.
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

  /** Physical rows recorded by the snapshot, or by its column groups when the
    * source has no snapshot Avro count.
    *
    * All column groups of a segment carry the same row total, so one group's
    * sum is the segment's. A reader compares this against what it actually
    * delivered: a short read has to be an error, not a short DataFrame.
    */
  def expectedRows: Option[Long] = snapshotRows.orElse(layout match {
    case SegmentLayout.Manifest(_, _) => scala.None
    case SegmentLayout.ColumnGroups(gs) =>
      gs.headOption.map(_.fileRowCounts.sum)
  })
}
