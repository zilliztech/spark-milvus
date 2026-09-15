package com.zilliz.milvus.storage.snapshot

/** One segment of a [[Snapshot]].
  *
  * V2 and V3 segments are the same type; the difference is confined to
  * [[SegmentLayout]]. `rows` is known for a V2 segment (the snapshot's Avro
  * carries it) and unknown for a V3 segment until its manifest is opened.
  */
final case class Segment(
    id: Long,
    partitionId: Long,
    storageVersion: Int,
    rows: Option[Long],
    layout: SegmentLayout,
    deletes: DeleteFiles
) {

  /** False for a delete-only (L0) segment, which has no column groups. */
  def hasData: Boolean = layout match {
    case SegmentLayout.Manifest(_, _)   => true
    case SegmentLayout.ColumnGroups(gs) => gs.nonEmpty
  }
}

/** Where a segment's column groups come from.
  *
  * This is the only thing that differs between the two segment layouts, which
  * is why it is the only thing modelled as a choice. Everything else a reader
  * needs is the same for both.
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

/** Where a segment's delete files are. Three of the four states carry
  * information; `Unknown` is the state a source that cannot say must use, so
  * that a read with deletes on fails at planning instead of returning deleted
  * rows.
  */
sealed trait DeleteFiles

object DeleteFiles {

  /** The source confirmed there are no deletes. */
  case object Empty extends DeleteFiles

  /** V2: the snapshot's Avro lists the delta logs. */
  final case class Listed(files: Seq[DeltaLogFile]) extends DeleteFiles

  /** V3: the manifest at the segment's read version lists them. */
  case object InManifest extends DeleteFiles

  /** The source cannot tell. */
  case object Unknown extends DeleteFiles
}
