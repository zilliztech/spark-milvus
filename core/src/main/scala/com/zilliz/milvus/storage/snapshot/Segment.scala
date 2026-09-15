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

  /** The column groups of a V2 segment; empty for a V3 segment, whose groups
    * are inside its manifest.
    */
  def columnGroups: Seq[V2ColumnGroup] = layout match {
    case SegmentLayout.ColumnGroups(gs) => gs
    case SegmentLayout.Manifest(_, _)   => Seq.empty
  }

  /** The delete files a source listed; empty when there are none or when they
    * are inside the manifest.
    */
  def deltaLogs: Seq[DeltaLogFile] = deletes match {
    case DeleteFiles.Listed(files) => files
    case _                         => Seq.empty
  }

  /** Keeps, for every field, only the column group with the largest slot. A V3
    * segment is returned as it is.
    *
    * A segment that went through add-field and backfill can list a field twice:
    * the older multi-field parquet still holds the column and reports it in its
    * own schema, and the newer single-field group reports it too. The native
    * reader would then pick either source, and the older one can return stale
    * or null values.
    *
    * The premise: milvus-storage names a single-field group's directory after
    * the field id (100 and up) and a multi-field group's after a small
    * synthetic number, and Milvus allocates field ids in increasing order, so
    * the newest owner of a field is the group with the largest slot. This is
    * observed behaviour, not a documented guarantee. A group with an unknown
    * slot (`-1`) disables the dedup for the whole segment.
    */
  def dedupColumnGroupsBySlot: Segment = layout match {
    case SegmentLayout.ColumnGroups(groups)
        if groups.nonEmpty && !groups.exists(_.slotFieldId < 0L) =>
      val maxSlotPerField: Map[Long, Long] =
        groups
          .flatMap(g => g.fieldIds.map(fid => fid -> g.slotFieldId))
          .groupBy(_._1)
          .map { case (fid, pairs) => fid -> pairs.map(_._2).max }
      val rebuilt = groups.flatMap { g =>
        val keptFids =
          g.fieldIds.filter(fid => maxSlotPerField(fid) == g.slotFieldId)
        if (keptFids.isEmpty) None
        else Some(g.copy(fieldIds = keptFids))
      }
      copy(layout = SegmentLayout.ColumnGroups(rebuilt))
    case _ => this
  }
}

object Segment {

  /** A `storage_version = 2` segment as its sources describe it: the Avro
    * segment manifest, a backup export, or the option string. The row count is
    * always known for one, and its delete files are always listed.
    */
  def v2(
      id: Long,
      partitionId: Long,
      rows: Long,
      columnGroups: Seq[V2ColumnGroup],
      deltaLogs: Seq[DeltaLogFile] = Seq.empty
  ): Segment =
    Segment(
      id = id,
      partitionId = partitionId,
      storageVersion = 2,
      rows = Some(rows),
      layout = SegmentLayout.ColumnGroups(columnGroups),
      deletes =
        if (deltaLogs.isEmpty) DeleteFiles.Empty
        else DeleteFiles.Listed(deltaLogs)
    )
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
