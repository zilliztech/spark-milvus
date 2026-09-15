package com.zilliz.milvus.storage.snapshot

/** One column group of a `storage_version = 2` segment: the parquet files that
  * hold `fieldIds`, in row order, with each file's row count.
  *
  * `slotFieldId` is the group's directory name in object storage
  * (`AvroFieldBinlog.field_id` in the segment manifest): the field id itself
  * for a single-field group, a small synthetic number for a group holding
  * several fields. `-1` means the source did not carry it, and the slot dedup
  * below is skipped.
  */
case class V2ColumnGroup(
    fieldIds: Seq[Long],
    filePaths: Seq[String],
    fileRowCounts: Seq[Long] = Seq.empty,
    slotFieldId: Long = -1L
)

/** One delete file of a segment, as the segment manifest lists it: its
  * `log_id`, its path and the number of entries it holds. Both storage lines
  * list their delete files this way.
  */
case class DeltaLogFile(
    logId: Long,
    logPath: String,
    entriesNum: Long
)

/** A `storage_version = 2` segment as its sources describe it: the Avro segment
  * manifest, a backup export, or the option string. Becomes a [[Segment]] in
  * `SnapshotCatalog.fromV2`.
  */
case class V2SegmentInfo(
    segmentId: Long,
    partitionId: Long,
    numOfRows: Long,
    storageVersion: Long,
    columnGroups: Seq[V2ColumnGroup],
    deltaLogs: Seq[DeltaLogFile] = Seq.empty
) {

  /** Keeps, for every field, only the column group with the largest slot.
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
  def dedupColumnGroupsBySlot: V2SegmentInfo = {
    val groups = columnGroups
    if (groups.isEmpty || groups.exists(_.slotFieldId < 0L)) {
      return this
    }
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
    copy(columnGroups = rebuilt)
  }
}
