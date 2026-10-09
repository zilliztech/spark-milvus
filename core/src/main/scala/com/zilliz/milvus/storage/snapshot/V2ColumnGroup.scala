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
