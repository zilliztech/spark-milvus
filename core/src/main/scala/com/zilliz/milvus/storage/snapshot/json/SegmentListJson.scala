package com.zilliz.milvus.storage.snapshot.json

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.JsonNode

import com.zilliz.milvus.storage.snapshot.{DeltaLogFile, Segment, V2ColumnGroup}

/** The connector's own JSON for carrying a segment list through one Spark
  * option: `milvus.snapshot.manifests` holds `Seq[ManifestItemJson]`,
  * `milvus.snapshot.v2.segments` holds `Seq[Segment]` (V2 segments). This is
  * the 1.x form of a snapshot read, kept while backfill hands its own read the
  * segments this way; it goes when `OptionSnapshotPlanner.fromOptionStrings`
  * goes.
  *
  * Every number is written as a `LongNode` and read back through
  * `JsonValues.toLong`: Jackson's Scala module erases `Seq[Long]` and boxes a
  * small value as `Integer`, which later fails `unboxToLong`.
  */
object SegmentListJson {

  def encodeManifestItems(items: Seq[ManifestItemJson]): String =
    Mapper.mapper.writeValueAsString(items)

  def decodeManifestItems(
      json: String
  ): Either[Throwable, Seq[ManifestItemJson]] =
    Mapper.read[Seq[ManifestItemJson]](json)

  def encodeV2Segments(segments: Seq[Segment]): String = {
    def long(v: Long): JsonNode = LongNode.valueOf(v)
    val items = segments.map { s =>
      V2SegmentJson(
        rawSegmentId = Some(long(s.id)),
        rawPartitionId = Some(long(s.partitionId)),
        rawNumOfRows = Some(long(s.rows.getOrElse(0L))),
        rawStorageVersion = Some(long(s.storageVersion)),
        columnGroups = s.columnGroups.map(cg =>
          V2ColumnGroupJson(
            fieldIds = cg.fieldIds.map(long),
            filePaths = cg.filePaths,
            fileRowCounts = cg.fileRowCounts.map(long),
            rawSlotFieldId = Some(long(cg.slotFieldId))
          )
        ),
        deltaLogs = s.deltaLogs.map(log =>
          DeltaLogFileJson(
            rawLogId = Some(long(log.logId)),
            logPath = log.logPath,
            rawEntriesNum = Some(long(log.entriesNum))
          )
        )
      )
    }
    Mapper.mapper.writeValueAsString(items)
  }

  def decodeV2Segments(json: String): Either[Throwable, Seq[Segment]] =
    Mapper
      .read[Seq[V2SegmentJson]](json)
      .map(_.map { d =>
        Segment.v2(
          id = d.segmentId,
          partitionId = d.partitionId,
          rows = d.numOfRows,
          columnGroups = d.columnGroups.map(cg =>
            V2ColumnGroup(
              fieldIds = cg.fieldIds.map(JsonValues.toLong),
              filePaths = cg.filePaths,
              fileRowCounts = cg.fileRowCounts.map(JsonValues.toLong),
              slotFieldId = cg.slotFieldId
            )
          ),
          deltaLogs = d.deltaLogs.map(log =>
            DeltaLogFile(
              logId = log.logId,
              logPath = log.logPath,
              entriesNum = log.entriesNum
            )
          )
        )
      })
}

private[json] case class V2ColumnGroupJson(
    @JsonProperty("field_ids") fieldIds: Seq[JsonNode] = Seq.empty,
    @JsonProperty("file_paths") filePaths: Seq[String] = Seq.empty,
    @JsonProperty("file_row_counts") fileRowCounts: Seq[JsonNode] = Seq.empty,
    @JsonProperty("slot_field_id") rawSlotFieldId: Option[JsonNode] = None
) {
  // -1 is "slot unknown", for a list written before the field existed; the
  // slot dedup in Segment is skipped for it rather than run on 0.
  def slotFieldId: Long =
    rawSlotFieldId.map(JsonValues.toLong).getOrElse(-1L)
}

private[json] case class DeltaLogFileJson(
    @JsonProperty("log_id") rawLogId: Option[JsonNode] = None,
    @JsonProperty("log_path") logPath: String = "",
    @JsonProperty("entries_num") rawEntriesNum: Option[JsonNode] = None
) {
  def logId: Long = rawLogId.map(JsonValues.toLong).getOrElse(0L)
  def entriesNum: Long = rawEntriesNum.map(JsonValues.toLong).getOrElse(0L)
}

private[json] case class V2SegmentJson(
    @JsonProperty("segment_id") rawSegmentId: Option[JsonNode] = None,
    @JsonProperty("partition_id") rawPartitionId: Option[JsonNode] = None,
    @JsonProperty("num_of_rows") rawNumOfRows: Option[JsonNode] = None,
    @JsonProperty("storage_version") rawStorageVersion: Option[JsonNode] = None,
    @JsonProperty("column_groups") columnGroups: Seq[V2ColumnGroupJson] =
      Seq.empty,
    @JsonProperty("delta_logs") deltaLogs: Seq[DeltaLogFileJson] = Seq.empty
) {
  def segmentId: Long = rawSegmentId.map(JsonValues.toLong).getOrElse(0L)
  def partitionId: Long = rawPartitionId.map(JsonValues.toLong).getOrElse(0L)
  def numOfRows: Long = rawNumOfRows.map(JsonValues.toLong).getOrElse(0L)
  def storageVersion: Long =
    rawStorageVersion.map(JsonValues.toLong).getOrElse(0L)
}
