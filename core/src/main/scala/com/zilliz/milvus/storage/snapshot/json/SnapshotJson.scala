package com.zilliz.milvus.storage.snapshot.json

import com.fasterxml.jackson.annotation.{JsonAlias, JsonProperty}
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.JsonNode

import com.zilliz.milvus.storage.manifest.AvroManifestEntry

/** `snapshot_info`. */
case class SnapshotInfoJson(
    @JsonProperty("name") name: String,
    @JsonProperty("id") rawId: Option[JsonNode] = None,
    @JsonProperty("description") description: Option[String] = None,
    @JsonProperty("collection_id") rawCollectionId: Option[JsonNode] = None,
    @JsonProperty("partition_ids") rawPartitionIds: Option[JsonNode] = None,
    @JsonProperty("create_ts") rawCreateTs: Option[JsonNode] = None,
    @JsonProperty("state") state: Option[String] = None,
    @JsonProperty("pending_start_time") pendingStartTime: Option[JsonNode] =
      None
) {
  def id: Long = rawId.map(JsonValues.toLong).getOrElse(0L)
  def collectionId: Long =
    rawCollectionId.map(JsonValues.toLong).getOrElse(0L)
  def partitionIds: Seq[Long] =
    rawPartitionIds.map(JsonValues.toLongSeq).getOrElse(Seq.empty)
  def createTs: Long = rawCreateTs.map(JsonValues.toLong).getOrElse(0L)
}

/** The document inside a `storagev2_manifest_list` entry's `manifest` string:
  * the manifest version and the segment's base path.
  */
case class ManifestContentJson(
    @JsonProperty("ver") ver: Int,
    @JsonProperty("base_path") basePath: String
)

object ManifestContentJson {
  def parse(json: String): Either[Throwable, ManifestContentJson] =
    Mapper.read[ManifestContentJson](json)
}

/** One entry of `storagev2_manifest_list`: a `storage_version = 3` segment.
  *
  * The JSON key says V2 because milvus-storage calls its manifest format
  * "format v2"; the key is written by DataCoord and is read here as it is.
  * `segmentIDLong` is this connector's own addition for the option-string form,
  * where the id must survive as a 64-bit value.
  */
case class ManifestItemJson(
    @JsonProperty("segmentID") @JsonAlias(
      Array("segment_id")
    ) rawSegmentID: Option[JsonNode] = None,
    @JsonProperty("manifest") manifest: String = "",
    @JsonProperty("segmentIDLong") rawSegmentIDLong: Option[JsonNode] = None
) {
  def segmentID: Long =
    rawSegmentIDLong
      .map(JsonValues.toLong)
      .orElse(rawSegmentID.map(JsonValues.toLong))
      .getOrElse(0L)
}

object ManifestItemJson {
  def apply(segmentID: Long, manifest: String): ManifestItemJson =
    new ManifestItemJson(None, manifest, Some(LongNode.valueOf(segmentID)))
}

/** One file of a `deltalog_files` group (`Binlog` in the Milvus proto). */
case class BinlogJson(
    @JsonProperty("entries_num") rawEntriesNum: Option[JsonNode] = None,
    @JsonProperty("log_path") logPath: String = "",
    @JsonProperty("log_id") rawLogId: Option[JsonNode] = None
) {
  def entriesNum: Long = rawEntriesNum.map(JsonValues.toLong).getOrElse(0L)
  def logId: Long = rawLogId.map(JsonValues.toLong).getOrElse(0L)
}

/** One `deltalog_files` group (`FieldBinlog` in the Milvus proto). */
case class FieldBinlogJson(
    @JsonProperty("field_id") rawFieldId: Option[JsonNode] = None,
    @JsonProperty("binlogs") binlogs: Seq[BinlogJson] = Seq.empty
) {
  def fieldId: Long = rawFieldId.map(JsonValues.toLong).getOrElse(0L)
}

/** One entry of `segments` (older files: `segment_infos`). */
case class SegmentJson(
    @JsonProperty("segment_id") rawSegmentId: Option[JsonNode] = None,
    @JsonProperty("partition_id") rawPartitionId: Option[JsonNode] = None,
    @JsonProperty("segment_level") rawSegmentLevel: Option[JsonNode] = None,
    @JsonProperty("storage_version") rawStorageVersion: Option[JsonNode] = None,
    @JsonProperty("deltalog_files") deltaLogFiles: Seq[FieldBinlogJson] =
      Seq.empty
) {
  def segmentId: Long = rawSegmentId.map(JsonValues.toLong).getOrElse(0L)
  def partitionId: Long = rawPartitionId.map(JsonValues.toLong).getOrElse(0L)
  def segmentLevel: Option[Long] = rawSegmentLevel.map(JsonValues.toLong)
  def storageVersion: Long =
    rawStorageVersion.map(JsonValues.toLong).getOrElse(0L)
}

/** The snapshot file. `manifest_list` names the Avro segment manifests of the
  * `storage_version = 2` segments; `storagev2_manifest_list` carries the
  * `storage_version = 3` segments. `format_version` decides the Avro schema of
  * the manifests.
  */
case class SnapshotJson(
    @JsonProperty("snapshot_info") @JsonAlias(
      Array("snapshot-info")
    ) snapshotInfo: SnapshotInfoJson,
    @JsonProperty("collection") collection: CollectionJson,
    @JsonProperty("format_version") formatVersion: Option[Int] = None,
    @JsonProperty("indexes") indexes: Seq[Any] = Seq.empty,
    @JsonProperty("manifest_list") @JsonAlias(
      Array("manifest-list")
    ) manifestList: Seq[String] = Seq.empty,
    @JsonProperty("storagev2_manifest_list") @JsonAlias(
      Array("storagev2-manifest-list")
    ) storageV2ManifestList: Option[Seq[ManifestItemJson]] = None,
    @JsonProperty("segments") segments: Seq[SegmentJson] = Seq.empty,
    @JsonProperty("segment_infos") @JsonAlias(
      Array("segment-infos", "segmentInfos")
    ) segmentInfos: Seq[SegmentJson] = Seq.empty
) {
  def allSegments: Seq[SegmentJson] =
    if (segments.nonEmpty) segments else segmentInfos

  def manifestSchemaVersion: Int = formatVersion match {
    case Some(v) if v >= 2 => v
    case _                 => 1
  }
}

object SnapshotJson {

  /** The size above which a snapshot file is refused rather than read. */
  val MaxBytes: Long = 64L * 1024L * 1024L

  def parse(json: String): Either[Throwable, SnapshotJson] =
    Mapper.read[SnapshotJson](json)

  /** Reads a stream as UTF-8 and fails once it passes `maxBytes`, so a misnamed
    * multi-gigabyte object never reaches the parser.
    */
  def readUtf8WithLimit(
      in: java.io.InputStream,
      path: String,
      maxBytes: Long = MaxBytes
  ): String = {
    if (maxBytes <= 0) {
      throw new IllegalArgumentException(
        s"Snapshot metadata max size must be positive, got $maxBytes"
      )
    }
    val out = new java.io.ByteArrayOutputStream()
    val buf = new Array[Byte](8192)
    var total = 0L
    var n = in.read(buf)
    while (n >= 0) {
      total += n
      if (total > maxBytes) {
        throw new IllegalArgumentException(
          s"Snapshot metadata $path exceeds max size $maxBytes bytes"
        )
      }
      out.write(buf, 0, n)
      n = in.read(buf)
    }
    out.toString(java.nio.charset.StandardCharsets.UTF_8.name())
  }

  /** The same document with its segment list taken from the Avro segment
    * manifests instead of the JSON's own `segments`.
    */
  def fromAvroEntries(
      snapshotInfo: SnapshotInfoJson,
      collection: CollectionJson,
      manifestList: Seq[String],
      storageV2ManifestList: Option[Seq[ManifestItemJson]],
      entries: Seq[AvroManifestEntry]
  ): SnapshotJson = {
    def long(v: Long): Option[JsonNode] = Some(LongNode.valueOf(v))
    SnapshotJson(
      snapshotInfo = snapshotInfo,
      collection = collection,
      manifestList = manifestList,
      storageV2ManifestList = storageV2ManifestList,
      segments = entries.map(entry =>
        SegmentJson(
          rawSegmentId = long(entry.segmentId),
          rawPartitionId = long(entry.partitionId),
          rawSegmentLevel = long(entry.segmentLevel),
          rawStorageVersion = long(entry.storageVersion),
          deltaLogFiles = entry.deltaLogFiles.map(group =>
            FieldBinlogJson(
              rawFieldId = long(group.slotFieldId),
              binlogs = group.binlogs.map(log =>
                BinlogJson(
                  rawEntriesNum = long(log.entriesNum),
                  logPath = log.logPath,
                  rawLogId = long(log.logId)
                )
              )
            )
          )
        )
      )
    )
  }
}
