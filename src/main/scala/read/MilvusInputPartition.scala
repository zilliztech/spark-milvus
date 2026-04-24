package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.spark.connector.MilvusOption

// InputPartition for milvus-segment-info `storage_version = 3` (StorageV3) —
// the manifest-based packed parquet format consumed by milvus-storage's
// `loon_reader_new` via `LoonManifest`. See `milvus/internal/storage/rw.go`
// for the authoritative segment-info enum (V1=0, V2=2, V3=3).
//
// For the non-manifest packed-parquet format (segment-info
// `storage_version = 2`, StorageV2) use [[MilvusPackedV2InputPartition]].
//
// Historical note: this class used to be called `MilvusStorageV2InputPartition`
// because the underlying milvus-storage library calls its own manifest format
// "format v2". That collided with the segment-info enum where V2 means
// something different; the class was renamed to match the enum.
case class MilvusStorageV3InputPartition(
    manifestPath: String, // Path to manifest in S3/MinIO
    milvusSchemaBytes: Array[Byte], // Serialized protobuf
    partitionName: String,
    milvusOption: MilvusOption,
    topK: Option[Int] = None,
    queryVector: Option[Array[Float]] = None,
    metricType: Option[String] = None,
    vectorColumn: Option[String] = None,
    segmentID: Long = -1L,
    readVersion: Long =
      -1L // -1 = LATEST, >0 = specific manifest version from snapshot
) extends InputPartition

/** InputPartition for milvus-segment-info `storage_version = 2` — the
  * non-manifest packed-parquet format. No `.milvus_manifest` file exists; the
  * column-group layout is recovered from the snapshot AVRO + parquet footer
  * kv-metadata by [[MilvusSegmentManifestReader]] +
  * [[MilvusParquetFooterReader]] and passed in as a materialized
  * `columnGroups`.
  *
  * @param columnGroups
  *   One [[V2ColumnGroup]] per physical parquet file set. The reader will
  *   project `neededColumnFieldIds` across these groups and only open the files
  *   of the groups that carry the requested columns.
  * @param neededColumnFieldIds
  *   Real milvus field IDs the caller wants. If empty the reader reads all
  *   columns declared by the column groups.
  */
case class MilvusPackedV2InputPartition(
    segmentID: Long,
    partitionID: Long,
    columnGroups: Seq[V2ColumnGroup],
    milvusSchemaBytes: Array[Byte],
    milvusOption: MilvusOption,
    neededColumnFieldIds: Seq[Long] = Seq.empty
) extends InputPartition
