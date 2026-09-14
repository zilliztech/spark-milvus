package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.storage.delete.MilvusDeletePlan
import com.zilliz.milvus.storage.read.plan.InputSpec
import com.zilliz.spark.connector.options.MilvusOption

/** The two segment layouts a read can produce, so a caller can dispatch on
  * which line a partition belongs to without matching on Spark's own type.
  */
sealed trait MilvusInputPartition extends InputPartition {
  def spec: InputSpec
  def milvusOption: MilvusOption
}

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
    spec: InputSpec, // What to read: layout, schema, fs.* map, deletes
    partitionName: String, // Snapshot reads store the partition ID string here.
    milvusOption: MilvusOption,
    topK: Option[Int] = None,
    queryVector: Option[Array[Float]] = None,
    metricType: Option[String] = None,
    vectorColumn: Option[String] = None
) extends MilvusInputPartition

/** InputPartition for milvus-segment-info `storage_version = 2` — the
  * non-manifest packed-parquet format. No `.milvus_manifest` file exists; the
  * column-group layout is recovered from the snapshot AVRO + parquet footer
  * kv-metadata by [[MilvusSegmentManifestReader]] +
  * [[MilvusParquetFooterReader]] on the driver, and arrives here inside `spec`
  * as `SegmentLayout.ColumnGroups`: one group per physical parquet file set.
  * The reader projects `spec.neededFieldIds` across them and only opens the
  * files of the groups carrying those columns.
  */
case class MilvusPackedV2InputPartition(
    spec: InputSpec, // What to read: layout, schema, fs.* map, deletes
    milvusOption: MilvusOption,
    inheritedDeletePlanPartitionId: Option[Long] = None
) extends MilvusInputPartition

case class MilvusPackedV2DeleteContext(
    inheritedPlansByPartition: Map[Long, MilvusDeletePlan]
)

object MilvusPackedV2DeleteContext {
  val empty: MilvusPackedV2DeleteContext =
    MilvusPackedV2DeleteContext(Map.empty)
}
