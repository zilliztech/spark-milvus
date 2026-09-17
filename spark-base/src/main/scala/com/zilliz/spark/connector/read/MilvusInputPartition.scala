package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.spark.connector.options.MilvusOption

/** The two segment layouts a read can produce, so a caller can dispatch on
  * which line a partition belongs to without matching on Spark's own type. A
  * `vector.search.*` request travels in `milvusOption` for both lines.
  */
sealed trait MilvusInputPartition extends InputPartition {
  def task: SegmentReadTask
  def milvusOption: MilvusOption
}

// `storage_version = 3`: parquet column groups under a manifest, read by
// milvus-storage's `loon_reader_new`. The enum is `milvus/internal/storage/rw.go`
// (V1=0, V2=2, V3=3); milvus-storage's own name for the manifest format,
// "format v2", is not used here.
//
// `storage_version = 2` (column groups with no manifest) is
// [[MilvusV2InputPartition]].
case class MilvusV3InputPartition(
    task: SegmentReadTask, // What to read: layout, schema, fs.* map, deletes
    partitionName: String, // Snapshot reads store the partition ID string here.
    milvusOption: MilvusOption
) extends MilvusInputPartition

/** InputPartition for milvus-segment-info `storage_version = 2` — the
  * non-manifest packed-parquet format. No `.milvus_manifest` file exists; the
  * column-group layout is recovered from the snapshot AVRO + parquet footer
  * kv-metadata by [[SegmentManifestReader]] + [[ParquetFooterReader]] on the
  * driver, and arrives here inside `task` as `SegmentLayout.ColumnGroups`: one
  * group per physical parquet file set. The reader projects
  * `task.neededFieldIds` across them and only opens the files of the groups
  * carrying those columns.
  */
case class MilvusV2InputPartition(
    task: SegmentReadTask, // What to read: layout, schema, fs.* map, deletes
    milvusOption: MilvusOption
) extends MilvusInputPartition
