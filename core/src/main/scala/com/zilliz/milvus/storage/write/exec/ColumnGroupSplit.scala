package com.zilliz.milvus.storage.write.exec

import com.zilliz.milvus.storage.schema.MilvusTypes
import io.milvus.grpc.schema.DataType

/** How a V3 segment's columns are split into column groups: the rule Milvus
  * applies when it writes a segment itself, expressed as the `schema_based`
  * patterns of milvus-storage's writer.
  *
  * Milvus's rule (`internal/storagecommon/split_policy.go`, the default
  * policies with their default parameters):
  *
  *   1. the system fields (id below 100), the primary key, the partition key
  *      and the clustering key make one group; 2. a field whose values average
  *      at least 1024 bytes gets its own group; 3. a vector or Text field gets
  *      its own group; 4. everything left makes one group.
  *
  * milvus-storage's `schema_based` policy matches each column name against the
  * patterns in order and puts a column into the group of the first pattern it
  * matches; columns matching none share the last group. Column names in a
  * written segment are the field ids, so a pattern is `^(0|1|100)$`.
  */
object ColumnGroupSplit {

  /** One column of the batch being written, by what the rule looks at. */
  final case class Column(
      fieldId: Long,
      dataType: DataType,
      isKey: Boolean
  )

  /** The first user field id; everything below is a system field. */
  val FirstUserFieldId: Long = 100L

  /** Milvus's `common.storage.stv2.splitByAvgSize.threshold` default. */
  val AvgSizeThreshold: Long = 1024L

  /** The `schema_based` patterns for `columns`, in the order the groups come
    * out. `avgBytes` is the average size of a value of a field, when the caller
    * has sampled it; a field with no sample is not split by size.
    */
  def milvusPatterns(
      columns: Seq[Column],
      avgBytes: Long => Option[Long] = _ => None
  ): Seq[String] = {
    val systemAndKeys =
      columns.filter(c => c.fieldId < FirstUserFieldId || c.isKey)
    val alone = columns.filterNot(systemAndKeys.contains).filter { c =>
      avgBytes(c.fieldId).exists(_ >= AvgSizeThreshold) ||
      MilvusTypes.isVectorType(c.dataType) || c.dataType == DataType.Text
    }
    val first =
      if (systemAndKeys.isEmpty) Seq.empty
      else Seq(systemAndKeys.map(_.fieldId).mkString("^(", "|", ")$"))
    first ++ alone.map(c => s"^${c.fieldId}$$")
  }

  /** The writer properties that apply `patterns`; empty patterns leave the
    * writer's default, one group for every column.
    */
  def writerProperties(patterns: Seq[String]): Map[String, String] =
    if (patterns.isEmpty) Map.empty
    else {
      patterns.find(_.contains(',')).foreach { p =>
        throw new IllegalArgumentException(
          s"column group pattern '$p' contains a comma, the list separator"
        )
      }
      Map(
        "writer.policy" -> "schema_based",
        "writer.split.schema_based.patterns" -> patterns.mkString(",")
      )
    }
}
