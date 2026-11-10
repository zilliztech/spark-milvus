package com.zilliz.spark.connector.types

import org.apache.spark.sql.types.{LongType, StructField, StructType}

import com.zilliz.spark.connector.options.MilvusOption

/** `_milvus_row_id`: where a row is stored in one fixed snapshot. A physical
  * read produces it and the column replacement write consumes it, so the value
  * a caller carries from one to the other must reach the write unchanged. Its
  * fields belong to the connector; callers neither build nor interpret them.
  *
  * It is a struct of longs rather than an encoded token because Spark's write
  * planning can cluster and order by a struct field directly, while it cannot
  * decode an arbitrary token. `snapshot_id` binds the row to the snapshot it
  * was read from, `segment_id` and `row_offset` to its stored position.
  */
object RowIdentity {
  val ColumnName: String = MilvusOption.MilvusRowIdColumn

  val SnapshotIdField = "snapshot_id"
  val SegmentIdField = "segment_id"
  val RowOffsetField = "row_offset"

  val SnapshotIdOrdinal = 0
  val SegmentIdOrdinal = 1
  val RowOffsetOrdinal = 2

  val dataType: StructType = StructType(
    Seq(
      StructField(SnapshotIdField, LongType, nullable = false),
      StructField(SegmentIdField, LongType, nullable = false),
      StructField(RowOffsetField, LongType, nullable = false)
    )
  )

  val field: StructField = StructField(ColumnName, dataType, nullable = false)
}
