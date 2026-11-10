package com.zilliz.spark.connector

/** DataSource batch planning and Arrow-backed partition readers for fixed
  * snapshots. MilvusScan preserves projection, predicates, deletes and runtime
  * pruning. SegmentScan exposes immutable segment metadata and planned
  * partitions to downstream consumers without owning their computation.
  * ColumnBinding maps logical fields onto physical columns; row and columnar
  * readers share the same read and exclusion contracts. MetadataColumns
  * synthesizes the position columns, among them the `_milvus_row_id` of a
  * physical read, from the planned snapshot id, segment and row offset.
  */
package object read
