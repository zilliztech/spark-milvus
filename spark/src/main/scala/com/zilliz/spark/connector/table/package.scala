package com.zilliz.spark.connector

/** Fixed V3 snapshots exposed as read-only Spark tables. `MilvusTables`
  * validates DataSource input, resolves one Snapshot and derives its schema;
  * `SnapshotSources` reads either a snapshot file or complete V3 metadata.
  * Stores close after metadata is materialized. Partition and segment selectors
  * narrow the resolved snapshot. `MilvusTable` advertises BATCH_READ only.
  *
  * Field identity, types, nullability and vector dimensions come from the
  * snapshot schema. `_segment_id` and `_row_offset` are synthesized metadata;
  * `_timestamp` is stored field 1. No online service or catalog is consulted.
  *
  * Capabilities: R1, R12, R16 (see design document capabilities.md).
  */
package object table
