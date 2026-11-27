package com.zilliz.spark.connector

/** DataSource batch planning and Arrow-backed partition readers for fixed
  * snapshots. The scan builder applies partition and segment ID predicates
  * before planning segment reads. MilvusScan applies deletes for live reads and
  * includes rows marked deleted in physical reads. Both modes support
  * projection, predicates and runtime pruning. SegmentScan exposes immutable
  * segment metadata and planned partitions to downstream consumers without
  * owning their computation. ColumnBinding maps logical fields onto physical
  * columns, expanding struct arrays to their child field IDs. For older
  * segments it requests the stored siblings that supply array lengths. For
  * resolved column groups it can identify stored members by their Arrow Parquet
  * field IDs when the adapter's mapping is incomplete. A member can be treated
  * as absent only when its identity is unambiguous. With no remaining sibling,
  * both layouts check the stored field ids to establish that the segment
  * predates the parent before returning null rows. The snapshot reader applies
  * deletes and predicates to Arrow batches and returns Spark columnar batches.
  * Each segment task receives the field IDs required by Spark projection and
  * predicates. MetadataColumns binds the hidden timestamp to stored field 1 and
  * supplies partition, segment and row position from each task. When Spark
  * projects `_milvus_row_id` on a physical read, planning requires a positive
  * snapshot ID and the reader combines it with the stored position. Operational
  * read metrics distinguish decoded rows, mutually exclusive deletion and
  * predicate exclusions, and rows handed to Spark after a pushed limit. Reader
  * state records exhaustion, early stops and failures once; planning reports
  * the current segment count without accumulating replans.
  */
package object read
