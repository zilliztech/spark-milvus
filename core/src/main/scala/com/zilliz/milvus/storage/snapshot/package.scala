package com.zilliz.milvus.storage

/** Resolves a fixed Storage V3 snapshot file into the read model. Main types:
  * Snapshot, Segment, SegmentLayout, DeleteFiles, SegmentStatistics,
  * SnapshotCatalog, DeltaLogFile, CollectionIndex, SegmentIndex and
  * SegmentIndexes. SnapshotSource resolves the input once; SnapshotCatalog
  * validates its JSON and segment Avro records, including persisted index
  * identity and file locations. It requires snapshot format version 4 or 6,
  * including for empty snapshots. Legacy storage formats are rejected before
  * planning. Segment roles and source channels survive materialization; L0
  * segments remain delete sources through narrowing.
  *
  * SnapshotSelection validates partition and segment selections from neutral
  * identity descriptors; external format adapters share the same selection
  * rules. PhysicalColumnGroups carries already-resolved files and column names
  * for consumers of the low-level reader, without compatibility interpretation.
  */
package object snapshot
