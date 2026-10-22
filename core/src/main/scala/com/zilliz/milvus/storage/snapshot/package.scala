package com.zilliz.milvus.storage

/** Resolves a fixed Storage V3 snapshot file into the read model.
  *
  * Main types: Snapshot, Segment, SegmentLayout, DeleteFiles,
  * SegmentStatistics, SnapshotCatalog, DeltaLogFile, CollectionIndex,
  * SegmentIndex and SegmentIndexes. SnapshotSource resolves the input once;
  * SnapshotCatalog validates its JSON and segment Avro records, including
  * persisted index identity and file locations. Legacy storage formats are
  * rejected before planning.
  *
  * SnapshotSelection validates partition and segment selections from neutral
  * identity descriptors; external format adapters share the same selection
  * rules. PhysicalColumnGroups carries already-resolved files and column names
  * for consumers of the low-level reader, without compatibility interpretation.
  *
  * Capabilities: R2, R3, R9, R13, R16, C3 (see
  * docs/design/projects/spark-milvus/capabilities.md).
  */
package object snapshot
