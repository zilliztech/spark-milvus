package com.zilliz.milvus.storage

/** Resolves a fixed Storage V3 snapshot file into the read model. Main types:
  * Snapshot, Segment, SegmentLayout, DeleteFiles, SegmentStatistics,
  * SnapshotCatalog, DeltaLogFile, CollectionIndex, SegmentIndex and
  * SegmentIndexes. SnapshotSource resolves the input once; SnapshotCatalog
  * validates its JSON and segment Avro records, including persisted index
  * identity and file locations. Every V3 location must have a segment Avro
  * record, and a nonempty segment_ids declaration must match those records. V3
  * manifest references carry a nonnegative 64-bit version; version zero
  * describes the initial empty state, and latest-version references are not
  * fixed snapshots. It requires snapshot format version 4 or 6, including for
  * empty snapshots. Legacy storage formats are rejected before planning.
  * Segment roles and source channels survive materialization; L0 segments
  * remain delete sources through scan pruning. The Milvus-allocated snapshot id
  * and the collection schema version are kept, so that a row identity can be
  * bound to its snapshot and a write can report the schema it was based on.
  * Snapshot requires unique ids for top-level fields, struct parents and their
  * stored members, and accepts indexes on stored member ids, before readers or
  * column replacement can resolve fields by name. SnapshotCatalog resolves the
  * caller's metadata location for both the file read and the write planner's
  * storage prefix check. The optional persisted layout distinguishes referenced
  * segment data from a self-contained export. Older snapshots may omit the
  * layout.
  *
  * Snapshot.retainDataSegments keeps the data segments selected by the scan
  * planner and the delete sources that apply to those segments.
  * PhysicalColumnGroups carries already-resolved files and column names for
  * consumers of the low-level reader, without compatibility interpretation.
  */
package object snapshot
