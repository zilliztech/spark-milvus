package com.zilliz.milvus.storage

/** Lists the snapshot directory, picks a snapshot and turns its JSON and Avro
  * into the read model.
  *
  * Main types: Snapshot, Segment, SegmentLayout, DeleteFiles, SnapshotCatalog,
  * V2ColumnGroup, DeltaLogFile (what a V2 segment's layout and delete list are
  * made of), the JSON shapes in `json`, SnapshotSource, and V2SegmentResolver
  * (the hook through which compat materializes V2 packed segments, since core
  * does not depend on compat). `Snapshot.narrow` validates and intersects the
  * common partition and segment selectors while retaining applicable L0 delete
  * segments. See docs/design/architecture/snapshot.html. Capabilities: R2, R3,
  * R9, R13, R16, C3 (see docs/design/capabilities.md).
  */
package object snapshot
