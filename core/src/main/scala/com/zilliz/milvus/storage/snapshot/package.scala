package com.zilliz.milvus.storage

/** Lists the snapshot directory, picks a snapshot and turns its JSON and Avro
  * into the read model.
  *
  * Main types: Snapshot, Segment, SegmentLayout, DeleteFiles, SnapshotCatalog,
  * V2SegmentResolver (the hook through which compat materializes V2 packed
  * segments, since core does not depend on compat). SnapshotSource and its
  * registry are not written yet; see docs/design/architecture/snapshot.html. Capabilities: R2, R3, R9, R13, R16, C3 (see
  * docs/design/capabilities.md).
  */
package object snapshot
