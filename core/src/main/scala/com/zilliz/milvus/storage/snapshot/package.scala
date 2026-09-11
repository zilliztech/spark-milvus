package com.zilliz.milvus.storage

/** Lists the snapshot directory, picks a snapshot, parses its JSON and Avro
  * into objects, and dispatches to the registered SnapshotSource.
  *
  * Main types: SnapshotCatalog, Snapshot, Segment, SnapshotSource,
  * SnapshotSourceRegistry. Capabilities: R2, R3, R9, R13, R16, C3 (see
  * docs/design/capabilities.md).
  */
package object snapshot
