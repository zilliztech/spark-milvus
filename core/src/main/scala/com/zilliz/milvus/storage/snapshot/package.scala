package com.zilliz.milvus.storage

/** Lists the snapshot directory, picks a snapshot and turns its JSON and Avro
  * into the read model.
  *
  * Main types: Snapshot, Segment, SegmentLayout, DeleteFiles,
  * SegmentStatistics, SnapshotCatalog, V2ColumnGroup, DeltaLogFile (what a V2
  * segment's layout, statistics and delete list are made of), CollectionIndex,
  * SegmentIndex and SegmentIndexes (definitions, exact persisted builds, and
  * whether a source supplied index metadata), the JSON shapes in `json`,
  * V2SegmentResolver (the hook through which compat materializes V2 packed
  * segments, since core does not depend on compat). SnapshotSource is the
  * common read entry point. The catalog decodes each snapshot Avro once and
  * shares its records with V2 footer recovery. `Snapshot.narrow` validates and
  * intersects the common partition and segment selectors; `retainDataSegments`
  * applies optimizer output. Both retain the applicable partition and
  * collection-wide L0 delete segments. See
  * docs/design/architecture/snapshot.html and vector-search.html section 2.4.
  *
  * `Snapshot` is the table description every source produces (decision 25):
  * open formats and user-declared schemas will produce it too, with
  * milvus-storage column groups as the unit layout. See
  * docs/design/architecture/table-description.html.
  *
  * Capabilities: R2, R3, R9, R13, R16, C3 (see docs/design/capabilities.md).
  */
package object snapshot
