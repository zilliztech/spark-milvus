package com.zilliz.milvus.storage

/** What one segment holds: column groups, delete files, statistics and index
  * registrations, read from the two files that describe a segment.
  *
  * A Milvus snapshot lists one Avro record per segment (Milvus's
  * `ManifestEntry` in `internal/snapshotio/snapshot.go`, reached from the
  * snapshot JSON's `manifest_list`); it is Milvus metadata and only the
  * snapshot-directory read path has it. milvus-storage writes its own
  * `_metadata/manifest-{n}.avro` inside every V3 segment directory. The word
  * "manifest" in this package names the milvus-storage file only; the
  * snapshot's record is a snapshot segment record.
  *
  * Main types: SnapshotSegmentReader (the snapshot's Avro segment record),
  * SnapshotSegmentWriter (the same record, encoded), SnapshotSegmentEntry and
  * IndexFileEntry (data files, index files, exact build ids, row counts and
  * separate index format/path versions), V3ManifestReader (milvus-storage's
  * `_metadata/manifest-{n}.avro`); the designed Manifest, ColumnGroup and
  * ManifestReader are not written yet. The reader takes the prefix of the
  * schema it needs; the writer fills the whole record, because Avro binary is
  * positional and Milvus reads every field. Schema versions 1 to 5 are bundled;
  * version 5 (Milvus v3.0.2) adds `manifest_has_index`, which says the
  * segment's own manifest registers the index, so a segment that names no index
  * files there is unknown rather than unindexed. Capabilities: R5, W8 (see
  * docs/design/capabilities.md).
  */
package object manifest
