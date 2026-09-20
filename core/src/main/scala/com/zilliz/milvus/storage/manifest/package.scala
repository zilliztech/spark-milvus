package com.zilliz.milvus.storage

/** One segment's manifest: column groups, delete files, statistics and index
  * registrations.
  *
  * Main types: SegmentManifestReader (the snapshot's Avro segment manifest),
  * SegmentManifestWriter (the same record, encoded), AvroManifestEntry and
  * AvroIndexFileEntry (data files, index files, exact build ids, row counts and
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
