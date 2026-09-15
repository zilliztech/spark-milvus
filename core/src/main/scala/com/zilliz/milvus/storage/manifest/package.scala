package com.zilliz.milvus.storage

/** One segment's manifest: column groups, delete files, statistics and index
  * registrations.
  *
  * Main types: SegmentManifestReader (the snapshot's Avro segment manifest),
  * V3ManifestReader (milvus-storage's `_metadata/manifest-{n}.avro`); the
  * designed Manifest, ColumnGroup and ManifestReader are not written yet.
  * Capabilities: R5 (see
  * docs/design/capabilities.md).
  */
package object manifest
