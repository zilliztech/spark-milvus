package com.zilliz.spark.connector

/** Option names, parsing and validation; wiring of compat implementations into
  * core.
  *
  * `ReadMode` decides which source resolves a read, `SnapshotReference` states
  * configured/latest/named/as-of selection, and `SnapshotSources` builds
  * exactly one `Snapshot`: the catalog, compat's `BackupSnapshotSource`,
  * `ClientSnapshotSource` or the option-string source. It owns and closes every
  * driver-side store after the snapshot has been materialized, then applies the
  * common `milvus.partitions` and `milvus.segments` selectors. `StorageOptions`
  * turns `fs.*` plus supported Hadoop and S3 aliases into a bucket, endpoint
  * and `ObjectStore`; Milvus-only endpoint-style paths are recognized only at
  * metadata boundaries.
  *
  * Capabilities: R16, W6, G1, G2, G3, G4 (see docs/design/capabilities.md).
  */
package object options
