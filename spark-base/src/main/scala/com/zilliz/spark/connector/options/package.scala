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
  * Numeric and Boolean user values go through one strict parser. Read batch and
  * task Arrow limits become a typed `ReadLimits` carried in every
  * `SegmentReadTask`; the write rolling limit is mapped once to the upstream
  * `writer.file_rolling.size` property. `milvus.filter` is parsed into a typed
  * core `Expr` on the driver; schema binding and execution belong to
  * `spark.read` and `core.expr`.
  *
  * A vector search's options and the executors it runs on come from here:
  * `SearchLimits` parses the `milvus.search.*` limits, and `TaskResources`
  * reads the executors a job has and declares, for a stage of Knowhere calls (a
  * search or an index build), that its tasks take every core of their executor
  * (docs/design/architecture/vector-search.html section 1.1). What one search
  * task may keep is the search's to derive (`search`'s `SearchResources`,
  * search-resources.html section 3.3).
  *
  * Index building (W6) takes no write option; its parameters belong to the
  * `build_index` procedure. No index or GPU session option exists yet (G4 is
  * declared unplaced in section 11 of capabilities.md).
  *
  * Capabilities: R16, G1, G2, G3 (see docs/design/capabilities.md).
  */
package object options
