package com.zilliz.spark.connector

/** Option names, parsing, validation and driver storage configuration.
  *
  * `ReadMode` selects the configured read source; `spark.table` composes and
  * resolves that source into a Snapshot. `StorageOptions` turns `fs.*` plus
  * supported Hadoop and S3 aliases into a bucket, endpoint and `ObjectStore`,
  * and scopes each driver's store use through `withStore`. Milvus-only
  * endpoint-style paths are recognized only at metadata boundaries.
  *
  * Numeric and Boolean user values go through one strict parser. Read batch and
  * task Arrow limits become a typed `ReadLimits` carried in every
  * `SegmentReadTask`; the write rolling limit is mapped once to the upstream
  * `writer.file_rolling.size` property. `milvus.filter` is parsed into a typed
  * core `FilterExpr` on the driver; schema binding and execution belong to
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
