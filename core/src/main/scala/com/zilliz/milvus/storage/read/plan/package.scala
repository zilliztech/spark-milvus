package com.zilliz.milvus.storage.read

/** Partition planning. Pure JVM and serializable, so it can be built on the
  * driver and shipped to executors.
  *
  * `SegmentReadTask` says what one partition reads and `ReadPlan` collects them
  * with what planning already knows about the total. Both hold descriptions
  * only: a native handle is a pointer inside one process, so nothing that
  * cannot survive serialization can be a field, and the reader opens what it
  * needs on the executor. `ReadLimits` carries the validated batch row/byte
  * bounds and per-task Arrow allocation bound; raw user strings never cross
  * this boundary. `SegmentReadTask.provenPhysicalRows` separates a complete
  * physical row-address bound from the estimates used for search sizing.
  * Resolved physical column groups carry format-owned names and files through
  * the same task. `DeleteSource.Deferred` carries a serializable format
  * decoder; `DeletePlans` supplies its borrowed store on the executor.
  *
  * `ReadPlan.of` is the planning itself: one fixed `Snapshot` becomes one task
  * per V3 data segment, each with its `fs.*` map, pinned manifest version,
  * needed field ids, snapshot row count, exact persisted index builds and
  * applicable delete-file descriptors. `DeleteFileListing.of` opens V3
  * manifests on the driver only to list files; it never decodes delete rows.
  * The executor does that through `core.read.exec.DeletePlans`. What only Spark
  * knows stays in `spark.read`.
  *
  * `Partitioner` is not here. One segment per partition is the only strategy,
  * and a trait with a single implementation and no second caller would be
  * guesswork. The two shapes that would need one are reporting a Spark
  * partition per Milvus partition, whose worth is still open, and segment
  * selection.
  *
  * Main types: SegmentReadTask, ReadLimits, SegmentLayout, DeleteSource,
  * ReadPlan, DeleteFileListing.
  */
package object plan
