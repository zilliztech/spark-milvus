package com.zilliz.milvus.storage.read

/** Partition planning. Pure JVM and serializable, so it can be built on the
  * driver and shipped to executors.
  *
  * `SegmentReadTask` says what one partition reads and `ReadPlan` collects them
  * with what planning already knows about the total. Both hold descriptions
  * only: a native handle is a pointer inside one process, so nothing that
  * cannot survive serialization can be a field, and the reader opens what it
  * needs on the executor.
  *
  * `ReadPlan.of` is the planning itself: one fixed `Snapshot` becomes one task
  * per data segment, V3 first, each with its `fs.*` map, pinned manifest
  * version, needed field ids and applicable delete-file descriptors.
  * `DeleteFileListing.of` opens V3 manifests on the driver only to list files;
  * it never decodes delete rows. The executor does that through
  * `core.read.exec.DeletePlans`. What only Spark knows stays in `spark.read`.
  *
  * `Partitioner` is not here. One segment per partition is the only strategy,
  * and a trait with a single implementation and no second caller would be
  * guesswork. The two shapes that would need one are reporting a Spark
  * partition per Milvus partition, whose worth is decision 19 in section 4 of
  * docs/design/README.md and still open, and segment selection.
  *
  * Main types: SegmentReadTask, SegmentLayout, DeleteSource, ReadPlan,
  * DeleteFileListing. Capabilities: R3, R5, R8, R10, R13 (see
  * docs/design/capabilities.md). Design: docs/design/architecture/read.html
  * section 5.1.
  */
package object plan
