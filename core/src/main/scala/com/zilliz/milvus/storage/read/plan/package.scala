package com.zilliz.milvus.storage.read

/** Partition planning. Pure JVM and serializable, so it can be built on the
  * driver and shipped to executors.
  *
  * `SegmentReadTask` says what one partition reads and `ReadPlan` collects them with
  * what planning already knows about the total. Both hold descriptions only: a
  * native handle is a pointer inside one process, so nothing that cannot
  * survive serialization can be a field, and the reader opens what it needs on
  * the executor.
  *
  * `Partitioner` is not here yet. One segment per partition is the only
  * strategy, and a trait with a single implementation and no second caller
  * would be guesswork. The two shapes that would need one are reporting a Spark
  * partition per Milvus partition, whose worth is decision 19 in section 4 of
  * docs/design/README.md and still open, and segment selection.
  *
  * Main types: SegmentReadTask, SegmentLayout, DeleteSource, ReadPlan. Capabilities:
  * R3, R5, R10 (see docs/design/capabilities.md). Design:
  * docs/design/architecture/read.html section 5.1.
  */
package object plan
