package com.zilliz.spark.connector

/** ScanBuilder, Scan, Batch, serializable InputPartitions and the executor-side
  * readers: one row reader for both storage lines (`MilvusRowPartitionReader`),
  * one columnar reader (`MilvusColumnarPartitionReader`), their
  * `ColumnBinding`s and the optional vector-search stage
  * (`SegmentVectorSearch`).
  *
  * `core.read.plan` turns the fixed Snapshot into one task per data segment and
  * `core.read.exec` opens the native reader and verifies any declared physical
  * row count. This package carries only the Spark interface: projection and
  * limit pushdown, metadata columns, task wrapping, and conversion of the same
  * Arrow batches to rows or ColumnarBatches. Delete-file descriptors travel in
  * the task and are materialized on the executor.
  *
  * Capabilities: R4, R5, R11, R12, R13, R16, R18 (see
  * docs/design/capabilities.md).
  */
package object read
