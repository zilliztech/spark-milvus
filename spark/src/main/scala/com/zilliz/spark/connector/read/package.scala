package com.zilliz.spark.connector

/** ScanBuilder, Scan, Batch, serializable InputPartitions and the executor-side
  * readers: one row reader (`MilvusRowPartitionReader`), one columnar reader
  * (`MilvusColumnarPartitionReader`) and their `ColumnBinding`s. One
  * `MilvusInputPartition` carries the SegmentReadTask and parsed Milvus filter;
  * the task layout selects the column binding, without shipping the full option
  * object. V3 manifests and resolved physical inputs share the same row and
  * columnar execution path.
  *
  * `core.read.plan` turns the fixed Snapshot into one task per data segment and
  * `core.read.exec` opens the native reader and verifies any declared physical
  * row count. This package carries only the Spark interface: projection and
  * limit pushdown, metadata columns, task wrapping, and conversion of the same
  * Arrow batches to rows or ColumnarBatches. Delete-file descriptors travel in
  * the task and are materialized on the executor. Each final partition reader
  * also owns and closes the task's bounded Arrow child allocator. Ordinary
  * scans that read their primary key expose it through
  * `SupportsRuntimeV2Filtering`; repeated runtime filters intersect a cached
  * segment-level Bloom plan. Scans also bind `milvus.filter` fields to the
  * fixed Snapshot and evaluate its typed core expression in both reader outlets
  * without adding hidden fields to output.
  *
  * A NEAREST BY over a Milvus table searches the partitions this scan plans,
  * with the predicate Spark pushed into it; the search itself is the `search`
  * package. `SegmentSearchScan` allows another input format to supply those
  * fixed partitions and metadata without a public Snapshot facade;
  * `PhysicalColumnBinding` consumes its resolved Arrow names.
  */
package object read
