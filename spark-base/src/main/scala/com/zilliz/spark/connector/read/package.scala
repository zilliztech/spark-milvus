package com.zilliz.spark.connector

/** ScanBuilder, Scan, Batch, serializable InputPartitions and the executor-side
  * readers: one row reader for both storage lines (`MilvusRowPartitionReader`),
  * one columnar reader (`MilvusColumnarPartitionReader`) and their
  * `ColumnBinding`s.
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
  * `MilvusSearch` runs the two-stage search over the segments of a Milvus table
  * input: it plans the segment sets and query groups through
  * `core.index.SearchPlan`, delivers the query set either broadcast whole or
  * packed by group into a shuffle that `SearchQueryRanges` reads one group at a
  * time (both through `SearchQueries`), runs the first stage
  * (`SegmentSetSearch`), merges every query's top-k (`CandidateBytes.merge`
  * over an RDD shuffle by query id) and reads the base columns of the rows that
  * survived (`SearchTake`, which hands rows on as `InternalRow`s). The
  * nearest-by join over a Milvus table runs those two stages
  * (`MilvusSearch.execute`) through `NearestBySearch`, the one way a search is
  * written: it searches the query rows whose vector Knowhere scores as Spark's
  * function does, hands the others to Spark's own execution of the join, and
  * gives each hit its query row back by number -- from a broadcast when the
  * driver holds the rows, by a join after the take stage when they stay on the
  * executors. The search reads the partitions of the Milvus scan the optimizer
  * left, with the predicate Spark pushed into it
  * (docs/design/architecture/dataframe-api.html sections 2 and 4). A base that
  * is not such a scan is a DataFrame input (`FrameSearch`): every pair of a
  * base partition and a query group is one task, which reads the partition a
  * block at a time into float32 batches for the exact scan and keeps the rows
  * some query's heap holds; the candidates carry those rows through the merge
  * (`CarriedCandidates`), so there is no take stage. `NearestBySearch` handles
  * the query rows the same way for both inputs; a DataFrame input's dimension
  * is the commonest length among the query vectors it could search.
  * Capabilities: R4, R5, R7, R11, R12, R13, R16, R18, V5, V7, V9, G3 (see
  * docs/design/capabilities.md).
  */
package object read
