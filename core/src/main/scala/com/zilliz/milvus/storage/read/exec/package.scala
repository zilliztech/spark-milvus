package com.zilliz.milvus.storage.read

/** Batch reading. Owns the upstream milvus-storage reader objects in core.
  *
  * `SegmentReader` hands over one Arrow `VectorSchemaRoot` at a time and
  * `SegmentReaderRegistry` opens the right one for a segment's layout, so the
  * two layouts have one entry point instead of a branch per call site. The
  * three ownership rules — a handle never crosses serialization, a constructor
  * that throws releases what it took, `close()` is idempotent — are kept by
  * `NativeSegmentReader` and explained in
  * docs/design/architecture/storage-io.html section 3. The reader keeps
  * MilvusStorageProperties alive until it is destroyed, and closes an owned
  * manifest only after the reader has released its borrowed column groups. JNI
  * and native library loading belong to milvus-storage.
  *
  * `SegmentReader.take` retrieves sorted, unique physical row indices with a
  * per-call projection through the upstream reader's takeRecordBatchReaderScala
  * and `loon_take`. Its `TakeResult` owns unread batches independently of the
  * source reader; each returned root is owned by the caller. The upstream
  * binding frees the complete native result array. Sequential streams open only
  * on the first `next()` call.
  *
  * `ColumnBatch` is not here yet. Decision 12 settled that the columnar outlet
  * is worth building and decision 6 settled how vector columns are typed, but
  * the Spark-facing half of that is a Spark type and belongs in layer 3; what
  * crosses into core is still a `VectorSchemaRoot`.
  *
  * `DeletePlans` reads and closes a task's delete files (`DeleteSource.Files`)
  * on the executor; an unreadable file fails the task. The registry wraps both
  * Spark output modes in the same expected-row check: reaching EOF with a
  * different physical count fails instead of returning a short DataFrame.
  *
  * A vector search reads through this package too, and that is the Milvus
  * format side of it: `RowExclusions` decides which rows the deletes and the
  * filter take out, `SegmentVectors` hands out one batch at a time with that
  * bitmap and the batch's first row offset, and `SegmentIndexHandle` opens the
  * index a snapshot pinned, over the index families the connector loads and the
  * element type the column carries. `IndexRowMapping` says what an index label
  * means in the segment, which differs from the row number when the column has
  * nulls. `core.index` computes on what these hand over and opens nothing
  * itself (docs/design/architecture/vector-search.html sections 2.3 and 2.4).
  *
  * `SegmentReader.metrics` is what the read cost on the crossing, as
  * `ReadMetrics`: calls and time, batches and bytes, the C side's copies, the
  * allocator's peak (G5; docs/design/architecture/storage-io.html section 5).
  *
  * Main types: SegmentReader, SegmentReaderRegistry, DeletePlans, ReadMetrics,
  * RowExclusions, SegmentVectors, SegmentIndexHandle, IndexRowMapping.
  * Capabilities: R3, R4, R8, R14, R17, G3, G5 (see
  * docs/design/capabilities.md). R4 and R17 name the columnar outlet; R17 is
  * partial: batches leave as `VectorSchemaRoot` and become Spark
  * `ColumnarBatch` in layer 3, while the raw-address signature for native
  * consumers (vector buffer address plus bitmap) does not exist yet. G3 maps
  * typed row/byte batch limits to the upstream reader; the Spark reader owns a
  * bounded child allocator that outlives every imported batch. Design:
  * docs/design/architecture/read.html section 5.2.
  */
package object exec
