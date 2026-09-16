package com.zilliz.milvus.storage.read

/** Batch reading. The only place in core that opens a native handle.
  *
  * `SegmentReader` hands over one Arrow `VectorSchemaRoot` at a time and
  * `SegmentReaderRegistry` opens the right one for a segment's layout, so the
  * two layouts have one entry point instead of a branch per call site. The
  * three ownership rules — a handle never crosses serialization, a constructor
  * that throws releases what it took, `close()` is idempotent — are kept by
  * `NativeSegmentReader` and explained in
  * docs/design/architecture/storage-io.html section 3.
  *
  * `SegmentReader.take` retrieves sorted, unique physical row indices with a
  * per-call projection through `loon_take`. Its `TakeResult` owns unread
  * batches independently of the source reader; each returned root is owned by
  * the caller. Sequential streams open only on the first `next()` call.
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
  * `SegmentReader.metrics` is what the read cost on the crossing, as
  * `ReadMetrics`: calls and time, batches and bytes, the C side's copies, the
  * allocator's peak (G5; docs/design/architecture/storage-io.html section 5).
  *
  * Main types: SegmentReader, SegmentReaderRegistry, DeletePlans, ReadMetrics.
  * Capabilities: R3, R4, R8, R14, R17, G3, G5 (see
  * docs/design/capabilities.md). R4 and R17 name the columnar outlet and G3 the
  * off-heap budget; the batch pull they both sit on is what exists today.
  * Design: docs/design/architecture/read.html section 5.2.
  */
package object exec
