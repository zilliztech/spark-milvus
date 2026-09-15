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
  * `ColumnBatch` and `take` are not here yet. Decision 12 settled that the
  * columnar outlet is worth building and decision 6 settled how vector columns
  * are typed, but the Spark-facing half of that is a Spark type and belongs in
  * layer 3; what crosses into core is still a `VectorSchemaRoot`.
  *
  * `DeletePlans` reads a task's delete files (`DeleteSource.Files`) into the
  * plan the reader applies; a file that cannot be read fails the task.
  *
  * Main types: SegmentReader, SegmentReaderRegistry, DeletePlans. Capabilities:
  * R3, R4, R14, R17, G3 (see docs/design/capabilities.md). R4 and R17 name the
  * columnar outlet and G3 the off-heap budget; the batch pull they both sit on
  * is what exists today. Design: docs/design/architecture/read.html section
  * 5.2.
  */
package object exec
