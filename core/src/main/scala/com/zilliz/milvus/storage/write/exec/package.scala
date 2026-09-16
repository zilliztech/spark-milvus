package com.zilliz.milvus.storage.write

/** Writing segments out. The only place in core that opens a native writer.
  *
  * `SegmentWriter` takes Arrow batches and hands them to milvus-storage:
  * `V3SegmentWriter` writes column groups under a segment base path and
  * `finish()` returns them as `WrittenColumnGroups`, which
  * `ManifestTransaction` appends to the segment's manifest or swaps in for
  * existing columns (backfill); `V2SegmentWriter` writes one parquet file per
  * column group at paths the caller names, the layout of a `storage_version =
  * 2` segment. `StagingLayout` is where an append writes before the job is
  * committed and registered: `{root}/staging/{job}/`, outside `insert_log/`,
  * which DataCoord garbage-collects.
  *
  * The rules a writer keeps are the ones the reader keeps
  * (docs/design/architecture/storage-io.html section 3): a handle never crosses
  * serialization, a constructor that throws releases what it took, `close()` is
  * idempotent. One rule is the writer's own: a batch handed to `write` is
  * exported through the Arrow C Data Interface and the C++ writer keeps
  * referring to its buffers until it flushes, so the caller builds a fresh
  * `VectorSchemaRoot` per batch and never reuses one.
  *
  * `ColumnGroupSplit` is the rule Milvus applies when it splits a segment's
  * columns into column groups, as the patterns of milvus-storage's
  * `schema_based` writer policy; a segment the connector writes is laid out the
  * way Milvus would have laid it out.
  *
  * Main types: SegmentWriter, V3SegmentWriter, V2SegmentWriter,
  * WrittenColumnGroups, ManifestTransaction, StagingLayout, ColumnGroupSplit.
  * `SegmentWriter.metrics` is what the write cost on the crossing, as
  * `WriteMetrics` (G5). Capabilities: W1, W2, G5 (see
  * docs/design/capabilities.md). Design: docs/design/README.md section 2.4.
  */
package object exec
