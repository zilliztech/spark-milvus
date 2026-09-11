package com.zilliz.milvus.storage.read

/** Batch reading, taking columns by row number, and the read outlet. This is
  * where core calls into the native layer.
  *
  * Main types: SegmentReader, SegmentReaderRegistry, ColumnBatch, Take.
  * Capabilities: R3, R4, R14, R17, G3 (see docs/design/capabilities.md).
  */
package object exec
