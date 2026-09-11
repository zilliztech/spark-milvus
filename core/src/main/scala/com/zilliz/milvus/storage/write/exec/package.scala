package com.zilliz.milvus.storage.write

/** Writing segments out and the staging layout. This is where the write path
  * calls into the native layer.
  *
  * Main types: SegmentWriter, StagingLayout. Capabilities: W1, W2, G2 (see
  * docs/design/capabilities.md).
  */
package object exec
