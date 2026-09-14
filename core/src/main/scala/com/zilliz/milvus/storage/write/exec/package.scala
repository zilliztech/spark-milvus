package com.zilliz.milvus.storage.write

/** Writing segments out and the staging layout. This is where the write path
  * calls into the native layer.
  *
  * Main types: SegmentWriter, StagingLayout. Capabilities: none until code lands here; the ids this package is
  * planned to carry are in section 11 of docs/design/capabilities.md..
  */
package object exec
