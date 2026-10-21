package com.zilliz.milvus.storage.write

/** Business encoders for stored Milvus values and index objects. Readers use
  * core.codec models and decoders without depending on these encoders. The
  * current writer remains in this repository until the private migration.
  *
  * Main types: ArrayEncoder, SparseFloatVectorEncoder, BinlogEncoder,
  * IndexFileEncoder. Capabilities: W3, W6 (see
  * docs/design/projects/spark-milvus/capabilities.md).
  */
package object codec
