package com.zilliz.milvus.storage

/** The smallest object-storage interface the format needs: open, list, exists,
  * stat, create. No rename and no delete.
  *
  * No core source outside `io.hadoop` mentions org.apache.hadoop, so swapping
  * the implementation does not change any core signature. The single
  * implementation lives in `io.hadoop` with hadoop-common declared provided, so
  * at runtime it uses the copy Spark ships. Executors receive a serializable
  * ObjectStoreFactory, never a live Hadoop Configuration.
  *
  * Main types: ObjectStore, ObjectStoreFactory, FileInfo, SeekableInput.
  * Capabilities: R2, R3, W1 (see docs/design/capabilities.md).
  */
package object io
