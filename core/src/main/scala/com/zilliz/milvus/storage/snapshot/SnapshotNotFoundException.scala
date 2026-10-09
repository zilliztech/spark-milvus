package com.zilliz.milvus.storage.snapshot

/** Existing snapshot metadata contains no snapshot satisfying a requested name
  * or as-of selection. An empty metadata prefix deliberately uses a different
  * error path because it can also mean incorrect storage configuration.
  */
final class SnapshotNotFoundException(message: String)
    extends IllegalArgumentException(message)
