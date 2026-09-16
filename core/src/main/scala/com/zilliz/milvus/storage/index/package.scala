package com.zilliz.milvus.storage

/** Vector execution and its buffers, exclusions and per-segment top-k.
  *
  * Main types: BruteForceSearch, PersistedIndexSearch, MilvusIndexFileDecoder,
  * SegmentIndexQuery. IndexCache and IndexWriter remain planned; loaded indexes
  * are task-owned. Capabilities: V2, V5, V7 (see docs/design/capabilities.md).
  */
package object index
