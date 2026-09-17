package com.zilliz.milvus.storage

/** Vector execution and its buffers, exclusions and per-segment top-k.
  *
  * Main types: BruteForceSearch, PersistedIndexSearch, MilvusIndexFileDecoder,
  * SegmentIndexQuery. IndexWriter remains planned. A task loads its segment's
  * index once and closes it; there is no cross-task index cache.
  *
  * The planned shape (docs/design/architecture/vector-search.html sections
  * 1-2): SegmentIndexQuery becomes SegmentSearch, which runs one group of
  * queries over a set of segments, one segment at a time, with one of two
  * strategies, ExactScan or IndexProbe, and returns each query's merged
  * candidates as (query, segment, row offset, score). TopKMerger keeps k per
  * query for batches and segments alike, KnowhereBuffers hands Arrow data
  * buffers to Knowhere as ByteBuffers, and IndexWriter builds a segment's index
  * and encodes its files (W6).
  *
  * Capabilities: V2, V5, V7 (see docs/design/capabilities.md).
  */
package object index
