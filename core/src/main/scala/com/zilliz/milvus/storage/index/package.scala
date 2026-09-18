package com.zilliz.milvus.storage

/** Vector execution and its buffers, exclusions and per-segment top-k.
  *
  * Main types: SegmentSearch with ExactScan and IndexProbe, SearchPlan,
  * TopKMerger, QueryMatrix, KnowhereBuffers, PersistedIndexSearch,
  * BruteForceSearch, SegmentIndexQuery. SearchPlan cuts a search into (segment
  * set, query group) tasks and TopKMerger keeps each query's best k, in a task
  * and again in the Spark aggregation. KnowhereBuffers hands one Arrow batch of
  * a dense vector column to Knowhere, as it lies when the layout allows and
  * copied once otherwise. IndexWriter remains planned. A task loads its
  * segment's index once and closes it; there is no cross-task index cache.
  *
  * The planned shape (docs/design/architecture/vector-search.html sections
  * 1-2): this package keeps the computation only. SearchPlan splits the segment
  * tasks into segment sets and the query set into query groups.
  * SegmentIndexQuery's computing half becomes SegmentSearch, which runs one
  * query group over a segment set, one segment at a time, with one of two
  * strategies, ExactScan over vector batches or IndexProbe over an index
  * handle, and returns each query's merged candidates as (query, segment, row
  * offset, score). Opening segments, building exclusion bitmaps and reading and
  * decoding index files belong to the Milvus TableFormat side
  * (docs/design/architecture/table-version.html section 3); IndexFileCodec and
  * MilvusIndexFileDecoder now live in core.codec. TopKMerger keeps k per query
  * for batches and segments alike, KnowhereBuffers hands Arrow data buffers to
  * Knowhere as ByteBuffers, and IndexWriter builds a segment's index and hands
  * its BinarySet to the Milvus TableFormat side, which encodes and writes the
  * files (W6).
  *
  * Capabilities: V2, V5, V7 (see docs/design/capabilities.md).
  */
package object index
