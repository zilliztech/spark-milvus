package com.zilliz.milvus.storage

/** Vector execution and its buffers, exclusions and per-segment top-k.
  *
  * Main types: SegmentSearch with ExactScan and IndexProbe, SearchPlan,
  * TopKMerger, CandidateBytes, QueryMatrix, KnowhereBuffers. SearchPlan cuts a
  * search into one task per segment set and query range and TopKMerger keeps
  * each query's best k, in a task and again in the Spark aggregation.
  * KnowhereBuffers hands one Arrow batch of a dense vector column to Knowhere,
  * as it lies when the layout allows and copied once otherwise. IndexWriter
  * gathers a segment's vectors into one buffer, builds over it and hands over
  * what Knowhere serialized. A task loads its segment's index once and closes
  * it; there is no cross-task index cache.
  *
  * This package keeps the computation only (docs/design/architecture/
  * vector-search.html sections 1-2). SearchPlan splits the query set into query
  * groups and the segment tasks into segment sets, as many as the search runs
  * tasks at once, and chooses which side a task keeps: its queries, when they
  * fit the task's heap budget, or its segment set, sized to the task's off-heap
  * budget. SegmentSearch searches a segment set with one of two strategies,
  * ExactScan over vector batches or IndexProbe over an index handle (ExactScan
  * calls the batched distance entry for float32 fields, compacting excluded
  * rows out of a batch first, and Knowhere.bruteForce for the other element
  * types), and returns each query's merged candidates as (query, segment, row
  * offset, score), packed by TopKMerger into one CandidateBytes record per
  * query: SegmentSearch.runGroups reads each segment once for all the groups a
  * task keeps, and a task keeping its segment set holds it through
  * SegmentSearch.hold while the groups pass. Opening segments, building
  * exclusion bitmaps and reading and decoding index files belong to the Milvus
  * TableFormat side (docs/design/architecture/table-version.html section 3);
  * IndexFileCodec and MilvusIndexFileDecoder now live in core.codec. TopKMerger
  * keeps k per query for batches and segments alike and packs each query's best
  * k into CandidateBytes for the caller to send on, 24 bytes per candidate in
  * score order, which merges by walking two sorted lists; KnowhereBuffers hands
  * Arrow data buffers to Knowhere as ByteBuffers, and IndexWriter builds a
  * segment's index and hands its BinarySet to the Milvus TableFormat side,
  * which encodes and writes the files (W6).
  *
  * Capabilities: V2, V5, V7, W6 (see docs/design/capabilities.md).
  */
package object index
