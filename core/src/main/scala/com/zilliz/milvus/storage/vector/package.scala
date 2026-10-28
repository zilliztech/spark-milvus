package com.zilliz.milvus.storage

/** Vector execution and its buffers, exclusions and per-segment top-k. Main
  * types: SegmentSearch with ExactScan and IndexSearch, SearchPlan, TopKMerger,
  * CandidateBytes, CandidateDictionary, CandidateEncoding, CandidateRuns,
  * QueryMatrix, KnowhereBuffers, MachineResources, RankingFunction and
  * EngineRange. SearchPlan cuts a search into one task per segment set and
  * query range; TopKMerger keeps each query's best k in a task, and
  * CandidateRuns batches the tasks' sorted answers for bounded multiway merging
  * with CandidateBytes or CarriedCandidates. MachineResources reads this
  * machine's memory limit from cgroup and /proc/meminfo, which local mode plans
  * against. KnowhereBuffers hands one Arrow batch of a dense vector column to
  * Knowhere, as it lies when the layout allows and copied once otherwise. A
  * task loads its segment's index once and closes it; there is no cross-task
  * index cache. IndexSearch.Pipeline overlaps collecting one group's answer
  * with the next group's search, and SegmentSearch.Prefetcher opens the next
  * segment's source on one background thread inside the task; neither survives
  * the task.
  *
  * This package keeps the computation only. The format supplies owned
  * VectorBatch.Stream and IndexSearch.Target values; the computation borrows
  * their batches and buffers until its work drains, then closes what it owns.
  * SearchPlan takes resolved segment identities and row counts, never read
  * tasks or snapshot metadata. SearchPlan splits the query set into query
  * groups and the segment tasks into segment sets, as many as the search runs
  * tasks at once, and chooses which side a task keeps: its queries, when they
  * fit the task's heap budget, or its segment set, sized to the task's off-heap
  * budget. SegmentSearch searches a segment set with one of two strategies,
  * ExactScan over vector batches or IndexSearch over an index handle (ExactScan
  * calls the batched distance entry for float32 fields, compacting excluded
  * rows out of a batch first, and Knowhere.bruteForce for the other element
  * types), and returns each query's merged candidates as (query, segment, row
  * offset, score), packed by TopKMerger into one CandidateBytes record per
  * query: SegmentSearch.runGroups reads each segment once for all the groups a
  * task keeps, and a task keeping its segment set holds it through
  * SegmentSearch.hold while the groups pass. Opening segments, building
  * exclusion bitmaps and reading and decoding index files belong to the Milvus
  * TableFormat side; IndexFileCodec and MilvusIndexFileDecoder now live in
  * core.codec. TopKMerger keeps k per query for batches and segments alike and
  * packs each query's best k into CandidateBytes for the caller to send on in
  * score order. CandidateBytes fixes 12 bytes per candidate for the whole
  * shuffle, using UInt32 segment ordinals and row offsets with Float32 scores.
  * Scores are rounded before local top-k selection, using the same order at
  * every merge. CandidateDictionary gives all tasks one signed-Long segment
  * order; CandidateEncoding holds only one task's mapping and rejects row
  * offsets outside UInt32 before encoding. Neither belongs to CandidateRuns
  * merge state. CandidateHeads selects from the heads of multiple sorted runs,
  * with two-run merges retaining a direct walk; KnowhereBuffers hands Arrow
  * data buffers to Knowhere as ByteBuffers.
  *
  * A search can be ranked by a RankingFunction, the vector function of a
  * nearest-by join. EngineRange says which vectors Knowhere scores as that
  * function computes in float; an exact scan reads each batch once to classify
  * its rows, hands Knowhere the rows in range and has the function score the
  * rest against every query of the group, into the same TopKMerger, and an
  * indexed search ranks NaN and infinite scores instead of failing on them.
  *
  * ExactScan and EngineRange take a VectorBatch, the format-neutral form of a
  * batch: a buffer of vectors, its exclusions and where it starts in its unit.
  * A Milvus segment's batches are SegmentVectors'; a DataFrame input's batches
  * borrow the buffer FrameSearch fills directly, a candidate's place being the
  * base partition and the row's position in it, both checked as UInt32 before
  * CarriedCandidates packs each query's best k with the rows they came from,
  * since such an input has no table to take them from after the merge.
  */
package object vector
