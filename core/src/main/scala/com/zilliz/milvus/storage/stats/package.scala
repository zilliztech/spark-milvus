package com.zilliz.milvus.storage

/** Segment statistics: what a written segment carries next to its data and what
  * a read prunes with.
  *
  * `PrimaryKeyStats` is the primary-key bloom filter Milvus keeps per segment
  * (`_stats/bloom_filter.<field id>/<log id>`, JSON), built by
  * `BlockedBloomFilter`, a bit-for-bit port of the blocked filter Milvus uses,
  * so a segment the connector writes carries the file Milvus would have
  * written. `PrimaryKeyBloomPruner` reads both StorageV2 `statslog_files` and
  * StorageV3 manifest statistics, accepts Milvus's object and compound-array
  * encodings, and retains a segment whenever any input is unavailable or
  * unsupported. Its cached inputs also carry segment-level runtime filtering.
  * Row-group min/max pruning (R10) remains outside this implementation.
  *
  * Main types: PrimaryKeyStats, BlockedBloomFilter, PrimaryKeyFilter,
  * PrimaryKeyBloomPruner. Capabilities: R9, R18 (see
  * docs/design/capabilities.md).
  */
package object stats
