package com.zilliz.milvus.storage

/** Segment statistics: what a written segment carries next to its data and what
  * a read prunes with.
  *
  * `PrimaryKeyStats` is the primary-key bloom filter Milvus keeps per segment
  * (`_stats/bloom_filter.<field id>/<log id>`, JSON). Its BlockedBloomFilter is
  * an immutable query model; builders and JSON encoders live in external writer
  * modules and consume the shared hash, ordering and bit-layout rules.
  * `PrimaryKeyBloomPruner` reads explicitly listed statistics files and
  * StorageV3 manifest statistics, accepts Milvus's object and compound-array
  * encodings, and retains a segment whenever any input is unavailable or
  * unsupported. Its cached inputs also carry segment-level runtime filtering.
  * Row-group min/max pruning (R10) remains outside this implementation.
  *
  * Main types: PrimaryKeyStats, BlockedBloomFilter, PrimaryKeyFilter,
  * PrimaryKeyBloomPruner.
  */
package object stats
