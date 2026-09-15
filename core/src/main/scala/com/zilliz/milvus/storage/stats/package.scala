package com.zilliz.milvus.storage

/** Segment statistics: what a written segment carries next to its data and what
  * a read prunes with.
  *
  * `PrimaryKeyStats` is the primary-key bloom filter Milvus keeps per segment
  * (`_stats/bloom_filter.<field id>/<log id>`, JSON), built by
  * `BlockedBloomFilter`, a bit-for-bit port of the blocked filter Milvus uses,
  * so a segment the connector writes carries the file Milvus would have
  * written. Reading the statistics back for pruning (R9, R10, R18) is not
  * written yet.
  *
  * Main types: PrimaryKeyStats, BlockedBloomFilter. Capabilities: none until
  * the pruning side lands; the ids this package is planned to carry are in
  * section 11 of docs/design/capabilities.md.
  */
package object stats
