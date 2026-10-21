package com.zilliz.milvus.storage.write

/** Builders and file encoders for primary-key statistics of written segments.
  * Read models and their hash, comparison and bit-layout rules remain in
  * core.stats. Builders return independent immutable read models.
  *
  * Main types: PrimaryKeyStatsBuilder, PrimaryKeyStatsEncoder,
  * BlockedBloomFilterBuilder, BlockedBloomFilterEncoder. Capabilities: W3 (see
  * docs/design/projects/spark-milvus/capabilities.md).
  */
package object stats
