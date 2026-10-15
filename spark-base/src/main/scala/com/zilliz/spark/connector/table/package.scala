package com.zilliz.spark.connector

/** MilvusTables validates and resolves one fixed Snapshot for both DataSource
  * and Catalog loads, then constructs MilvusTable. MilvusTable owns that
  * Snapshot's schema, capability set and metadata columns, and
  * `MilvusTable.readOptions` the options one read of it runs with.
  * `SnapshotSources` composes the catalog, backup, client and option-string
  * sources; `SnapshotReference` states configured/latest/named/as-of selection.
  * It materializes metadata with driver stores scoped by
  * `StorageOptions.withStore`, then applies the common `milvus.partitions` and
  * `milvus.segments` selectors. `V2SegmentResolvers` supplies compat's footer
  * resolver to core without introducing a dependency from core to compat.
  * `MilvusTables.named` resolves a name Spark resolves (a catalog table or a
  * view) to the whole Milvus table it reads, for the procedures that take a
  * `table` (docs/design/projects/spark-milvus/architecture/dataframe-api.html
  * section 9).
  *
  * Field identity, Milvus type, nullability, key flags and vector dimensions
  * all come from the Snapshot schema. `_segment_id` and `_row_offset` are
  * synthesized read metadata; `_timestamp` is stored Milvus field id 1. Row
  * deletion through Spark SQL is deliberately not implemented (section 10 of
  * capabilities.md); the table does not implement SupportsDeleteV2.
  *
  * Capabilities: R12, R16 (see
  * docs/design/projects/spark-milvus/capabilities.md).
  */
package object table
