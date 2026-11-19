package com.zilliz.spark.connector

/** Fixed V3 snapshots exposed as read-only Spark tables. `MilvusTables`
  * validates DataSource input, resolves one Snapshot and derives its schema;
  * `SnapshotSources` reads either a snapshot file or complete V3 metadata.
  * Stores close after metadata is materialized. The scan builder applies
  * partition predicates to the resolved snapshot. `MilvusTable` advertises
  * BATCH_READ only.
  *
  * Nonempty inputs require a complete Milvus snapshot file, including its
  * segment Avro records for identity and L0 validation. Explicit schema options
  * describe empty snapshots only.
  *
  * Field identity, types, nullability and vector dimensions come from the
  * snapshot schema. The table advertises the partition ID, segment ID, row
  * offset and stored system timestamp as Spark metadata columns. A visible
  * collection field takes precedence over a metadata name. Physical reads of
  * identified snapshots also advertise the hidden snapshot-bound row identity
  * `_milvus_row_id`. No online service or catalog is consulted.
  */
package object table
