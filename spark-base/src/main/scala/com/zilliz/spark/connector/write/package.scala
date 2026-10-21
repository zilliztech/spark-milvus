package com.zilliz.spark.connector

/** WriteBuilder, BatchWrite, DataWriterFactory and DataWriter, for the append
  * and backfill modes. ArrowWriter turns rows into Arrow batches; the batches
  * go to `core.write.exec`, which holds the native writer. Overwrite and
  * truncate are deliberately not implemented (section 10 of capabilities.md):
  * Spark refuses `mode("overwrite")` at analysis time and no data is touched.
  * The validated file-rolling byte limit is mapped to the upstream writer for
  * both storage lines.
  *
  * V3 driver coordination lives with `MilvusV3BatchWrite`, while executor
  * writers and their factory live with `MilvusV3PartitionWriter`.
  * `MilvusV3Writer` provides the direct DataFrame entry. `WriteSchema`
  * validates the Spark schema and builds its Arrow representation, using
  * canonical system field ids from `core.schema.SchemaMapper`. Staging paths
  * and their storage-root policy belong to `core.write.exec.StagingLayout`.
  *
  * Capabilities: W1, W2, G2 (see
  * docs/design/projects/spark-milvus/capabilities.md).
  */
package object write
