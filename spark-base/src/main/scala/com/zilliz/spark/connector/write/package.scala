package com.zilliz.spark.connector

/** WriteBuilder, BatchWrite, DataWriterFactory and DataWriter, for the append
  * and backfill modes. Rows become Arrow batches here; the batches go to
  * `core.write.exec`, which holds the native writer. Overwrite and truncate are
  * deliberately not implemented (section 10 of capabilities.md): Spark refuses
  * `mode("overwrite")` at analysis time and no data is touched.
  *
  * Capabilities: W1, W2 (see docs/design/capabilities.md).
  */
package object write
