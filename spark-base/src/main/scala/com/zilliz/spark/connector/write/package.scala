package com.zilliz.spark.connector

/** WriteBuilder, BatchWrite, DataWriterFactory and DataWriter, plus the
  * truncate, overwrite and backfill modes. Rows become Arrow batches here; the
  * batches go to `core.write.exec`, which holds the native writer.
  *
  * Capabilities: W1, W2, W4 (see docs/design/capabilities.md).
  */
package object write
