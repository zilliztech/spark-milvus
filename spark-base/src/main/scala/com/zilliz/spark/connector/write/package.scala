package com.zilliz.spark.connector

/** WriteBuilder, BatchWrite, DataWriterFactory and DataWriter, plus the
  * truncate, overwrite and backfill modes. MilvusFieldData packs rows for the
  * gRPC insert writer.
  *
  * Capabilities: W1, W2, W4 (see docs/design/capabilities.md).
  */
package object write
