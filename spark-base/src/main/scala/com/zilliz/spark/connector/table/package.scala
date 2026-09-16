package com.zilliz.spark.connector

/** MilvusTable: schema, capability set and metadata columns. Row deletion
  * through Spark SQL is deliberately not implemented (section 10 of
  * capabilities.md); the table does not implement SupportsDeleteV2.
  *
  * Capabilities: R12 (see docs/design/capabilities.md).
  */
package object table
