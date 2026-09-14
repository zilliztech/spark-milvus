package com.zilliz.spark.connector

/** Arrow type to Spark type mapping, and how vector columns surface in Spark.
  *
  * ArrowConverter turns Arrow values into InternalRow and back for the row
  * readers and the writers; the ColumnVector implementations here are the
  * columnar route that replaces its read half.
  *
  * Capabilities: R15 (see docs/design/capabilities.md).
  */
package object types
