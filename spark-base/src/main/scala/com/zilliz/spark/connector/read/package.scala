package com.zilliz.spark.connector

/** ScanBuilder, Scan, Batch, InputPartition, the columnar PartitionReader and
  * the ColumnVector implementations.
  *
  * Planning sits in the `plan` sub-package until `core.read.plan` exists; this
  * package is the Spark interface and the executor-side readers.
  *
  * Capabilities: R4, R5, R11, R12, R13, R16, R18 (see
  * docs/design/capabilities.md).
  */
package object read
