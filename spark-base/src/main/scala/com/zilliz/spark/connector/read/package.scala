package com.zilliz.spark.connector

/** ScanBuilder, Scan, Batch, InputPartition and the executor-side readers: one
  * row reader for both storage lines (`MilvusRowPartitionReader`), one columnar
  * reader (`MilvusColumnarPartitionReader`), the `ColumnBinding` that names
  * columns per line and says which the query needs, the optional vector-search
  * stage (`SegmentVectorSearch`) and the ColumnVector implementations. Planning
  * is `core.read.plan`'s and opening a segment is `core.read.exec`'s; this
  * package is the Spark interface only.
  *
  * Capabilities: R4, R5, R11, R12, R13, R16, R18 (see
  * docs/design/capabilities.md).
  */
package object read
