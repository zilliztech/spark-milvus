package com.zilliz.spark.connector

/** The single Milvus-to-Spark type contract and its Arrow value conversion.
  *
  * `SparkTypes.toStructField` owns type, nullability and Milvus field metadata.
  * Unsupported Milvus or Spark types fail instead of becoming binary or null.
  * ArrowConverter turns supported values into InternalRow and back for row
  * readers and writers; the ColumnVector implementations consume the same
  * batches on the columnar route. MilvusArrayColumn and Utf8FromBinaryColumn
  * cover the supported fields whose stored Arrow type differs from their Spark
  * type.
  *
  * Capabilities: R15 (see docs/design/capabilities.md).
  */
package object types
