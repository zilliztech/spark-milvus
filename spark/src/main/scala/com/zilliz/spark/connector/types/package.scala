package com.zilliz.spark.connector

/** The single Milvus-to-Spark type contract and its Arrow value conversion.
  * `SparkTypes.toStructField` owns type, nullability and Milvus field metadata.
  * Unsupported Milvus or Spark types fail instead of becoming binary or null.
  * ArrowConverter turns supported values into InternalRow for row readers.
  * ArrowValueTypes owns the metadata and layout checks shared with writers; the
  * ColumnVector implementations consume the same batches on the columnar route.
  * MilvusArrayColumn and Utf8FromBinaryColumn cover the supported fields whose
  * stored Arrow type differs from their Spark type. `ArrowAllocator` keeps the
  * process root alive and creates a bounded child for each read task; the final
  * partition reader owns that child. `ConstantColumn` supplies one value for
  * every row, `RowOffsetColumn` supplies consecutive row positions, and
  * `SelectedRowsColumn` presents selected rows of another column without
  * copying its buffers.
  */
package object types
