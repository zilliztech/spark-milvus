package com.zilliz.spark.connector

/** The single Milvus-to-Spark type contract and its Arrow value conversion.
  * `SparkTypes.toStructField` owns type, nullability and Milvus field metadata.
  * Unsupported Milvus or Spark types fail instead of becoming binary or null.
  * Struct arrays retain exact member names when binding stored data and expose
  * one array of non-null structs. Each struct member's type and nullability
  * describe one element of its physical child column. MilvusStructArrayColumn
  * aligns sibling arrays by row and element offset. A batch with an added
  * nullable member uses the stored siblings' cardinality. Typed dense vector
  * arrays read each blob from its Arrow byte offset. ColumnVector
  * implementations expose Arrow batch values to Spark. ArrowValueTypes supplies
  * the type metadata and layout checks for writers. MilvusArrayColumn and
  * Utf8FromBinaryColumn cover the supported fields whose stored Arrow type
  * differs from their Spark type. `ArrowAllocator` keeps the process root alive
  * and creates a bounded child for each read task; the final partition reader
  * owns that child. `ConstantColumn` supplies one value for every row,
  * `RowOffsetColumn` supplies consecutive row positions, and
  * `SelectedRowsColumn` presents selected rows of another column, children
  * included, without copying its buffers. `RowIdentity` defines the hidden
  * `_milvus_row_id` column and its struct that binds a physical row to its
  * snapshot and stored position; `RowIdColumn` produces it for a batch.
  */
package object types
