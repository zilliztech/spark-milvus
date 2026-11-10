package com.zilliz.spark.connector

/** Converts Spark values and resolved Milvus schemas to Arrow column batches,
  * and replaces stored columns of a fixed V3 snapshot from a DataFrame.
  *
  * The column replacement is a DataSource V2 batch append: `ColumnReplace`
  * plans it on the driver and asks Spark to cluster rows by the segment of
  * their `_milvus_row_id` and order them by offset; `ColumnReplaceDataWriter`
  * writes each segment's new column files in tasks, checking that every stored
  * row arrives exactly once; the driver commits every segment's manifest from
  * the snapshot's pinned revision and publishes the receipt. `WriteResult` owns
  * the records under the write result address, including the claim that gives
  * an address to one write, and `WriteResultTable` reads a receipt back.
  *
  * Nothing here appends rows, calls a Milvus service, registers a revision or
  * creates a snapshot; callers own job registration and online visibility.
  */
package object write
