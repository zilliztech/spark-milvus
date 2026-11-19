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
  * an address to one write, and `WriteResultTable` reads a receipt back. A
  * conditional create of `_operation.json` claims the address before tasks
  * start, and `_result.json` is also published by conditional creation. An
  * ambiguous create failure is resolved by reading the complete record back.
  * Unsupported conditional writes fail without falling back to an existence
  * check. Every record is uploaded by one request, so a failed upload leaves no
  * partial record. Addresses claimed by the earlier manifest protocol remain
  * occupied, and a claim also creates a guard record at that protocol's claim
  * key so that connectors using it reject the address in turn.
  *
  * Operational metrics distinguish rows accepted by a task attempt, complete
  * files adopted by Spark, confirmed segment manifest revisions and a confirmed
  * published receipt. The driver owns durable-result accounting and reports
  * partial and unknown commits without treating task output as a committed
  * replacement. Task counters describe attempt work, including retries, and
  * failure reporting preserves the original write exception. Profile-specific
  * `DriverMeasuredWrite` exposes driver write metrics to Spark 4 SQL execution;
  * Spark 3.5 exposes those outcomes through the optional operational metrics
  * plugin because its write interface has no callback.
  *
  * Nothing here appends rows, calls a Milvus service, registers a revision or
  * creates a snapshot; callers own job registration and online visibility.
  */
package object write
