package com.zilliz.spark.connector

/** Public read, write and storage option parsing. MilvusOption describes
  * snapshot inputs, read limits, read modes, vector decoding and the write
  * operation. Snapshot scans deliver column batches. A snapshot read returns
  * live rows by default and all stored rows in physical mode. Spark projections
  * select collection and metadata columns, including the row identity on
  * physical reads, and predicates select rows and partitions. StorageOptions
  * and HadoopStorageKeys resolve per-input storage settings. OptionParsing
  * provides strict shared value parsing; TaskResources describes executor
  * resources and stage-level task core allocation.
  */
package object options
