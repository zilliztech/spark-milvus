package com.zilliz.spark.connector

/** Offline vector functions, nearest-by joins and native executor startup.
  * `MilvusReadSparkSessionExtensions` registers search functions and
  * strategies; `MilvusSparkPlugin` loads the native bundle on each executor.
  * Shared logical and physical search plans live here; explicit scala-shims
  * roots adapt Datasets, NearestByExtensions and NearestByEntry to each
  * supported Spark API. No management SQL parser or procedure is registered.
  *
  * Capabilities: V5, V7, V9 (see
  * docs/design/projects/spark-milvus/capabilities.md).
  */
package object extensions
