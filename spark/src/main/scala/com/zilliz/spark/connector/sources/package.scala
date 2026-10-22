package com.zilliz.spark.connector

/** The `format("milvus")` entry point: `MilvusDataSource`, the TableProvider
  * Spark finds through `DataSourceRegister`.
  *
  * It stays at this fully qualified name because apps and user jobs name it by
  * the string `com.zilliz.spark.connector.sources.MilvusDataSource`. Everything
  * else that used to live here is in `table`, `read` and `options`.
  *
  * Capabilities: R1 (see docs/design/projects/spark-milvus/capabilities.md).
  */
package object sources
