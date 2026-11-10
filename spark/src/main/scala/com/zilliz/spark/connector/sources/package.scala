package com.zilliz.spark.connector

/** The `format("milvus")` entry point: `MilvusDataSource`, the TableProvider
  * Spark finds through `DataSourceRegister`. It routes a load to a snapshot
  * table or a write result table and a write to a column replacement.
  *
  * It stays at this fully qualified name because apps and user jobs name it by
  * the string `com.zilliz.spark.connector.sources.MilvusDataSource`. Everything
  * else that used to live here is in `table`, `read` and `options`.
  */
package object sources
