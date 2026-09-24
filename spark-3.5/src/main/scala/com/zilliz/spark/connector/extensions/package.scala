package com.zilliz.spark.connector

/** SparkSessionExtensions, the `CALL milvus.system.<name>(...)` parser
  * extension and the strategy that runs it. The grammar, the visitor, the
  * logical and physical nodes and the extension class are shared
  * (`spark-base`); this directory holds the parser antlr generates at this
  * line's version and `MilvusSqlParser`, the `ParserInterface` adapter, which
  * on Spark 3.5 has no `parseRoutineParam`. Design:
  * docs/design/architecture/procedure.html.
  *
  * The shared directory also holds `MilvusSparkPlugin`: set through
  * `spark.plugins`, it loads the native bundle on every executor at start, so
  * the first task does not wait for the extraction.
  *
  * Capabilities: A1, A2, A3, A4, A5 (see docs/design/capabilities.md).
  */
package object extensions
