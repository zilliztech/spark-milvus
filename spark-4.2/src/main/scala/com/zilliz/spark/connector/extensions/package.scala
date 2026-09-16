package com.zilliz.spark.connector

/** SparkSessionExtensions, the `CALL milvus.system.<name>(...)` parser
  * extension and the strategy that runs it. The grammar, the visitor, the
  * logical and physical nodes and the extension class are shared
  * (`spark-base`); this directory holds the parser antlr generates at this
  * line's version and `MilvusSqlParser`, the `ParserInterface` adapter, which
  * on Spark 4.0 and later forwards `parseRoutineParam` too. Design:
  * docs/design/architecture/procedure.html.
  *
  * Capabilities: A4 (see docs/design/capabilities.md).
  */
package object extensions
