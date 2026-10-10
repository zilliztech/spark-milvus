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
  * The nearest-by join: the shared directory holds the node
  * `MilvusNearestByJoin`, the strategy that plans it, the nodes that execute it
  * over a Milvus table input and over a DataFrame input
  * (`MilvusNearestByJoinExec`, `MilvusNearestByJoinFrameExec`),
  * `SparkNearestBy`, what of the join stays Spark's, and `NearestByTakeover`,
  * which decides whether the connector executes a join. For the lines without
  * Spark's NearestByJoin it also holds `NearestByJoinRequest`, the node the
  * connector's `nearestByJoin` and the table function `nearest_by_join` build;
  * `ReplaceNearestByJoinRequest`, the rule that replaces it in the analyzer's
  * Post-Hoc Resolution batch; `GeneralNearestBy`, the plan of a join the
  * connector does not execute; and the vector functions `vector_l2_distance`,
  * `vector_cosine_similarity` and `vector_inner_product` (`VectorFunction`,
  * `VectorKernels`). This directory's `NearestByExtensions` registers the
  * functions, the table function and the rule, and `Datasets` builds a
  * DataFrame over a plan and a relation over rows with this line's
  * constructors. Design: docs/design/architecture/dataframe-api.html.
  *
  * Capabilities: A1, A2, A3, A4, A5, V5, V7, V9 (see
  * docs/design/capabilities.md).
  */
package object extensions
