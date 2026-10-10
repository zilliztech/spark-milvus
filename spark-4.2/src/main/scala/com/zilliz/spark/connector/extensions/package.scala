package com.zilliz.spark.connector

/** SparkSessionExtensions, the `CALL milvus.system.<name>(...)` parser
  * extension and the strategy that runs it. The grammar, the visitor, the
  * logical and physical nodes and the extension class are shared
  * (`spark-base`); this directory holds the parser antlr generates at this
  * line's version and `MilvusSqlParser`, the `ParserInterface` adapter, which
  * on Spark 4.0 and later forwards `parseRoutineParam` too. Design:
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
  * which decides whether the connector executes a join. This directory holds
  * `NearestByExtensions`, which injects `ReplaceNearestByJoin`, the rule that
  * takes Spark 4.2's NearestByJoin over in the analyzer's Post-Hoc Resolution
  * batch; the line's `SparkNearestBy`: `RewrittenNearestBy` runs the replaced
  * join through Spark's own rewrite for the query rows the connector does not
  * search, and `SparkVectorFunction` calls Spark's vector function for the base
  * rows Knowhere is not given; and `Datasets`, which makes the relation over
  * those query rows. The shared request node, general plan and vector functions
  * serve the lines before 4.2 and are not installed here. Design:
  * docs/design/architecture/dataframe-api.html.
  *
  * Capabilities: A1, A2, A3, A4, A5, V5, V7, V9 (see
  * docs/design/capabilities.md).
  */
package object extensions
