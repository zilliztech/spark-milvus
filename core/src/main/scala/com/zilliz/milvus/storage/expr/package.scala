package com.zilliz.milvus.storage

/** Spark-free scalar predicate representations and Arrow-batch evaluation.
  *
  * `Expr`, `PlanParser` and `Evaluator` implement the supported R7 Milvus
  * syntax subset. `PredicateExpr`, `PredicateEvaluator` and `Bitmap` implement
  * the schema-bound R6 Spark V2 contract without changing R7 syntax semantics.
  * Both preserve null through three-valued logic before selecting rows.
  * Capabilities: R6, R7 (see docs/design/capabilities.md).
  */
package object expr
