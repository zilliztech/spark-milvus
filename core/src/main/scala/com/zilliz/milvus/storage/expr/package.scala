package com.zilliz.milvus.storage

/** Spark-free scalar predicate representations and Arrow-batch evaluation.
  *
  * `FilterExpr`, `PlanParser` and `FilterEvaluator` implement the supported R7
  * Milvus filter subset (`milvus.filter`) for ordinary table filters and
  * vector-search filters. `PredicateExpr` and `PredicateEvaluator` implement
  * the schema-bound R6 Spark V2 predicate contract without changing R7 syntax
  * semantics. Both preserve null through three-valued logic and give the rows
  * they exclude as a `ExclusionBitmap`. Capabilities: R6, R7 (see
  * docs/design/projects/spark-milvus/capabilities.md).
  */
package object expr
