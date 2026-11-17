package com.zilliz.milvus.storage

/** Spark-free scalar predicate representations and Arrow-batch evaluation.
  * `PredicateExpr` and `PredicateEvaluator` implement the schema-bound Spark
  * DataSource V2 predicate contract. They preserve null through three-valued
  * logic and give the rows they exclude as an `ExclusionBitmap`.
  */
package object expr
