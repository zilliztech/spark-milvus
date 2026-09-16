package com.zilliz.milvus.storage

/** The predicate intermediate representation, the parser for Milvus expression
  * syntax and the column-batch evaluator.
  *
  * Main types: Expr, PlanParser, Evaluator, Bitmap. Capabilities: none until
  * code lands here; the ids this package is planned to carry are in section 11
  * of docs/design/capabilities.md.
  */
package object expr
