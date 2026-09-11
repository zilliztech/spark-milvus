package com.zilliz.milvus.storage

/** The predicate intermediate representation, the parser for Milvus expression
  * syntax, the column-batch evaluator and the printer that turns IR back into
  * Milvus syntax.
  *
  * Main types: Expr, PlanParser, Evaluator, ExprPrinter, Bitmap. Capabilities:
  * R6, R7, W5 (see docs/design/capabilities.md).
  */
package object expr
