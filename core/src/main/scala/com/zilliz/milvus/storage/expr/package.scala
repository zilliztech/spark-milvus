package com.zilliz.milvus.storage

/** The predicate intermediate representation, the parser for Milvus expression
  * syntax, the column-batch evaluator and the printer that turns IR back into
  * Milvus syntax.
  *
  * Main types: Expr, PlanParser, Evaluator, ExprPrinter, Bitmap. Capabilities:
  * none until code lands here; the ids this package is planned to carry are in
  * section 11 of docs/design/capabilities.md..
  */
package object expr
