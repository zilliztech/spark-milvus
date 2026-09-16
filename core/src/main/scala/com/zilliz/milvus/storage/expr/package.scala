package com.zilliz.milvus.storage

/** The predicate intermediate representation, the parser for Milvus expression
  * syntax and the column-batch evaluator.
  *
  * Main types: Expr, PlanParser. The scalar subset rejects unsupported syntax
  * and preserves null through three-valued logic before selecting rows.
  * Capabilities: R7 (see docs/design/capabilities.md).
  */
package object expr
