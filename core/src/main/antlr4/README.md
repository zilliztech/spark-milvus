# core antlr4 sources

This directory intentionally contains no grammar. The R7 scalar `PlanParser` is
handwritten in `core.expr`; core is one artifact shared by Spark lines whose
antlr runtimes are not interchangeable. R6 uses the schema-bound
`PredicateExpr` and `PredicateEvaluator` in the same Spark-free package; it
does not parse Milvus expression strings or change Plan.g4 evaluation rules.

SQL extension grammars are different: they live under `spark-base`, are
generated once per Spark line, and use that line's Spark-provided antlr runtime.
The decision is recorded in section 6 of `docs/design/README.md` (2026-09-10).
