# core antlr4 sources

Plan.g4 belongs here: the grammar for Milvus expressions, from which the
PlanParser in `core.expr` is generated. It carries capability R7.

Where the antlr runtime lives is decision 17 in docs/design/README.md. Either
core ships and relocates it, or Plan.g4 moves to the shared source directory and
each Spark line generates its own parser with its own antlr version. Spark 3.5
ships antlr 4.9.3 and the 4.x lines ship 4.13.1, the generated code is not
interchangeable, and core is a single artifact across all lines. Until that is
settled, this directory holds only the grammar file.
