package com.zilliz.spark.connector

/** The procedures: what a `CALL milvus.system.<name>(...)` runs. `Procedure` is
  * the interface (parameters, result table, `run` on the driver), `Procedures`
  * the registry by name, and each body is a plain Scala entry point a job can
  * also call directly. The SQL front that reaches them is `spark.extensions`.
  * Design: docs/design/architecture/procedure.html.
  *
  * `Register` hands a committed write job's segments to Milvus through
  * `BatchUpdateManifest` (the backfill branch of A4); `RegisterProcedure` is
  * its CALL form.
  *
  * Capabilities: A4 (see docs/design/capabilities.md).
  */
package object procedure
