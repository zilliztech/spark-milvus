package com.zilliz.spark.connector

/** The procedures: what a `CALL milvus.system.<name>(...)` runs. `Procedure` is
  * the interface (parameters, result table, `run` on the driver), `Procedures`
  * the registry by name, and each body is a plain Scala entry point a job can
  * also call directly. The SQL front that reaches them is `spark.extensions`.
  * Design: docs/design/architecture/procedure.html.
  *
  * Snapshot, index, collection-lifecycle, and describe procedures delegate to
  * `client.api` and own their client for the duration of one driver-side call.
  * `Register` additionally reads a committed write job and hands its existing
  * segments to Milvus through `BatchUpdateManifest` (the backfill branch of
  * A4). `CleanupStagingProcedure` exposes A7's safe candidate audit, dry-run
  * and file-object deletion; recursive directory deletion remains blocked on a
  * native filesystem API, so it never reports a staging prefix deleted.
  *
  * Capabilities: A1, A2, A3, A4, A5, A7 (see docs/design/capabilities.md).
  */
package object procedure
