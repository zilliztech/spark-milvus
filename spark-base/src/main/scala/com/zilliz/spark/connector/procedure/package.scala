package com.zilliz.spark.connector

/** The procedures: what a `CALL milvus.system.<name>(...)` runs. The bodies
  * live in `spark-base` and are plain Scala entry points; the SQL front (parser
  * extension, logical node, planner strategy, decided 2026-09-10) is not wired
  * yet, so a job calls them directly.
  *
  * `Register` hands a committed write job's segments to Milvus through
  * `BatchUpdateManifest` (the backfill branch of A4).
  *
  * Capabilities: A4 (see docs/design/capabilities.md).
  */
package object procedure
