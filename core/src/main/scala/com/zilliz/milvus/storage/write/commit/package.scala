package com.zilliz.milvus.storage.write

/** The job manifest, commit protocol and idempotency: what the driver does once
  * every task of a write has committed its own segment manifest.
  *
  * `Committer.commit` writes the `JobManifest` (every segment's base path,
  * manifest version, row count and, for a segment written into, its id) to
  * `staging/{job}/manifest.json`, then the marker `staging/{job}/_committed`; a
  * rerun that finds the marker does nothing. `Committer.abort` deletes the
  * files under the staging prefix.
  *
  * `Registration.backfillItems` turns a job manifest into what Milvus's
  * `BatchUpdateManifest` takes (segment id, manifest version) and refuses a job
  * that created new segments, which Milvus cannot register yet;
  * `Committer.markRegistered` records a registration so it runs once. The
  * procedure that calls Milvus is `spark.procedure.Register`; nothing here
  * calls Milvus.
  *
  * Main types: JobManifest, CommittedSegment, Committer, CommitOutcome,
  * Registration. Capabilities: W3, A4 (see docs/design/capabilities.md).
  * Design: docs/design/README.md section 2.4.
  */
package object commit
