package com.zilliz.milvus.storage.write

/** The job manifest, commit protocol and idempotency: what the driver does once
  * every task of a write has committed its own segment manifest.
  *
  * `Committer.commit` writes the `JobManifest` (every segment's base path,
  * manifest version and row count) to `staging/{job}/manifest.json`, then the
  * marker `staging/{job}/_committed`; a rerun that finds the marker does
  * nothing. `Committer.abort` deletes the files under the staging prefix.
  * Registration reads the job manifest and is a Spark procedure; nothing here
  * calls Milvus.
  *
  * Main types: JobManifest, CommittedSegment, Committer, CommitOutcome.
  * Capabilities: W3, A4 (see docs/design/capabilities.md); A4's procedure side
  * is still to come. Design: docs/design/README.md section 2.4.
  */
package object commit
