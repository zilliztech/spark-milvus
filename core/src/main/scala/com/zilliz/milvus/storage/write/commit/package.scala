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
  * `StagingCleaner` implements A7's fail-closed ownership, heartbeat, retention
  * and dry-run decisions, and deletes eligible file objects. The native
  * filesystem has no directory-delete binding yet, so it reports the remaining
  * directory entries and never claims that the prefix was removed. Backfill,
  * registered jobs, legacy manifests and inconsistent metadata are always
  * preserved.
  *
  * A build job records what it wrote as `CommittedIndex` entries in the same
  * job manifest, one per segment index (W6). Planned for W8: writing a
  * snapshot, that is the snapshot JSON (the shapes in `core.snapshot.json`) and
  * the segment Avro manifests, including those index records, so that Milvus
  * can restore it into a new collection. `core.snapshot` stays read-only.
  *
  * Main types: JobManifest, CommittedSegment, CommittedIndex, Committer,
  * CommitOutcome, Registration, StagingCleaner. Capabilities: W3, A4, A7, W6
  * (see docs/design/capabilities.md). A7 remains partial until native recursive
  * directory deletion is available. Design: docs/design/README.md section 2.4
  * and docs/design/architecture/procedure.html section 3.3.
  */
package object commit
