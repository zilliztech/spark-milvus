package com.zilliz.milvus.storage

/** Normalizes the seven ways the format spells a location into (bucket, key).
  *
  * `key` is bucket-relative with no leading slash, matching what the C layer
  * expects: its FilesystemCache wraps the backend in a FileSystemProxy(bucket,
  * fs) subtree, so prepending the bucket here produces doubled paths. The
  * bucket travels separately, in `fs.bucket_name` or
  * `extfs.<name>.bucket_name`.
  *
  * A path with no scheme is always a key. `files/insert_log/...` and
  * `bucket/files/...` are indistinguishable as strings, so the caller supplies
  * the default bucket rather than the parser guessing at the first segment.
  *
  * Format knowledge stays out: `core.delete` and `core.manifest` call resolve
  * with their own `_delta/` and `_metadata/manifest-N.avro` fragments.
  *
  * Main types: Located, StoragePath. Capabilities: R3 (see
  * docs/design/capabilities.md). Design:
  * docs/design/architecture/storage-access.html 2.2.
  */
package object path
