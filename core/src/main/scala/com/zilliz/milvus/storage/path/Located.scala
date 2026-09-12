package com.zilliz.milvus.storage.path

/** A location in object storage, split the way the C layer wants it.
  *
  * `key` is bucket-relative and carries no leading slash.
  *
  * `bucket` is empty when nothing named one — a local path, or a relative path
  * with no default bucket supplied. There is no object-storage location in that
  * case, so `key` holds the original string verbatim and [[uri]] hands it back
  * unchanged for whatever default filesystem the caller is on.
  */
final case class Located(bucket: String, key: String) {

  def hasBucket: Boolean = bucket.nonEmpty

  /** Renders back to `scheme://bucket/key`, or to `key` when there is no
    * bucket.
    *
    * Only the migration-period Hadoop callers need this. Code that talks to the
    * C filesystem passes `key` and lets `fs.bucket_name` carry the bucket.
    */
  def uri(scheme: String): String =
    if (hasBucket) s"${StoragePath.normalizeScheme(scheme)}://$bucket/$key"
    else key
}
