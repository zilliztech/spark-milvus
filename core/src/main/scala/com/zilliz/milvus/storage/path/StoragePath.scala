package com.zilliz.milvus.storage.path

/** Normalizes storage paths.
  *
  * The format writes the same location three ways: a fully qualified path with
  * a scheme, a bucket-relative key, and a path that carries the bucket name but
  * no scheme. A snapshot's base_path has appeared in two of those forms in the
  * same file. Collapsing them here keeps every consumer from deciding on its
  * own.
  */
object StoragePath {

  /** Fills in the scheme and the bucket name.
    *
    * Both s3 and s3a are rewritten to the scheme the caller asks for. A single
    * snapshot mixes the two prefixes, and Hadoop picks a FileSystem
    * implementation by scheme, so leaving them alone sends some reads to an
    * implementation that has no credentials configured.
    */
  def resolvePath(
      path: String,
      bucket: String,
      storageScheme: String = "s3a"
  ): String = {
    val scheme = storageScheme.stripSuffix("://")
    if (path == null) path
    else if (path.startsWith("s3a://"))
      s"$scheme://" + path.stripPrefix("s3a://")
    else if (path.startsWith("s3://"))
      s"$scheme://" + path.stripPrefix("s3://")
    else if (path.contains("://")) path
    else if (bucket != null && bucket.nonEmpty)
      s"$scheme://$bucket/${path.stripPrefix("/")}"
    else path
  }
}
