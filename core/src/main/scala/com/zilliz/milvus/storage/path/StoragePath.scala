package com.zilliz.milvus.storage.path

/** Normalizes the seven ways the format spells a location.
  *
  * The rules match `StorageUri::Parse` in milvus-storage's `fs.cpp`, because
  * whatever this produces is handed to that code:
  *
  *   - no scheme → the whole string is the key, the caller's bucket wins
  *   - scheme://b/k → the authority is the bucket, the rest is the key
  *
  * The Milvus three-segment form `scheme://address/bucket/key` is not produced
  * here, but it is read: DescribeSnapshot's `s3_location` is
  * `https://<endpoint>/<bucket>/<key>`, and for `http`/`https` the authority
  * is the endpoint and the first path segment is the bucket.
  */
object StoragePath {

  private val SchemeSeparator = "://"

  /** Turns any of the seven spellings into (bucket, key).
    *
    * `defaultBucket` is used when the path does not name one. A scheme-less
    * path is always a key: `files/insert_log/...` and `bucket/files/...` are
    * the same shape as strings, so the caller says which bucket it means
    * instead of this function guessing at the first segment.
    */
  def parse(raw: String, defaultBucket: String = ""): Located = {
    val trimmed = Option(raw).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      throw new IllegalArgumentException("path must not be blank")
    }

    val bucketHint = trim(defaultBucket)
    val separator = trimmed.indexOf(SchemeSeparator)
    if (separator < 0) {
      // With no bucket there is no object-storage location to speak of; keep
      // the string verbatim so it still resolves against the default
      // filesystem, which is how local paths reach Hadoop today.
      return if (bucketHint.isEmpty) Located("", trimmed)
      else Located(bucketHint, stripLeadingSlash(trimmed))
    }

    val authorityAndPath = trimmed.substring(separator + SchemeSeparator.length)
    val slash = authorityAndPath.indexOf('/')
    if (slash < 0) {
      throw new IllegalArgumentException(
        s"storage URI names a bucket but no key: $trimmed"
      )
    }

    // An empty authority is the `file:///path` form. It names no bucket, so it
    // stays verbatim unless the caller supplied one.
    val authority = authorityAndPath.substring(0, slash)
    val key = stripLeadingSlash(authorityAndPath.substring(slash + 1))
    if (key.isEmpty) {
      throw new IllegalArgumentException(
        s"storage URI is missing the key: $trimmed"
      )
    }
    val scheme = trimmed.substring(0, separator).toLowerCase
    if (authority.nonEmpty && (scheme == "http" || scheme == "https")) {
      // Path-style endpoint URL: scheme://endpoint/bucket/key. Milvus writes a
      // snapshot's location this way (DescribeSnapshot's s3_location), so the
      // authority is the S3 endpoint, not the bucket, and the first path
      // segment is the bucket. Virtual-hosted (bucket.endpoint) is not produced
      // by Milvus and is not handled here.
      val bucketSlash = key.indexOf('/')
      if (bucketSlash < 0) {
        throw new IllegalArgumentException(
          s"endpoint URI names a bucket but no key: $trimmed"
        )
      }
      return Located(
        key.substring(0, bucketSlash),
        stripLeadingSlash(key.substring(bucketSlash + 1))
      )
    }
    if (authority.nonEmpty) Located(authority, key)
    else if (bucketHint.nonEmpty) Located(bucketHint, key)
    else Located("", trimmed)
  }

  /** Joins a fragment onto a location.
    *
    * An absolute fragment replaces the base, which is how a manifest that
    * records full URIs alongside relative ones is read. Format fragments such
    * as `_delta/` are supplied by the caller; this function knows none of them.
    */
  def resolve(base: Located, child: String): Located = {
    val trimmed = Option(child).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) return base
    if (trimmed.contains(SchemeSeparator)) return parse(trimmed, base.bucket)

    val prefix = base.key.stripSuffix("/")
    val suffix = stripLeadingSlash(trimmed)
    val joined = if (prefix.isEmpty) suffix else s"$prefix/$suffix"
    base.copy(key = joined)
  }

  /** Accepts `s3a` and `s3a://` alike so call sites need not agree on which. */
  private[path] def normalizeScheme(scheme: String): String =
    Option(scheme).map(_.trim).getOrElse("").stripSuffix(SchemeSeparator)

  private def stripLeadingSlash(value: String): String = {
    var i = 0
    while (i < value.length && value.charAt(i) == '/') i += 1
    value.substring(i)
  }

  private def trim(value: String): String =
    Option(value).map(_.trim).getOrElse("")
}
