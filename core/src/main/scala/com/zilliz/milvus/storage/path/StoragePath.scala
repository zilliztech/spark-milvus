package com.zilliz.milvus.storage.path

/** Normalizes the seven ways the format spells a location.
  *
  * The rules match `StorageUri::Parse` in milvus-storage's `fs.cpp`, because
  * whatever this produces is handed to that code:
  *
  *   - no scheme → the whole string is the key, the caller's bucket wins
  *   - scheme://b/k → the authority is the bucket, the rest is the key
  *
  * Milvus-produced `scheme://address/bucket/key` paths go through
  * [[parseMilvus]]. This general parser never guesses that an authority is an
  * endpoint.
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
    if (authority.nonEmpty) Located(authority, key)
    else if (bucketHint.nonEmpty) Located(bucketHint, key)
    else Located("", trimmed)
  }

  /** Parses a path emitted by Milvus metadata.
    *
    * With a custom object-storage endpoint, Milvus writes
    * `scheme://endpoint/bucket/key` instead of the standard
    * `scheme://bucket/key`. The two forms are distinguished only when the
    * authority carries an explicit port, or its host exactly matches the
    * configured endpoint host. No path-segment or dotted-host heuristic is
    * used, because either would rewrite valid standard bucket URIs.
    *
    * Callers must use this method only for paths produced by Milvus. User input
    * continues through [[parse]].
    */
  def parseMilvus(
      raw: String,
      defaultBucket: String = "",
      endpoint: String = ""
  ): Located = {
    val standard = parse(raw, defaultBucket)
    val trimmed = raw.trim
    val separator = trimmed.indexOf(SchemeSeparator)
    if (separator < 0) return standard

    val authorityAndPath = trimmed.substring(separator + SchemeSeparator.length)
    val slash = authorityAndPath.indexOf('/')
    if (slash < 0) return standard

    val authority = authorityAndPath.substring(0, slash)
    if (
      authority.isEmpty ||
      (!hasExplicitPort(authority) && !matchesEndpointHost(authority, endpoint))
    ) {
      return standard
    }

    val bucketAndKey =
      stripLeadingSlash(authorityAndPath.substring(slash + 1))
    val bucketSlash = bucketAndKey.indexOf('/')
    if (bucketSlash < 0) {
      throw new IllegalArgumentException(
        s"endpoint URI names a bucket but no key: $trimmed"
      )
    }
    val bucket = bucketAndKey.substring(0, bucketSlash)
    val key = stripLeadingSlash(bucketAndKey.substring(bucketSlash + 1))
    if (bucket.isEmpty || key.isEmpty) {
      throw new IllegalArgumentException(
        s"endpoint URI is missing the bucket or key: $trimmed"
      )
    }
    Located(bucket, key)
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

  private def hasExplicitPort(authority: String): Boolean = {
    val hostPort = withoutUserInfo(authority)
    val colon = hostPort.lastIndexOf(':')
    colon >= 0 && colon < hostPort.length - 1 &&
    hostPort.substring(colon + 1).forall(_.isDigit) &&
    (!hostPort.startsWith("[") || hostPort.substring(0, colon).endsWith("]"))
  }

  private def matchesEndpointHost(
      authority: String,
      endpoint: String
  ): Boolean = {
    val expected = endpointHost(endpoint)
    expected.nonEmpty && authorityHost(authority).equalsIgnoreCase(expected)
  }

  private def endpointHost(endpoint: String): String = {
    val value = trim(endpoint)
    if (value.isEmpty) return ""
    val separator = value.indexOf(SchemeSeparator)
    val withoutScheme =
      if (separator < 0) value
      else value.substring(separator + SchemeSeparator.length)
    authorityHost(withoutScheme.takeWhile(_ != '/'))
  }

  private def authorityHost(authority: String): String = {
    val hostPort = withoutUserInfo(authority)
    if (hostPort.startsWith("[")) {
      val close = hostPort.indexOf(']')
      if (close >= 0) hostPort.substring(0, close + 1).toLowerCase
      else hostPort.toLowerCase
    } else {
      val colon = hostPort.lastIndexOf(':')
      val host =
        if (
          colon >= 0 && colon < hostPort.length - 1 &&
          hostPort.substring(colon + 1).forall(_.isDigit)
        ) hostPort.substring(0, colon)
        else hostPort
      host.toLowerCase
    }
  }

  private def withoutUserInfo(authority: String): String = {
    val at = authority.lastIndexOf('@')
    if (at < 0) authority else authority.substring(at + 1)
  }
}
