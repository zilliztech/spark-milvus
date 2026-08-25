package com.zilliz.spark.connector

import java.net.URI

/** Neutral storage-path utilities shared by the config layer (MilvusOption)
  * and the read layer (MilvusSnapshotReader).
  *
  * Milvus storage can surface the same object in three shapes depending on how
  * the server was configured:
  *
  *   - bucket-relative key: `file/snapshots/...` (no scheme)
  *   - standard S3: `s3://<bucket>/<key>`
  *   - Milvus-format: `s3://<address>/<bucket>/<key>` (address = storage
  *     endpoint, embedded whenever `fs.address` is non-empty, e.g. MinIO
  *     deployments)
  *
  * [[toStandardS3Path]] collapses all three to the Hadoop `s3a://<bucket>/<key>`
  * form; [[toBucketRelativeKey]] reduces a path to the bare object key that
  * milvus-storage's native FFI resolves against the `fs.*` properties.
  */
object MilvusStoragePath {

  /** Probe the scheme of a storage path without throwing on input
    * `java.net.URI` rejects (a space, an unescaped `%`, brackets).
    *
    * `s3://` / `s3a://` prefixes are matched textually so the unparseable
    * fallbacks in [[toStandardS3Path]] / [[toBucketRelativeKey]] / [[bucketOf]]
    * stay reachable from callers that first branch on the scheme.
    */
  private[connector] def schemeOf(path: String): Option[String] = {
    if (path == null) None
    else {
      val trimmed = path.trim
      if (trimmed.isEmpty) None
      else if (trimmed.startsWith("s3://")) Some("s3")
      else if (trimmed.startsWith("s3a://")) Some("s3a")
      else {
        try Option(new URI(trimmed).getScheme).map(_.toLowerCase)
        catch { case _: Exception => None }
      }
    }
  }

  /** Textually split a URI authority into its (host, port).
    *
    * `java.net.URI.getHost` / `getPort` return null / -1 for hosts that
    * violate RFC 2396 `domainlabel` — e.g. a docker-compose service name with
    * an underscore like `milvus_minio` — so fall back to parsing the raw
    * authority: drop through the last `@` (userinfo) and split on the last
    * `:` (a `[...]:port` IPv6 form keeps its brackets).
    */
  private def authorityHostPort(
      authority: String
  ): (String, Option[Int]) = {
    if (authority == null || authority.isEmpty) ("", None)
    else {
      val noUser = authority.split("@").last
      val colon = noUser.lastIndexOf(':')
      if (colon > 0) {
        val host = noUser.substring(0, colon)
        val port = noUser.substring(colon + 1).toIntOption
        (host, port)
      } else (noUser, None)
    }
  }

  /** Normalize a configured storage endpoint to a bare lowercase host, so it
    * can be compared against a URI authority host.
    *
    * Handles `http://host:port`, `host:port`, and bare-host forms. Unknown
    * shapes fall back to the trimmed, lowercased input.
    */
  private[connector] def storageEndpointHost(endpoint: String): String = {
    if (endpoint == null) return ""
    val trimmed = endpoint.trim
    if (trimmed.isEmpty) return ""
    val uri =
      try new URI(if (trimmed.contains("://")) trimmed else "//" + trimmed)
      catch { case _: Exception => return trimmed.toLowerCase }
    // URI.getHost preserves case and is null for hosts that violate RFC 2396
    // (e.g. an underscore), so normalize via the textual authority too.
    Option(uri.getHost)
      .map(_.toLowerCase)
      .orElse(
        Option(uri.getRawAuthority)
          .map(a => authorityHostPort(a)._1.toLowerCase)
          .filter(_.nonEmpty)
      )
      .getOrElse(trimmed.toLowerCase)
  }

  /** True when `uri` is a Milvus-format storage URI whose authority is the
    * storage endpoint rather than the bucket.
    *
    * Signals, in order:
    *
    *   - the authority carries a port (`host:port`): an S3 bucket name may only
    *     contain lowercase letters, digits, `-` and `.`, so it can never carry
    *     a port. The port is read from `getPort` when the host parses as a
    *     server-based authority, else derived textually from the raw authority
    *     (hosts with an underscore fail RFC 2396 and yield getPort == -1).
    *   - the authority host equals the configured storage endpoint host. This
    *     covers port-less endpoints (e.g. `s3.amazonaws.com`).
    *   - an endpoint-spelling-independent signal using the configured bucket:
    *     when the port and endpoint-host checks cannot decide, an authority
    *     that equals the configured bucket is standard S3 (the bucket is in
    *     the authority), while a first path segment equal to the configured
    *     bucket is Milvus-format (the bucket is embedded after the endpoint).
    *     This survives spelling differences between what Milvus embedded in
    *     the URI and what the connector was configured with (regional vs
    *     global host, IP vs DNS name, an alias behind a proxy).
    */
  private def isEndpointPrefixed(
      uri: URI,
      endpoint: String,
      configuredBucket: String
  ): Boolean = {
    val rawAuthority = Option(uri.getRawAuthority).getOrElse("")
    val (textHost, textPort) = authorityHostPort(rawAuthority)
    val port = if (uri.getPort > 0) uri.getPort else textPort.getOrElse(-1)
    if (port > 0) {
      true
    } else {
      val host = Option(uri.getHost)
        .map(_.toLowerCase)
        .orElse(Some(textHost.toLowerCase).filter(_.nonEmpty))
        .getOrElse("")
      if (host.nonEmpty && storageEndpointHost(endpoint) == host) {
        true
      } else {
        val configured = configuredBucket.trim.toLowerCase
        if (configured.nonEmpty && host.nonEmpty) {
          val rawPath = Option(uri.getRawPath).getOrElse("").stripPrefix("/")
          val slash = rawPath.indexOf('/')
          val firstSegment =
            (if (slash < 0) rawPath else rawPath.substring(0, slash)).toLowerCase
          if (host == configured) false // authority IS the bucket → standard
          else if (host.contains("."))
            // endpoint-like authority (a DNS name or IP): a first path segment
            // equal to the configured bucket implies Milvus-format. A
            // single-label authority such as "archive" or "data-lake" is a
            // standard bucket, not an endpoint — reading it as Milvus-format
            // would silently rewrite the path to the configured bucket.
            firstSegment == configured
          else false
        } else false
      }
    }
  }

  /** Textually split an unparseable `s3://` / `s3a://` URI (one that
    * `java.net.URI` rejects — a space, a brace, a stray `%`) into its
    * authority, path, host and port.
    *
    * `java.net.URI` cannot parse these inputs, but the authority/path split on
    * `/` still works textually, so Milvus-format detection (a `host:port`
    * authority, or a first path segment equal to the configured bucket) keeps
    * working instead of leaving the endpoint as the bucket.
    */
  private def unparseableAuthorityParts(
      trimmed: String
  ): (String, String, String, Option[Int]) = {
    val schemeEnd = trimmed.indexOf("://")
    val rest = if (schemeEnd >= 0) trimmed.substring(schemeEnd + 3) else trimmed
    val slash = rest.indexOf('/')
    val authority = if (slash < 0) rest else rest.substring(0, slash)
    val path = if (slash < 0) "" else rest.substring(slash + 1)
    val (host, port) = authorityHostPort(authority)
    (authority, path, host, port)
  }

  /** Reduce a bare object key from an unparseable `s3://` / `s3a://` URI. */
  private def unparseableBucketRelativeKey(trimmed: String): String = {
    val (_, path, _, port) = unparseableAuthorityParts(trimmed)
    if (port.nonEmpty) {
      // Milvus-format: scheme://address/bucket/key — strip the bucket segment.
      // With no path segment after the bucket there is no object key.
      val slash = path.indexOf('/')
      if (slash < 0) "" else path.substring(slash + 1)
    } else {
      // Standard S3: scheme://bucket/key — everything after the authority.
      path
    }
  }

  /** Extract the bucket from an unparseable `s3://` / `s3a://` URI, so a bare
    * key stripped from the same input is never left to resolve silently
    * against the connector's configured bucket.
    */
  private def unparseableBucket(trimmed: String): String = {
    val (authority, path, host, port) = unparseableAuthorityParts(trimmed)
    if (port.nonEmpty) {
      // Milvus-format: the first path segment is the bucket.
      val slash = path.indexOf('/')
      if (slash < 0) path else path.substring(0, slash)
    } else if (host.nonEmpty) {
      // Standard S3: the authority host is the bucket.
      host
    } else {
      authority
    }
  }

  /** Canonicalize an unparseable `s3://` / `s3a://` URI to
    * `s3a://bucket/key`, applying the same Milvus-format detection as the
    * parseable path.
    */
  private def unparseableStandardS3Path(trimmed: String): String = {
    val (authority, path, _, port) = unparseableAuthorityParts(trimmed)
    if (port.nonEmpty) {
      val slash = path.indexOf('/')
      if (slash < 0) "s3a://" + path
      else {
        val bucket = path.substring(0, slash)
        val key = path.substring(slash + 1)
        s"s3a://$bucket/$key"
      }
    } else s"s3a://$authority/$path"
  }

  /** Extract the bare bucket-relative object key from a storage path.
    *
    * This is the form milvus-storage's native FFI expects: it resolves
    * scheme-less keys against the `fs.*` properties and rejects any
    * scheme-bearing path for lack of an `extfs.*` config block. Hadoop APIs
    * should keep the [[toStandardS3Path]] form instead.
    *
    *   - `files/snapshots/...` → `files/snapshots/...`
    *   - `s3://bucket/files/...` → `files/...`
    *   - `s3://minio:9000/bucket/files/...` (Milvus-format) → `files/...`
    *
    * @param path
    *   storage path or URI to reduce
    * @param endpoint
    *   configured storage endpoint (e.g. `fs.address` / `s3Endpoint`), used to
    *   recognize endpoint-prefixed Milvus-format URIs
    */
  private[connector] def toBucketRelativeKey(
      path: String,
      endpoint: String = "",
      configuredBucket: String = ""
  ): String = {
    if (path == null) return null
    val trimmed = path.trim
    if (trimmed.isEmpty) return trimmed
    val uri =
      try new URI(trimmed)
      catch {
        case _: Exception =>
          // Unparseable s3 URI: textually split the authority so Milvus-format
          // detection still applies and a bare key is returned for the native
          // reader, instead of a scheme-bearing path it would reject.
          if (trimmed.startsWith("s3://") || trimmed.startsWith("s3a://"))
            return unparseableBucketRelativeKey(trimmed)
          else return trimmed
      }
    Option(uri.getScheme).map(_.toLowerCase) match {
      case Some("s3") | Some("s3a") =>
        // getRawPath preserves percent-encoding; getPath would decode %20 to a
        // space / %23 to a # and produce a broken object key or URI fragment.
        // Re-append a literal '?'/'#' so the key is not truncated.
        val rawPath = Option(uri.getRawPath).getOrElse("").stripPrefix("/")
        val suffix =
          Option(uri.getRawQuery).map("?" + _).getOrElse("") +
            Option(uri.getRawFragment).map("#" + _).getOrElse("")
        if (isEndpointPrefixed(uri, endpoint, configuredBucket)) {
          // Milvus-format: scheme://address/bucket/key — the bucket segment
          // precedes the object key. With no path segment after the bucket
          // there is no object key.
          val slash = rawPath.indexOf('/')
          val key = if (slash < 0) "" else rawPath.substring(slash + 1)
          key + suffix
        } else {
          // Standard S3: scheme://bucket/key — everything after the authority
          // is the object key.
          rawPath + suffix
        }
      case _ => trimmed
    }
  }

  /** Extract the bucket a qualified storage URI names, or `None` when `path`
    * carries no scheme (bucket-relative key).
    *
    * Unlike `MilvusScan.snapshotBucket` this never throws on a non-s3 scheme —
    * it is used to pin the correct bucket on native partition options before
    * a path is reduced to a bare key.
    */
  private[connector] def bucketOf(
      path: String,
      endpoint: String = "",
      configuredBucket: String = ""
  ): Option[String] = {
    if (path == null) return None
    val trimmed = path.trim
    if (trimmed.isEmpty) return None
    val uri =
      try new URI(trimmed)
      catch {
        case _: Exception =>
          // Unparseable s3 URI: textually split the authority so the bucket is
          // still extracted — toBucketRelativeKey strips the bucket from the
          // same input, so giving up here would leave the bare key to resolve
          // silently against the connector's configured bucket.
          if (trimmed.startsWith("s3://") || trimmed.startsWith("s3a://"))
            return Some(unparseableBucket(trimmed))
          else return None
      }
    Option(uri.getScheme).map(_.toLowerCase) match {
      case Some("s3") | Some("s3a") =>
        if (isEndpointPrefixed(uri, endpoint, configuredBucket)) {
          // Milvus-format: scheme://address/bucket/key — the first path
          // segment is the bucket.
          val rawPath = Option(uri.getRawPath).getOrElse("").stripPrefix("/")
          val slash = rawPath.indexOf('/')
          val bucket = if (slash < 0) rawPath else rawPath.substring(0, slash)
          Option(bucket).filter(_.nonEmpty)
        } else {
          // Standard S3: scheme://bucket/key — the authority host is the bucket.
          // getHost is null for hosts with e.g. an underscore, so fall back to
          // the authority and strip userinfo (everything after the last '@')
          // and any port.
          Option(uri.getHost).orElse(
            Option(uri.getRawAuthority)
              .map(_.split("@").last)
              .map(_.split(":").head)
              .filter(_.nonEmpty)
          )
        }
      case _ => None
    }
  }

  /** Canonicalize a Milvus storage path to Hadoop S3A path-style
    * `s3a://<bucket>/<key>`.
    *
    * This collapses the three shapes (bucket-relative, standard S3,
    * Milvus-format) to `s3a://<bucket>/<key>` so downstream Hadoop parsers
    * (bucket extraction, per-bucket S3A conf, S3A reads) see one shape.
    * Native milvus-storage consumers should use [[toBucketRelativeKey]]
    * instead.
    *
    * Detection of the Milvus-format uses both the authority carrying a port
    * and a match against the configured `endpoint` host — see
    * [[isEndpointPrefixed]].
    *
    * @param path
    *   storage path or URI to canonicalize
    * @param fallbackBucket
    *   bucket to prefix when `path` is bucket-relative (no scheme)
    * @param endpoint
    *   configured storage endpoint (e.g. `fs.address` / `s3Endpoint`); used to
    *   recognize endpoint-prefixed Milvus-format URIs
    */
  private[connector] def toStandardS3Path(
      path: String,
      fallbackBucket: String = "",
      endpoint: String = "",
      configuredBucket: String = ""
  ): String = {
    if (path == null) return null
    val trimmed = path.trim
    if (trimmed.isEmpty) return trimmed
    val uri =
      try new URI(trimmed)
      catch {
        case _: Exception =>
          // Unparseable s3 URI (a space, an unescaped %, [ ]): textually split
          // the authority so an endpoint-prefixed path still collapses to
          // s3a://bucket/key instead of leaving the endpoint as the bucket.
          if (trimmed.startsWith("s3://") || trimmed.startsWith("s3a://"))
            return unparseableStandardS3Path(trimmed)
          else return trimmed
      }
    Option(uri.getScheme).map(_.toLowerCase) match {
      case Some("s3") | Some("s3a") =>
        // getRawAuthority / getRawPath preserve percent-encoding so the
        // reconstructed URI stays parseable (getPath would turn %20 into a
        // literal space and %23 into a # fragment). getRawPath stops at a
        // literal '?' / '#', so re-append the raw query / fragment — a global
        // prefix rewrite would otherwise leave the endpoint as the bucket for
        // Milvus-format URIs.
        val authority = Option(uri.getRawAuthority).map(_.trim).filter(_.nonEmpty)
        val rawPath = Option(uri.getRawPath).getOrElse("").stripPrefix("/")
        val suffix =
          Option(uri.getRawQuery).map("?" + _).getOrElse("") +
            Option(uri.getRawFragment).map("#" + _).getOrElse("")
        if (isEndpointPrefixed(uri, endpoint, configuredBucket)) {
          // Milvus-format: scheme://address/bucket/key — the first path
          // segment is the bucket, the authority is the storage endpoint.
          val slash = rawPath.indexOf('/')
          val reconstructed =
            if (slash < 0) {
              s"s3a://$rawPath"
            } else {
              val bucket = rawPath.substring(0, slash)
              val key = rawPath.substring(slash + 1)
              s"s3a://$bucket/$key"
            }
          reconstructed + suffix
        } else {
          // Standard S3: scheme://bucket/key (also normalize s3 -> s3a).
          s"s3a://${authority.getOrElse("")}/$rawPath" + suffix
        }
      case None if fallbackBucket.trim.nonEmpty =>
        s"s3a://${fallbackBucket.trim}/${trimmed.stripPrefix("/")}"
      case _ => trimmed
    }
  }
}
