package com.zilliz.spark.connector.options

import org.apache.hadoop.conf.Configuration

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.{NativeObjectStore, ObjectStore}

/** Translates the Hadoop-style storage keys a runtime already has
  * (`spark.hadoop.fs.s3a.*`, `fs.oss.*`) into the `fs.*` keys
  * [[StorageProperties]] takes.
  *
  * This is the layer-3 shim described in storage-access.html 3.3. It exists so
  * an existing deployment — a managed platform that injects `fs.s3a.*`, or a
  * self-managed Spark configured the Hadoop way — keeps working without a
  * config change. The long-term shape is for whoever produces the configuration
  * to emit `fs.*` directly; this shim goes away with the last such deployment.
  *
  * The translation itself reads keys and renames them; it resolves no
  * credentials. [[objectStore]] is the one place that turns the result into a
  * live store, so every driver-side read opens storage the same way.
  *
  * Every entry point takes the bucket being opened, because a Hadoop key can be
  * set per bucket and that value wins over the global one.
  */
object HadoopStorageKeys {

  private val S3A = "fs.s3a."
  private val OSS = "fs.oss."

  /** Maps a Hadoop key suffix to its `fs.*` equivalent. The scheme prefix
    * (`fs.s3a.` / `fs.oss.`) is stripped before lookup, so one table serves
    * both. Keys not listed here are not forwarded.
    */
  private val s3aToFs: Map[String, String] = Map(
    "assumed.role.arn" -> StorageProperties.RoleArn,
    "assumed.role.external.id" -> StorageProperties.ExternalId,
    "assumed.role.session.name" -> StorageProperties.SessionName,
    "endpoint" -> StorageProperties.Address,
    "endpoint.region" -> StorageProperties.Region,
    "access.key" -> StorageProperties.AccessKeyId,
    "secret.key" -> StorageProperties.AccessKeyValue
  )

  private val ossToFs: Map[String, String] = Map(
    "assumed.role.arn" -> StorageProperties.RoleArn,
    "assumed.role.external.id" -> StorageProperties.ExternalId,
    "assumed.role.session.name" -> StorageProperties.SessionName,
    "endpoint" -> StorageProperties.Address,
    "accessKeyId" -> StorageProperties.AccessKeyId,
    "accessKeySecret" -> StorageProperties.AccessKeyValue
  )

  /** Reads the Hadoop keys for `bucket` out of `conf` and returns the `fs.*`
    * map.
    *
    * Hadoop lets a key be set once globally and again for one bucket
    * (`fs.s3a.bucket.<name>.access.key`), and the per-bucket value wins. A
    * deployment that talks to two buckets with different endpoints or different
    * credentials — backfill's source bucket and the Milvus storage bucket, for
    * one — has its real configuration only in the per-bucket keys, so reading
    * the global prefix alone picks up the wrong endpoint or no credentials at
    * all. `BackfillConfig.resolveAwsS3AssumeRole` resolves the same way.
    *
    * An empty `bucket` reads the global keys only.
    *
    * Empty when neither an `fs.s3a.*` nor an `fs.oss.*` key is present, so a
    * caller can merge it over user options unconditionally.
    */
  def toFsProperties(
      conf: Configuration,
      bucket: String = ""
  ): Map[String, String] = {
    val s3a = translate(conf, S3A, s3aToFs, bucket)
    val oss = translate(conf, OSS, ossToFs, bucket)

    // Which backend runs is not a Hadoop key; Hadoop dispatches by scheme. Pick
    // the provider from whichever namespace supplied keys. Both present is a
    // misconfiguration, so leave cloud_provider unset and let the C layer's
    // validation speak.
    val provider =
      if (s3a.nonEmpty && oss.isEmpty)
        Some(StorageProperties.CloudProvider -> "aws")
      else if (oss.nonEmpty && s3a.isEmpty)
        Some(StorageProperties.CloudProvider -> "aliyun")
      else None

    val out = s3a ++ oss ++ provider
    out
  }

  /** Opens the store the driver reads snapshots, manifests, footers, delete
    * files and backup metadata through.
    *
    * An empty `bucket` means the caller has no object-storage location: a local
    * directory, or a path that names its own. That is `fs.storage_type=local`,
    * which is the C layer's local backend and needs neither an endpoint nor
    * credentials.
    *
    * Keys passed to the returned store are bucket-relative. The C filesystem
    * wraps its backend in a subtree rooted at `fs.bucket_name` and appends
    * whatever it is handed, so a fully qualified path arrives doubled.
    */
  def objectStore(conf: Configuration, bucket: String): ObjectStore = {
    val trimmed = Option(bucket).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      return storeFrom(
        Map(StorageProperties.StorageType -> StorageProperties.StorageTypeLocal)
      )
    }
    storeFrom(
      toFsProperties(conf, trimmed) ++ Map(
        StorageProperties.BucketName -> trimmed
      )
    )
  }

  /** Opens a store from an `fs.*` map that a caller has already assembled.
    *
    * Validation and defaults are [[StorageProperties.from]]'s; the only thing
    * added here is the IAM fallback, which cannot live there because it is a
    * statement about Hadoop-shaped configuration, not about `fs.*`.
    */
  def storeFrom(properties: Map[String, String]): ObjectStore =
    NativeObjectStore
      .Factory(StorageProperties.from(properties ++ iamFallback(properties)))
      .open()

  /** A deployment that injects no static keys expects the instance role to be
    * used — IRSA on EKS, RRSA on ACK, an instance profile elsewhere. Hadoop
    * spells that as the absence of `access.key`, so there is no key to
    * translate; the C layer spells it `fs.use_iam`, which has to be set
    * explicitly or its provider chain is never consulted.
    */
  private def iamFallback(
      properties: Map[String, String]
  ): Map[String, String] = {
    val isLocal = properties
      .get(StorageProperties.StorageType)
      .exists(_.equalsIgnoreCase(StorageProperties.StorageTypeLocal))
    val hasStaticKeys = properties.contains(StorageProperties.AccessKeyId) &&
      properties.contains(StorageProperties.AccessKeyValue)
    val assumesRole = properties.contains(StorageProperties.RoleArn)
    val alreadySet = properties.contains(StorageProperties.UseIam)
    if (isLocal || hasStaticKeys || assumesRole || alreadySet) Map.empty
    else Map(StorageProperties.UseIam -> "true")
  }

  /** Per-bucket key first, then the global one, matching how Hadoop's own
    * connectors resolve a bucket's configuration.
    */
  private def translate(
      conf: Configuration,
      prefix: String,
      table: Map[String, String],
      bucket: String
  ): Map[String, String] = {
    val trimmedBucket = Option(bucket).map(_.trim).getOrElse("")
    val bucketPrefix =
      if (trimmedBucket.isEmpty) None else Some(s"${prefix}bucket.$trimmedBucket.")

    def read(key: String): Option[String] =
      Option(conf.get(key)).map(_.trim).filter(_.nonEmpty)

    table.iterator.flatMap { case (suffix, fsKey) =>
      bucketPrefix
        .flatMap(bp => read(bp + suffix))
        .orElse(read(prefix + suffix))
        .map(fsKey -> _)
    }.toMap
  }
}
