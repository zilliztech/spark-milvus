package com.zilliz.spark.connector.loon

import org.apache.hadoop.conf.Configuration

import com.zilliz.milvus.storage.credential.StorageProperties

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
  * It reads keys and renames them. It resolves no credentials and constructs
  * nothing.
  */
object HadoopStorageConfig {

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

  /** Reads the Hadoop keys out of `conf` and returns the `fs.*` map.
    *
    * Empty when neither an `fs.s3a.*` nor an `fs.oss.*` key is present, so a
    * caller can merge it over user options unconditionally.
    */
  def toFsProperties(conf: Configuration): Map[String, String] = {
    val s3a = translate(conf, S3A, s3aToFs)
    val oss = translate(conf, OSS, ossToFs)

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

  private def translate(
      conf: Configuration,
      prefix: String,
      table: Map[String, String]
  ): Map[String, String] =
    table.iterator.flatMap { case (suffix, fsKey) =>
      Option(conf.get(prefix + suffix))
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(fsKey -> _)
    }.toMap
}
