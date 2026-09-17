package com.zilliz.spark.connector.options

import java.net.URI
import scala.collection.{Map => CollectionMap}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.json.SnapshotJson

/** How the driver reaches object storage from the options it was given.
  *
  * Bucket resolution for snapshot and backup locations, the translation of the
  * connector's `fs.*` options into a Hadoop `Configuration`, and `storeFor`,
  * the one place the driver opens an `ObjectStore`. Used by the table (schema
  * rehydration in backup mode) and by every scan planner.
  */
object StorageOptions extends Logging {
  private val DefaultAwsCredentialsProvider =
    "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
  private val SimpleAwsCredentialsProvider =
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  private val s3aProviderKey = "fs.s3a.aws.credentials.provider"

  private[connector] def resolveClientSnapshotLocation(
      location: String,
      bucket: String,
      endpoint: String = ""
  ): String = {
    val trimmed = Option(location).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      throw new IllegalArgumentException("snapshot s3_location is empty")
    }

    val scheme = Option(new URI(trimmed).getScheme).map(_.toLowerCase)
    scheme match {
      case Some("s3a") | Some("s3") =>
        StoragePath.parseMilvus(trimmed, bucket, endpoint).uri("s3a")
      case Some(other) =>
        throw new IllegalArgumentException(
          s"Unsupported snapshot s3_location scheme '$other': $trimmed"
        )
      case None if bucket.trim.nonEmpty =>
        s"s3a://${bucket.trim}/${trimmed.stripPrefix("/")}"
      case None =>
        throw new IllegalArgumentException(
          "bucket-relative snapshot s3_location requires connector S3 bucket"
        )
    }
  }

  private[connector] def snapshotBucket(location: String): Option[String] = {
    val trimmed = Option(location).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      None
    } else {
      val uri = new URI(trimmed)
      Option(uri.getScheme).map(_.toLowerCase) match {
        case Some("s3a") | Some("s3") =>
          Option(uri.getHost).orElse {
            Option(uri.getAuthority)
              .map(_.takeWhile(_ != '@'))
              .map(_.split(":").head)
              .filter(_.nonEmpty)
          }
        // Non-S3 schemes (e.g. `file://` for a local snapshot/backup dir) carry
        // no bucket to configure; treat them as "no bucket". Explicit scheme
        // validation lives in resolveClientSnapshotLocation.
        case Some(_) => None
        case None    => None
      }
    }
  }

  private[connector] def snapshotBucketsToConfigure(
      snapshotPath: String,
      connectorBucket: String
  ): Seq[String] = {
    (Seq(connectorBucket).filter(_.nonEmpty) ++ snapshotBucket(
      snapshotPath
    )).distinct
  }

  private[connector] def optionValue(
      options: CollectionMap[String, String],
      key: String
  ): Option[String] = {
    options.collectFirst {
      case (optionKey, value) if optionKey.equalsIgnoreCase(key) => value
    }
  }

  /** Configured endpoint used by both Milvus-path detection and storage
    * clients. The native `fs.address` key is authoritative; Hadoop's
    * `fs.s3a.endpoint` and the legacy `s3.endpoint` spelling remain aliases.
    */
  private[connector] def effectiveEndpoint(
      options: CollectionMap[String, String]
  ): Option[String] =
    Seq(
      StorageProperties.Address,
      "fs.s3a.endpoint",
      MilvusOption.S3Endpoint
    ).view
      .flatMap(key => optionValue(options, key))
      .map(_.trim)
      .find(_.nonEmpty)

  /** Path-style setting shared by Hadoop S3A and the native store.
    * `fs.use_virtual_host` expresses the inverse, so it is converted once at
    * this boundary.
    */
  private[connector] def effectivePathStyleAccess(
      options: CollectionMap[String, String]
  ): Option[Boolean] =
    booleanOption(options, "fs.s3a.path.style.access")
      .orElse(booleanOption(options, MilvusOption.S3PathStyleAccess))
      .orElse(
        booleanOption(options, StorageProperties.UseVirtualHost).map(!_)
      )

  private def booleanOption(
      options: CollectionMap[String, String],
      key: String
  ): Option[Boolean] =
    optionValue(options, key).map(_ =>
      OptionParsing.boolean(
        candidate => optionValue(options, candidate),
        key,
        defaultValue = false
      )
    )

  /** The one place the driver opens object storage.
    *
    * Two sources feed it, and explicit beats inferred: the connector's own
    * `fs.*` options win over the `fs.s3a.*` / `fs.oss.*` keys translated out of
    * the Hadoop configuration, which are there for a deployment that only
    * configures storage the Hadoop way.
    *
    * An empty `bucket` means no object-storage location — a local directory, or
    * a path that carries its own bucket. That is the C layer's local backend,
    * which needs neither an endpoint nor credentials.
    *
    * Keys handed to the store are bucket-relative: the C filesystem appends
    * them to a subtree rooted at `fs.bucket_name`.
    */
  private[connector] def storeFor(
      conf: Configuration,
      bucket: String,
      options: CollectionMap[String, String] = Map.empty
  ): ObjectStore = {
    val trimmed = Option(bucket).map(_.trim).getOrElse("")
    HadoopStorageKeys.storeFrom(storagePropertiesFor(conf, trimmed, options))
  }

  /** The exact native `fs.*` bag for a bucket, exposed separately from the live
    * store so alias translation stays unit-testable without loading JNI.
    */
  private[connector] def storagePropertiesFor(
      conf: Configuration,
      bucket: String,
      options: CollectionMap[String, String]
  ): Map[String, String] = {
    val trimmed = Option(bucket).map(_.trim).getOrElse("")
    val declared = options.filter { case (k, _) =>
      k.startsWith(
        StorageProperties.Prefix
      ) ||
      k.startsWith(
        StorageProperties.ExternalPrefix
      )
    }.toMap
    if (trimmed.isEmpty) {
      return HadoopStorageKeys.canonicalProperties(
        Map(
          StorageProperties.StorageType -> StorageProperties.StorageTypeLocal
        ) ++ declared
      )
    }
    val translatedAliases =
      effectiveEndpoint(options)
        .map(StorageProperties.Address -> _)
        .toMap ++
        effectivePathStyleAccess(options)
          .map(pathStyle =>
            StorageProperties.UseVirtualHost -> (!pathStyle).toString
          )
          .toMap
    val merged = HadoopStorageKeys
      .toFsProperties(conf, trimmed, declared = declared) ++ declared ++
      translatedAliases ++
      Map(
        StorageProperties.BucketName -> trimmed
      )
    HadoopStorageKeys.canonicalProperties(merged)
  }

  /** The endpoint of the store [[storeFor]] opens for the same arguments. A
    * snapshot or manifest URI in Milvus's `https://<endpoint>/<bucket>/<key>`
    * form is recognized against this value, so an endpoint that reaches the
    * store only through the session's Hadoop keys is recognized too.
    */
  private[connector] def storeEndpoint(
      conf: org.apache.hadoop.conf.Configuration,
      bucket: String,
      options: scala.collection.Map[String, String]
  ): String =
    storagePropertiesFor(conf, bucket, options)
      .getOrElse(StorageProperties.Address, "")

  /** The native `fs.*` bag a write uses, resolved once on the driver the way a
    * read resolves it: aliases, the session's Hadoop keys and the IAM fallback
    * included. The task writers and the job committer take this map as it is,
    * so a read that works cannot leave a write that fails on the same options
    * (review 749178e #10).
    *
    * A declared `fs.storage_type=local` keeps its declared `fs.*` (the local
    * backend is rooted at `fs.root_path`). Otherwise the bucket is the
    * snapshot's, else the option's.
    */
  private[connector] def writeStorageProperties(
      options: scala.collection.Map[String, String],
      snapshotBucket: String
  ): Map[String, String] = {
    val declaredLocal = optionValue(options, StorageProperties.StorageType)
      .exists(_.trim.equalsIgnoreCase(StorageProperties.StorageTypeLocal))
    if (declaredLocal) {
      return HadoopStorageKeys.canonicalProperties(options.toMap)
    }
    val bucket = Option(snapshotBucket)
      .map(_.trim)
      .filter(_.nonEmpty)
      .orElse(connectorS3BucketOption(options))
      .getOrElse(
        throw new IllegalArgumentException(
          s"${StorageProperties.BucketName} must be set to write"
        )
      )
    storagePropertiesFor(
      buildHadoopConfForOptions(options, ""),
      bucket,
      options
    )
  }

  private[connector] def connectorS3BucketOption(
      options: CollectionMap[String, String]
  ): Option[String] = {
    Seq(
      StorageProperties.BucketName,
      MilvusOption.FsBucketName,
      MilvusOption.S3BucketName
    ).view
      .flatMap(key => optionValue(options, key).map(_.trim).filter(_.nonEmpty))
      .headOption
  }

  private[connector] def resolveConnectorS3Bucket(
      options: CollectionMap[String, String]
  ): String = {
    connectorS3BucketOption(options).getOrElse {
      throw new IllegalArgumentException(
        s"${StorageProperties.BucketName} is required for client snapshot reads"
      )
    }
  }

  private[connector] def snapshotS3BucketForRelativePaths(
      snapshotPath: String,
      options: CollectionMap[String, String]
  ): Option[String] = {
    snapshotBucket(snapshotPath).orElse(connectorS3BucketOption(options))
  }

  private[connector] def isBucketRelativeSnapshotLocation(
      location: String
  ): Boolean = {
    val trimmed = Option(location).map(_.trim).getOrElse("")
    trimmed.nonEmpty && Option(new URI(trimmed).getScheme).isEmpty
  }

  private[connector] def parsePositiveLongOption(
      options: CaseInsensitiveStringMap,
      key: String,
      defaultValue: Long
  ): Long =
    OptionParsing.positiveLong(
      candidate => OptionParsing.value(options, candidate),
      key,
      defaultValue
    )

  /** Backup `full_meta.json` size limit, honoring
    * `milvus.snapshot.max.json.bytes` (the same option the snapshot read uses).
    */
  private[connector] def backupMaxJsonBytes(
      options: CaseInsensitiveStringMap
  ): Long =
    parsePositiveLongOption(
      options,
      MilvusOption.SnapshotMaxJsonBytes,
      SnapshotJson.MaxBytes
    )

  /** Build a Hadoop `Configuration` for reading objects referenced by a
    * snapshot/backup path, applying the connector's `fs.*` options plus any
    * per-bucket S3A configuration. Extracted as a companion method so both the
    * scan planner and table-level schema rehydration (backup mode) share it.
    */
  private[connector] def buildHadoopConfForOptions(
      rawOptions: CollectionMap[String, String],
      path: String
  ): Configuration = {
    val conf = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    val endpoint = effectiveEndpoint(rawOptions)
    val accessKey = optionValue(rawOptions, StorageProperties.AccessKeyId)
    val secretKey =
      optionValue(rawOptions, StorageProperties.AccessKeyValue)
    val useSsl = booleanOption(rawOptions, StorageProperties.UseSSL)
      .map(_.toString)
    val region = optionValue(rawOptions, StorageProperties.Region)
    val useIam =
      booleanOption(rawOptions, StorageProperties.UseIam).getOrElse(false)
    val pathStyle = effectivePathStyleAccess(rawOptions).map(_.toString)

    def setIfDefined(key: String, value: Option[String]): Unit = {
      value.map(_.trim).filter(_.nonEmpty).foreach(conf.set(key, _))
    }

    // The session's own chain, read before this method changes it: a managed
    // runtime selects an AssumeRole provider for its data role, and fs.use_iam
    // names only the source credential that role is assumed from, so a chain
    // that is only that role is kept. Any other chain gives way to the default
    // chain fs.use_iam asks for.
    val sessionProvider =
      Option(conf.getTrimmed(s3aProviderKey)).filter(_.nonEmpty)
    def assumesRole(prefix: String): Boolean =
      Option(conf.getTrimmed(s"$prefix.aws.credentials.provider"))
        .filter(_.nonEmpty)
        .orElse(sessionProvider)
        .exists(HadoopStorageKeys.onlyAssumedRole)

    def configureS3A(prefix: String): Unit = {
      setIfDefined(s"$prefix.endpoint", endpoint)
      setIfDefined(s"$prefix.connection.ssl.enabled", useSsl)
      setIfDefined(s"$prefix.path.style.access", pathStyle)
      setIfDefined(s"$prefix.endpoint.region", region)
      setIfDefined(s"$prefix.region", region)
      if (useIam) {
        conf.unset(s"$prefix.access.key")
        conf.unset(s"$prefix.secret.key")
        if (!assumesRole(prefix)) {
          conf.set(
            s"$prefix.aws.credentials.provider",
            DefaultAwsCredentialsProvider
          )
        }
      } else {
        setIfDefined(s"$prefix.access.key", accessKey)
        setIfDefined(s"$prefix.secret.key", secretKey)
        if (
          accessKey.exists(_.trim.nonEmpty) && secretKey.exists(_.trim.nonEmpty)
        ) {
          conf.set(
            s"$prefix.aws.credentials.provider",
            SimpleAwsCredentialsProvider
          )
        }
      }
    }

    if (conf.get("fs.s3a.impl") == null) {
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    }
    conf.set("fs.s3a.impl.disable.cache", "true")
    if (
      !useIam && (accessKey
        .forall(_.trim.isEmpty) || secretKey.forall(_.trim.isEmpty))
    ) {
      logWarning(
        "Snapshot/backup S3 credentials were not provided; Hadoop S3A will use the default AWS credential provider chain."
      )
    }
    configureS3A("fs.s3a")

    snapshotBucketsToConfigure(
      path,
      connectorS3BucketOption(rawOptions).getOrElse("")
    ).foreach(bucket => configureS3A(s"fs.s3a.bucket.$bucket"))
    conf
  }
}
