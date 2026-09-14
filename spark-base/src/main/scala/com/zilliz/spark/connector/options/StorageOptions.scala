package com.zilliz.spark.connector.options

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.snapshot.MilvusSnapshotReader
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.options.MilvusOption

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

  private[connector] def resolveClientSnapshotLocation(
      location: String,
      bucket: String
  ): String = {
    val trimmed = Option(location).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      throw new IllegalArgumentException("snapshot s3_location is empty")
    }

    val scheme = Option(new URI(trimmed).getScheme).map(_.toLowerCase)
    scheme match {
      case Some("s3a") => trimmed
      case Some("s3")  =>
        // Not written as an interpolation: IntelliJ's Scala lexer reads the
        // `//` of a nested "://" literal inside `${}` as a line comment and
        // mis-parses the rest of the file.
        "s3a://" + trimmed.substring(trimmed.indexOf("://") + 3)
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
      options: scala.collection.Map[String, String],
      key: String
  ): Option[String] = {
    options.collectFirst {
      case (optionKey, value) if optionKey.equalsIgnoreCase(key) => value
    }
  }

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
      conf: org.apache.hadoop.conf.Configuration,
      bucket: String,
      options: scala.collection.Map[String, String] = Map.empty
  ): com.zilliz.milvus.storage.io.ObjectStore = {
    val trimmed = Option(bucket).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      return HadoopStorageConfig
        .objectStore(conf, "")
    }
    val declared = options.filter { case (k, _) =>
      k.startsWith(
        com.zilliz.milvus.storage.credential.StorageProperties.Prefix
      ) ||
      k.startsWith(
        com.zilliz.milvus.storage.credential.StorageProperties.ExternalPrefix
      )
    }.toMap
    val merged = HadoopStorageConfig
      .toFsProperties(conf, trimmed) ++ declared ++
      Map(
        com.zilliz.milvus.storage.credential.StorageProperties.BucketName -> trimmed
      )
    HadoopStorageConfig.storeFrom(merged)
  }

  private[connector] def connectorS3BucketOption(
      options: scala.collection.Map[String, String]
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
      options: scala.collection.Map[String, String]
  ): String = {
    connectorS3BucketOption(options).getOrElse {
      throw new IllegalArgumentException(
        s"${StorageProperties.BucketName} is required for client snapshot reads"
      )
    }
  }

  private[connector] def snapshotS3BucketForRelativePaths(
      snapshotPath: String,
      options: scala.collection.Map[String, String]
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
  ): Long = {
    val value = Option(options.get(key))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { raw =>
        try raw.toLong
        catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Option '$key' must be a positive long, got '$raw'"
            )
        }
      }
      .getOrElse(defaultValue)
    if (value <= 0) {
      throw new IllegalArgumentException(
        s"Option '$key' must be positive, got $value"
      )
    }
    value
  }

  /** Backup `full_meta.json` size limit, honoring
    * `milvus.snapshot.max.json.bytes` (the same option the snapshot read uses).
    */
  private[connector] def backupMaxJsonBytes(
      options: CaseInsensitiveStringMap
  ): Long =
    parsePositiveLongOption(
      options,
      MilvusOption.SnapshotMaxJsonBytes,
      MilvusSnapshotReader.MaxSnapshotJsonBytes
    )

  /** Build a Hadoop `Configuration` for reading objects referenced by a
    * snapshot/backup path, applying the connector's `fs.*` options plus any
    * per-bucket S3A configuration. Extracted as a companion method so both the
    * scan planner and table-level schema rehydration (backup mode) share it.
    */
  private[connector] def buildHadoopConfForOptions(
      rawOptions: scala.collection.Map[String, String],
      path: String
  ): Configuration = {
    val conf = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    val endpoint = optionValue(rawOptions, StorageProperties.Address)
    val accessKey = optionValue(rawOptions, StorageProperties.AccessKeyId)
    val secretKey =
      optionValue(rawOptions, StorageProperties.AccessKeyValue)
    val useSsl = optionValue(rawOptions, StorageProperties.UseSSL)
    val region = optionValue(rawOptions, StorageProperties.Region)
    val useIam = optionValue(rawOptions, StorageProperties.UseIam)
      .exists(_.trim.equalsIgnoreCase("true"))
    val useVirtualHost =
      optionValue(rawOptions, StorageProperties.UseVirtualHost)
        .filter(_.trim.nonEmpty)
    val pathStyle = optionValue(rawOptions, "fs.s3a.path.style.access")
      .orElse(
        useVirtualHost.map(v => (!v.trim.equalsIgnoreCase("true")).toString)
      )

    def setIfDefined(key: String, value: Option[String]): Unit = {
      value.map(_.trim).filter(_.nonEmpty).foreach(conf.set(key, _))
    }

    def configureS3A(prefix: String): Unit = {
      setIfDefined(s"$prefix.endpoint", endpoint)
      setIfDefined(s"$prefix.connection.ssl.enabled", useSsl)
      setIfDefined(s"$prefix.path.style.access", pathStyle)
      setIfDefined(s"$prefix.endpoint.region", region)
      setIfDefined(s"$prefix.region", region)
      if (useIam) {
        conf.unset(s"$prefix.access.key")
        conf.unset(s"$prefix.secret.key")
        conf.set(
          s"$prefix.aws.credentials.provider",
          DefaultAwsCredentialsProvider
        )
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
