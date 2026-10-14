package com.zilliz.spark.connector.apps.backfill

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.{NativeObjectStore, ObjectStore}
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.spark.connector.options.HadoopStorageKeys

/** Storage paths and access scopes used by the backfill application. Snapshot
  * bytes and staging stores use native storage; parquet input and result JSON
  * retain the application's Hadoop configuration rules.
  */
object BackfillStorage {

  private val logger = LoggerFactory.getLogger(getClass)

  private[backfill] def stagingStore(
      config: BackfillConfig
  ): ObjectStore = {
    NativeObjectStore
      .Factory(
        StorageProperties.from(config.getS3WriteOptionsForBasePath(""))
      )
      .open()
  }

  /** Write backfill result JSON to the given output path (S3 or local). Uses
    * Spark's Hadoop FileSystem API for portability.
    *
    * Returns Right(()) on success and Left(BackfillError) on failure — callers
    * MUST check the result and propagate failure to the user. The previous
    * version swallowed exceptions, which caused silent successes (exit 0 with
    * success message printed but no result file written).
    *
    * The output path may live in a bucket whose credentials are not yet
    * configured on the Spark Hadoop conf, so we run [[withScopedHadoopStorage]]
    * for it first, using the Milvus storage credentials rather than the
    * source-input overrides.
    */
  def writeResultJson(
      spark: SparkSession,
      result: BackfillResult,
      rawOutputPath: String,
      config: BackfillConfig
  ): Either[BackfillError, Unit] = {
    // Use the same s3:// → s3a:// normalization as the parquet input.
    // Without it, an s3:// URL would route to the legacy
    // S3FileSystem which ignores fs.s3a.bucket.<b>.* config.
    val outputPath = normalizeObjectStorageScheme(rawOutputPath, config)
    try {
      withScopedHadoopStorage(spark, outputPath, config, isSource = false) {
        val hadoopPath = new Path(outputPath)
        val fs =
          hadoopPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val out = fs.create(hadoopPath, true)
        try {
          out.write(
            result.toJson.getBytes(StandardCharsets.UTF_8)
          )
        } finally {
          out.close()
        }
      }
      logger.info(s"Backfill result JSON written to: $outputPath")
      Right(())
    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to write result JSON to $outputPath: ${e.getMessage}",
          e
        )
        Left(
          WriteError(
            segmentId = -1,
            outputPath = outputPath,
            message = s"Failed to write result JSON: ${e.getMessage}",
            cause = Some(e)
          )
        )
    }
  }

  /** Normalize an `s3://` URL to `s3a://`. The legacy `s3://` scheme maps to
    * Hadoop's S3FileSystem (or, on 3.4.x, an alias that does NOT honor
    * `fs.s3a.bucket.<b>.*` per-bucket config), so per-bucket credentials we
    * write would be silently ignored. All read/write code paths in this object
    * route through this helper before touching Hadoop FS APIs.
    */
  private[backfill] def normalizeS3Scheme(path: String): String = {
    if (path == null) null
    else if (path.startsWith("s3://")) "s3a://" + path.stripPrefix("s3://")
    else path
  }

  private[backfill] def normalizeObjectStorageScheme(
      path: String,
      config: BackfillConfig
  ): String = {
    if (path == null) null
    else if (config.s3CloudProvider.trim.equalsIgnoreCase("aliyun")) {
      if (path.startsWith("s3://")) "oss://" + path.stripPrefix("s3://")
      else if (path.startsWith("s3a://")) "oss://" + path.stripPrefix("s3a://")
      else path
    } else normalizeS3Scheme(path)
  }

  private[backfill] def storageScheme(config: BackfillConfig): String =
    if (config.s3CloudProvider.trim.equalsIgnoreCase("aliyun")) "oss"
    else "s3a"

  private[backfill] def storagePath(
      config: BackfillConfig,
      suffix: String
  ): String = {
    storageScheme(config) + "://" + config.s3BucketName + "/" + suffix
      .stripPrefix("/")
  }

  private val OssHadoopKeys = Seq(
    "fs.oss.impl",
    "fs.oss.impl.disable.cache",
    "fs.oss.endpoint",
    "fs.oss.connection.secure.enabled",
    "fs.oss.accessKeyId",
    "fs.oss.accessKeySecret",
    BackfillConfig.HadoopOssSecurityToken,
    BackfillConfig.HadoopOssCredentialsProvider
  )

  private[backfill] def withScopedHadoopStorage[T](
      spark: SparkSession,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  )(operation: => T): T = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    if (path == null || !path.startsWith("oss://")) {
      configureHadoopStorageForPath(hadoopConf, path, config, isSource)
      return operation
    }

    val previous = mutable.LinkedHashMap.empty[String, Option[String]]
    OssHadoopKeys.foreach(key => previous.put(key, Option(hadoopConf.get(key))))
    try {
      configureHadoopStorageForPath(hadoopConf, path, config, isSource)
      hadoopConf.set("fs.oss.impl.disable.cache", "true")
      operation
    } finally {
      previous.foreach {
        case (key, Some(value)) => hadoopConf.set(key, value)
        case (key, None)        => hadoopConf.unset(key)
      }
    }
  }

  private[backfill] def configureHadoopStorageForPath(
      hadoopConf: Configuration,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = {
    val normalized = path
    if (normalized != null && normalized.startsWith("oss://")) {
      configureHadoopOssForPath(hadoopConf, normalized, config, isSource)
    } else {
      configureHadoopS3ForPath(hadoopConf, normalized, config, isSource)
    }
  }

  private[backfill] def configureHadoopStorageForPath(
      spark: SparkSession,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = configureHadoopStorageForPath(
    spark.sparkContext.hadoopConfiguration,
    normalizeObjectStorageScheme(path, config),
    config,
    isSource
  )

  private[backfill] def configureHadoopOssForPath(
      hadoopConf: Configuration,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = {
    if (path == null || !path.startsWith("oss://")) return
    val withoutScheme = path.stripPrefix("oss://")
    val slash = withoutScheme.indexOf('/')
    val bucket =
      if (slash < 0) withoutScheme else withoutScheme.substring(0, slash)
    if (bucket.isEmpty) return

    val endpoint =
      if (isSource) config.sourceS3Endpoint.getOrElse(config.s3Endpoint)
      else config.s3Endpoint
    val accessKey =
      if (isSource) config.sourceS3AccessKey.getOrElse(config.s3AccessKey)
      else config.s3AccessKey
    val secretKey =
      if (isSource) config.sourceS3SecretKey.getOrElse(config.s3SecretKey)
      else config.s3SecretKey
    val useSSL =
      if (isSource) config.sourceS3UseSSL.getOrElse(config.s3UseSSL)
      else config.s3UseSSL
    val useIam =
      if (isSource) config.sourceS3UseIam.getOrElse(config.s3UseIam)
      else config.s3UseIam
    hadoopConf.set(
      "fs.oss.impl",
      "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem"
    )
    if (endpoint != null && endpoint.nonEmpty)
      hadoopConf.set("fs.oss.endpoint", endpoint)
    if (useSSL) hadoopConf.set("fs.oss.connection.secure.enabled", "true")
    else hadoopConf.unset("fs.oss.connection.secure.enabled")
    if (useIam) {
      val provider = Option(
        hadoopConf.getTrimmed(BackfillConfig.HadoopOssCredentialsProvider)
      ).filter(_.nonEmpty)
      if (provider.isEmpty) {
        throw new IllegalArgumentException(
          s"${BackfillConfig.HadoopOssCredentialsProvider} must be configured for OSS IAM access"
        )
      }
      hadoopConf.unset("fs.oss.accessKeyId")
      hadoopConf.unset("fs.oss.accessKeySecret")
    } else {
      hadoopConf.set("fs.oss.accessKeyId", accessKey)
      hadoopConf.set("fs.oss.accessKeySecret", secretKey)
      hadoopConf.unset(BackfillConfig.HadoopOssSecurityToken)
      // hadoop-aliyun constructs AliyunCredentialsProvider itself from the
      // accessKeyId/accessKeySecret properties. The class has no (URI,
      // Configuration) or no-arg constructor, so configuring it explicitly
      // causes filesystem initialization to fail.
      hadoopConf.unset(BackfillConfig.HadoopOssCredentialsProvider)
    }
  }

  private[backfill] def configureHadoopOssForPath(
      spark: SparkSession,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = configureHadoopOssForPath(
    spark.sparkContext.hadoopConfiguration,
    path,
    config,
    isSource
  )

  /** Configure Hadoop S3A credentials for the bucket referenced by `path`.
    *
    * Uses per-bucket keys (`fs.s3a.bucket.<bucket>.*`) so that the backfill
    * *source* bucket and the Milvus storage bucket (snapshot / segments) can
    * each use their own endpoint and credentials within the same Spark session.
    * No-op for non-S3 paths.
    *
    * @param isSource
    *   true when configuring the backfill input bucket — in that case we
    *   consult the `sourceS3*` overrides and fall back to the main credentials
    *   when a particular field is unset.
    */
  private[backfill] def configureHadoopS3ForPath(
      hadoopConf: Configuration,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = {
    if (path == null) return
    if (!(path.startsWith("s3://") || path.startsWith("s3a://"))) return

    // Extract bucket name from s3(a)://bucket/key
    val withoutScheme = path.stripPrefix("s3a://").stripPrefix("s3://")
    val bucket = {
      val slash = withoutScheme.indexOf('/')
      if (slash < 0) withoutScheme else withoutScheme.substring(0, slash)
    }
    if (bucket.isEmpty) return

    // Resolve the effective credentials for this bucket. For the source bucket
    // any unset override falls back to the main credentials, preserving the
    // existing single-bucket behavior.
    val endpoint =
      if (isSource) config.sourceS3Endpoint.getOrElse(config.s3Endpoint)
      else config.s3Endpoint
    val accessKey =
      if (isSource) config.sourceS3AccessKey.getOrElse(config.s3AccessKey)
      else config.s3AccessKey
    val secretKey =
      if (isSource) config.sourceS3SecretKey.getOrElse(config.s3SecretKey)
      else config.s3SecretKey
    val useSSL =
      if (isSource) config.sourceS3UseSSL.getOrElse(config.s3UseSSL)
      else config.s3UseSSL
    val useIam =
      if (isSource) config.sourceS3UseIam.getOrElse(config.s3UseIam)
      else config.s3UseIam
    val region =
      if (isSource) config.sourceS3Region.getOrElse(config.s3Region)
      else config.s3Region

    val prefix = s"fs.s3a.bucket.$bucket"

    val tencentRole =
      if (useIam) HadoopStorageKeys.resolveTencentRole(hadoopConf, bucket)
      else None
    // Source overrides may point at another cloud. Its provider/endpoint,
    // rather than the target bucket's cloud alone, determines COS behavior.
    val endpointHost = Option(endpoint)
      .getOrElse("")
      .stripPrefix("https://")
      .stripPrefix("http://")
    val isTencent = tencentRole.nonEmpty ||
      (config.s3CloudProvider.trim == "tencent" && (!isSource || config.sourceS3Endpoint.isEmpty)) ||
      (endpointHost.startsWith("cos.") && endpointHost.endsWith(
        ".myqcloud.com"
      ))
    require(
      !useIam || !isTencent || tencentRole.nonEmpty,
      "Tencent IAM storage requires TencentS3RoleCredentialsProvider in Hadoop configuration"
    )

    // Endpoint + path style + SSL are safe to set in both IAM and static modes
    if (endpoint != null && endpoint.nonEmpty) {
      hadoopConf.set(s"$prefix.endpoint", endpoint)
    }
    if (region != null && region.nonEmpty) {
      // Newer hadoop-aws (3.3.x+) reads endpoint.region; set both keys for
      // compatibility with older versions that only honor `region`.
      hadoopConf.set(s"$prefix.endpoint.region", region)
      hadoopConf.set(s"$prefix.region", region)
    }
    hadoopConf.set(s"$prefix.path.style.access", (!isTencent).toString)
    if (isTencent) {
      hadoopConf.set(s"$prefix.bucket.probe", "0")
      hadoopConf.set(s"$prefix.multiobjectdelete.enable", "false")
    }
    hadoopConf.set(
      s"$prefix.connection.ssl.enabled",
      if (useSSL) "true" else "false"
    )

    if (useIam) {
      val bucketProviderKey = s"$prefix.aws.credentials.provider"
      val assumedRole =
        if (tencentRole.nonEmpty) None
        else BackfillConfig.resolveAwsS3AssumeRole(hadoopConf, bucket)

      // Build an explicit IRSA/EKS-friendly provider chain instead of the
      // v1 DefaultAWSCredentialsProviderChain, which has historically been
      // unreliable on EKS pods (it does not always pick up the projected
      // service-account web-identity token before falling back to the
      // EC2 instance profile of the node — leaking the node's role).
      //
      // Order matters:
      //   1. WebIdentityTokenCredentialsProvider — IRSA / GKE Workload Identity
      //   2. EnvironmentVariableCredentialsProvider — local dev / CI overrides
      //   3. IAMInstanceCredentialsProvider — EC2 / EKS node role fallback
      // A managed runtime may already have selected an AssumeRole provider
      // globally for its data role or per bucket for an external volume. Do
      // not replace that security boundary with the pod's ambient identity.
      if (assumedRole.isEmpty && tencentRole.isEmpty) {
        hadoopConf.set(
          bucketProviderKey,
          Seq(
            "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider",
            "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider",
            "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
          ).mkString(",")
        )
      }
    } else {
      hadoopConf.set(s"$prefix.access.key", accessKey)
      hadoopConf.set(s"$prefix.secret.key", secretKey)
      // Force the simple static-credentials provider for this bucket so it
      // doesn't get shadowed by a globally configured provider chain.
      hadoopConf.set(
        s"$prefix.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
    }

    logger.info(
      s"Configured Hadoop S3A for bucket '$bucket' " +
        s"(endpoint=$endpoint, useIam=$useIam, isSource=$isSource)"
    )
  }

  private[backfill] def configureHadoopS3ForPath(
      spark: SparkSession,
      path: String,
      config: BackfillConfig,
      isSource: Boolean
  ): Unit = configureHadoopS3ForPath(
    spark.sparkContext.hadoopConfiguration,
    path,
    config,
    isSource
  )

  /** Read snapshot JSON content from S3 or local file system. Returns the JSON
    * string.
    */
  private[backfill] def readSnapshotJson(
      snapshotPath: String,
      config: BackfillConfig
  ): Either[BackfillError, String] = {
    if (snapshotPath == null || snapshotPath.isEmpty) {
      return Right("") // Empty path means use client fallback
    }

    try {
      // A standard object-store URI lives in its named bucket. A path without
      // a scheme and file:// remain local CLI input.
      val local = !snapshotPath.contains("://") ||
        snapshotPath.startsWith("file://")
      if (!local) {

        // The snapshot JSON is read the way every other driver-side read
        // goes: through the native store the fs.* options describe, at the
        // (bucket, key) named by the user-supplied URI. Spark's Hadoop
        // filesystem is not involved.
        // The path is what Milvus printed for the snapshot, so the
        // scheme://endpoint/bucket/key form is accepted when the host is the
        // configured endpoint, as SnapshotCatalog.read accepts it.
        val located = StoragePath.parseMilvus(
          snapshotPath,
          config.s3BucketName,
          config.s3Endpoint
        )
        val properties = StorageProperties.from(
          config.getMilvusReadOptions ++ Map(
            StorageProperties.BucketName -> located.bucket
          )
        )
        val store = NativeObjectStore.Factory(properties).open()
        val json =
          try
            new String(
              store.readAll(located.key),
              StandardCharsets.UTF_8
            )
          finally store.close()

        Right(json)

      } else {
        // Local file path, read directly
        val source =
          Source.fromFile(snapshotPath.stripPrefix("file://"))
        try {
          val json = source.mkString
          Right(json)
        } finally {
          source.close()
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to read snapshot JSON: ${e.getMessage}", e)
        Left(
          DataReadError(
            snapshotPath,
            s"Failed to read snapshot file: ${e.getMessage}",
            Some(e)
          )
        )
    }
  }
}
