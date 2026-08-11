package com.zilliz.spark.connector.operations.backfill

import org.apache.hadoop.conf.Configuration

import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.MilvusOption

/** Configuration for backfill operation
  *
  * @param milvusUri
  *   Milvus server URI (e.g., "http://localhost:19530")
  * @param milvusToken
  *   Authentication token in format "username:password"
  * @param databaseName
  *   Milvus database name
  * @param collectionName
  *   Milvus collection name to backfill
  * @param partitionName
  *   Optional specific partition name to backfill
  * @param s3Endpoint
  *   S3/Minio endpoint (e.g., "localhost:9000")
  * @param s3BucketName
  *   S3 bucket name
  * @param s3AccessKey
  *   S3 access key ID
  * @param s3SecretKey
  *   S3 secret access key
  * @param s3UseSSL
  *   Whether to use SSL for S3 connections
  * @param s3RootPath
  *   Root path in S3 bucket
  * @param s3Region
  *   S3 region
  * @param s3CloudProvider
  *   Native storage provider for the Milvus storage bucket
  * @param batchSize
  *   Batch size for writing data
  * @param customOutputPath
  *   Optional custom output path override
  * @param joinKey
  *   Row identity used to match snapshot rows with backfill input rows
  */
case class BackfillConfig(
    // Milvus connection (optional for snapshot-only mode)
    milvusUri: String = "",
    milvusToken: String = "",
    databaseName: String = "default",
    collectionName: String = "",
    partitionName: Option[String] = None,

    // S3 storage configuration
    s3Endpoint: String,
    s3BucketName: String,
    s3AccessKey: String,
    s3SecretKey: String,
    s3UseSSL: Boolean = false,
    s3RootPath: String = "files",
    s3Region: String = "us-east-1",
    s3CloudProvider: String = BackfillConfig.DefaultCloudProvider,
    // When true, both Milvus FFI and Spark Hadoop S3A use the default credentials
    // chain (env / web identity / instance profile) instead of static AK/SK.
    s3UseIam: Boolean = false,

    // Optional separate credentials for the backfill *input* parquet bucket.
    // When set, these are used (via Hadoop per-bucket S3A config) to read the
    // backfill data parquet, while the main s3* fields above continue to be
    // used for snapshot reads and segment writes (Milvus storage bucket).
    // Leave as None to reuse the main credentials for both buckets.
    sourceS3Endpoint: Option[String] = None,
    sourceS3AccessKey: Option[String] = None,
    sourceS3SecretKey: Option[String] = None,
    sourceS3UseSSL: Option[Boolean] = None,
    sourceS3UseIam: Option[Boolean] = None,
    sourceS3Region: Option[String] = None,

    // Writer configuration
    batchSize: Int = 1024,
    customOutputPath: Option[String] = None,

    // Optional mapping: parquet column name -> Milvus field name.
    // When set, the backfill parquet is reprojected through this map before
    // join/write: parquet columns not listed as keys are dropped, and each
    // listed column is renamed to its target. The value set must contain the
    // configured join field so the identity column can be consumed after
    // renaming. With the default primary-key join and no mapping, legacy
    // behavior applies: the parquet must contain a literal "pk" column plus
    // one or more field columns. With a physical join field and no mapping,
    // the parquet must contain that exact field name.
    columnMapping: Option[Map[String, String]] = None,

    // Merge mode for backfill values:
    //   "coalesce" (default) — read the target field's current value from
    //     source and pick coalesce(src, parquet) per row per field (source
    //     wins when non-null; parquet fills nulls). Unmatched source rows
    //     keep their original target values.
    //   "overwrite" — parquet overrides the target field for rows whose join
    //     key matches (null included). Unmatched source rows keep their
    //     original target values.
    //   "replace" — parquet is the absolute source of truth: every source
    //     row's target field becomes the parquet value (null if the join key
    //     has no parquet match). Destructive on unmatched rows.
    mode: String = MilvusOption.BackfillModeCoalesce,

    // Optional main-storage AssumeRole settings. BackfillApp derives these
    // from an existing Hadoop S3A configuration when the runtime platform has
    // already selected a data role for the job.
    s3RoleArn: Option[String] = None,
    s3RoleSessionName: Option[String] = None,
    s3ExternalId: Option[String] = None,

    // Row identity used to join snapshot rows with backfill data. The primary
    // key remains the default for backward compatibility.
    joinKey: BackfillJoinKey = BackfillJoinKey.PrimaryKey
) {

  /** Whether the merge path needs to read each target field from the source
    * side too. Coalesce and overwrite both compare source and parquet values
    * per row at join time; replace takes parquet verbatim and therefore only
    * needs the join key + segment tracking columns from source.
    */
  def readsSourceFields: Boolean =
    mode != MilvusOption.BackfillModeReplace

  /** Validate S3 and writer configuration (always required)
    */
  def validate(): Either[String, Unit] = {
    val normalizedCloudProvider = s3CloudProvider.trim
    val normalizedRoleArn = s3RoleArn.map(_.trim).filter(_.nonEmpty)
    val hasRoleDetails =
      s3RoleSessionName.exists(_.trim.nonEmpty) || s3ExternalId.exists(
        _.trim.nonEmpty
      )

    if (s3Endpoint.isEmpty) {
      Left("s3Endpoint cannot be empty")
    } else if (s3BucketName.isEmpty) {
      Left("s3BucketName cannot be empty")
    } else if (
      !BackfillConfig.AllowedCloudProviders.contains(normalizedCloudProvider)
    ) {
      Left(
        s"s3CloudProvider must be one of ${BackfillConfig.AllowedCloudProviders
            .mkString("[", ", ", "]")} (got '$s3CloudProvider')"
      )
    } else if (batchSize <= 0) {
      Left("batchSize must be positive")
    } else if (
      mode != MilvusOption.BackfillModeReplace &&
      mode != MilvusOption.BackfillModeCoalesce &&
      mode != MilvusOption.BackfillModeOverwrite
    ) {
      Left(
        s"mode must be one of '${MilvusOption.BackfillModeReplace}', " +
          s"'${MilvusOption.BackfillModeCoalesce}', " +
          s"'${MilvusOption.BackfillModeOverwrite}' (got '$mode')"
      )
    } else if (
      joinKey match {
        case BackfillJoinKey.PhysicalField(name) =>
          Option(name).forall(_.trim.isEmpty)
        case _ => false
      }
    ) {
      Left("physical join-key field name cannot be blank")
    } else if (!s3UseIam && (s3AccessKey.isEmpty || s3SecretKey.isEmpty)) {
      // Hard invariant: must use IAM or supply both AK and SK. Half-set
      // static credentials are never valid — they would silently fall back
      // to the default provider chain and mask config mistakes.
      Left(
        "s3AccessKey and s3SecretKey must both be set unless s3UseIam=true"
      )
    } else if (normalizedRoleArn.nonEmpty && !s3UseIam) {
      Left("s3RoleArn requires s3UseIam=true")
    } else if (
      normalizedRoleArn.nonEmpty &&
      !BackfillConfig.NativeAssumeRoleCloudProviders.contains(
        normalizedCloudProvider
      )
    ) {
      Left(
        s"s3RoleArn is supported only when s3CloudProvider is one of " +
          s"${BackfillConfig.NativeAssumeRoleCloudProviders.mkString("[", ", ", "]")} " +
          s"(got '$s3CloudProvider')"
      )
    } else if (normalizedRoleArn.isEmpty && hasRoleDetails) {
      Left("s3RoleSessionName and s3ExternalId require s3RoleArn")
    } else {
      // Same invariant for the source (input parquet) bucket. Any field
      // left as None falls back to the main credentials, which we already
      // validated above, so we only fail when an asymmetric override would
      // produce half-set static credentials.
      val srcUseIam = sourceS3UseIam.getOrElse(s3UseIam)
      val srcAk = sourceS3AccessKey.getOrElse(s3AccessKey)
      val srcSk = sourceS3SecretKey.getOrElse(s3SecretKey)
      if (!srcUseIam && (srcAk.isEmpty || srcSk.isEmpty)) {
        Left(
          "source bucket: sourceS3AccessKey and sourceS3SecretKey must both " +
            "be set unless sourceS3UseIam=true (or fall back to main)"
        )
      } else {
        Right(())
      }
    }
  }

  /** Validate that Milvus client connection config is present (required when no
    * snapshot)
    */
  def validateForClientMode(): Either[String, Unit] = {
    validate().flatMap { _ =>
      if (milvusUri.isEmpty)
        Left(
          "milvusUri cannot be empty (required when no snapshot is provided)"
        )
      else if (collectionName.isEmpty)
        Left(
          "collectionName cannot be empty (required when no snapshot is provided)"
        )
      else Right(())
    }
  }

  /** Get base Milvus read options as a Map for DataSource. The resolved join
    * field IDs are added by `MilvusBackfill` to minimize data transfer.
    */
  def getMilvusReadOptions: Map[String, String] = {
    var options = withS3Authentication(
      Map(
        "milvus.uri" -> milvusUri,
        "milvus.token" -> milvusToken,
        "milvus.database.name" -> databaseName,
        "milvus.collection.name" -> collectionName,
        MilvusOption.MilvusExtraColumns -> Seq(
          MilvusOption.MilvusExtraColumnSegmentID,
          MilvusOption.MilvusExtraColumnRowOffset
        ).mkString(","),
        "fs.address" -> s3Endpoint,
        "fs.bucket_name" -> s3BucketName,
        "fs.root_path" -> s3RootPath,
        "fs.use_ssl" -> s3UseSSL.toString,
        Properties.FsConfig.FsCloudProvider -> s3CloudProvider.trim,
        "fs.use_iam" -> s3UseIam.toString
      )
    )

    // Add optional configurations
    partitionName.foreach(p =>
      options = options + ("milvus.partition.name" -> p)
    )

    options
  }

  /** Get S3 write options as a Map for MilvusLoonWriter
    */
  def getS3WriteOptions(
      collectionId: Long,
      partitionId: Long,
      segmentId: Long,
      fieldNameToId: Map[String, Long] = Map.empty
  ): Map[String, String] = {
    val outputPath = customOutputPath.getOrElse(
      s"$s3RootPath/insert_log/$collectionId/$partitionId/$segmentId"
    )
    getS3WriteOptionsForBasePath(outputPath, segmentId, fieldNameToId)
  }

  /** Get S3 write options using a specific segment base path (e.g., from
    * manifest)
    */
  def getS3WriteOptionsForBasePath(
      segmentBasePath: String,
      segmentId: Long,
      fieldNameToId: Map[String, Long] = Map.empty
  ): Map[String, String] = {
    var opts = withS3Authentication(
      Map(
        "fs.storage_type" -> "remote",
        "fs.address" -> s3Endpoint,
        "fs.bucket_name" -> s3BucketName,
        "fs.root_path" -> s3RootPath,
        "fs.use_ssl" -> s3UseSSL.toString,
        "fs.use_iam" -> s3UseIam.toString,
        "fs.region" -> s3Region,
        Properties.FsConfig.FsCloudProvider -> s3CloudProvider.trim,
        "milvus.collection.name" -> s"segment_${segmentId}_backfill",
        "milvus.writer.customPath" -> segmentBasePath,
        "milvus.writer.commitType" -> "addfield",
        "milvus.insertMaxBatchSize" -> batchSize.toString
      )
    )
    // Pass field name -> field ID mapping for correct column naming
    if (fieldNameToId.nonEmpty) {
      opts = opts + ("milvus.writer.fieldIds" -> fieldNameToId
        .map { case (k, v) => s"$k:$v" }
        .mkString(","))
    }
    opts
  }

  private[backfill] def withHadoopStorageAssumeRole(
      hadoopConf: Configuration,
      defaultSessionName: String
  ): BackfillConfig = {
    val provider = s3CloudProvider.trim
    if (!s3UseIam || s3RoleArn.exists(_.trim.nonEmpty)) {
      this
    } else if (provider == "aws") {
      withAwsS3AssumeRole(hadoopConf, defaultSessionName)
    } else if (provider == "aliyun") {
      withAlibabaOssAssumeRole(hadoopConf, defaultSessionName)
    } else {
      this
    }
  }

  private def withAwsS3AssumeRole(
      hadoopConf: Configuration,
      defaultSessionName: String
  ): BackfillConfig = {
    BackfillConfig
      .resolveAwsS3AssumeRole(hadoopConf, s3BucketName)
      .map { role =>
        withNativeAssumeRole(
          role.roleArn,
          role.sessionName,
          role.externalId,
          defaultSessionName
        )
      }
      .getOrElse(this)
  }

  private def withAlibabaOssAssumeRole(
      hadoopConf: Configuration,
      defaultSessionName: String
  ): BackfillConfig = {
    val provider = Option(
      hadoopConf.getTrimmed(BackfillConfig.HadoopOssCredentialsProvider)
    ).getOrElse("")
    val roleArn = Option(
      hadoopConf.getTrimmed(BackfillConfig.HadoopOssAssumedRoleArn)
    ).filter(_.nonEmpty)

    if (roleArn.isEmpty && BackfillConfig.isOssAssumeRoleProvider(provider)) {
      throw new IllegalArgumentException(
        s"${BackfillConfig.HadoopOssAssumedRoleArn} must be set when " +
          s"${BackfillConfig.HadoopOssCredentialsProvider} uses " +
          BackfillConfig.HadoopOssAssumedRoleProvider
      )
    }

    roleArn
      .map { arn =>
        withNativeAssumeRole(
          arn,
          Option(
            hadoopConf.getTrimmed(
              BackfillConfig.HadoopOssAssumedRoleSessionName
            )
          ).filter(_.nonEmpty),
          Option(
            hadoopConf.getTrimmed(BackfillConfig.HadoopOssAssumedRoleExternalId)
          ).filter(_.nonEmpty),
          defaultSessionName
        )
      }
      .getOrElse(this)
  }

  private def withNativeAssumeRole(
      roleArn: String,
      sessionName: Option[String],
      externalId: Option[String],
      defaultSessionName: String
  ): BackfillConfig = {
    copy(
      s3RoleArn = Some(roleArn),
      s3RoleSessionName = sessionName
        .map(BackfillConfig.normalizeRoleSessionName)
        .orElse(
          Some(BackfillConfig.normalizeRoleSessionName(defaultSessionName))
        ),
      s3ExternalId = externalId
    )
  }

  private def withS3Authentication(
      options: Map[String, String]
  ): Map[String, String] = {
    val credentialOptions = if (s3UseIam) {
      options
    } else {
      options ++ Map(
        Properties.FsConfig.FsAccessKeyId -> s3AccessKey,
        Properties.FsConfig.FsAccessKeyValue -> s3SecretKey
      )
    }

    Seq(
      Properties.FsConfig.FsRoleArn -> s3RoleArn,
      Properties.FsConfig.FsSessionName -> s3RoleSessionName,
      Properties.FsConfig.FsExternalId -> s3ExternalId
    ).foldLeft(credentialOptions) { case (result, (key, value)) =>
      value.map(_.trim).filter(_.nonEmpty) match {
        case Some(normalized) => result + (key -> normalized)
        case None             => result
      }
    }
  }
}

object BackfillConfig {

  private[backfill] final case class ResolvedAssumeRole(
      roleArn: String,
      sessionName: Option[String],
      externalId: Option[String]
  )

  private[backfill] val DefaultCloudProvider = "aws"
  private[backfill] val AllowedCloudProviders =
    Set("aws", "gcp", "aliyun", "azure", "tencent", "huawei")
  private[backfill] val NativeAssumeRoleCloudProviders = Set("aws", "aliyun")

  private[backfill] val HadoopS3CredentialsProvider =
    "fs.s3a.aws.credentials.provider"
  private[backfill] val HadoopS3AssumedRoleArn =
    "fs.s3a.assumed.role.arn"
  private[backfill] val HadoopS3AssumedRoleSessionName =
    "fs.s3a.assumed.role.session.name"
  private[backfill] val HadoopS3AssumedRoleExternalId =
    "fs.s3a.assumed.role.external.id"
  private[backfill] val HadoopS3AssumedRoleProvider =
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
  private[backfill] val HadoopOssAssumedRoleArn =
    "fs.oss.assumed.role.arn"
  private[backfill] val HadoopOssCredentialsProvider =
    "fs.oss.credentials.provider"
  private[backfill] val HadoopOssAssumedRoleSessionName =
    "fs.oss.assumed.role.session.name"
  private[backfill] val HadoopOssAssumedRoleExternalId =
    "fs.oss.assumed.role.external.id"
  private[backfill] val HadoopOssAssumedRoleProvider =
    "com.zilliz.cloud.hadoop.AliyunOSSRoleCredentialsProvider"

  private[backfill] def isAssumedRoleProvider(provider: String): Boolean =
    provider
      .split(',')
      .exists(_.trim == HadoopS3AssumedRoleProvider)

  private[backfill] def isOssAssumeRoleProvider(provider: String): Boolean =
    provider
      .split(',')
      .exists(_.trim == HadoopOssAssumedRoleProvider)

  private[backfill] def resolveAwsS3AssumeRole(
      hadoopConf: Configuration,
      bucketName: String
  ): Option[ResolvedAssumeRole] = {
    val bucketPrefix = s"fs.s3a.bucket.$bucketName"

    def getTrimmed(key: String): Option[String] =
      Option(hadoopConf.getTrimmed(key)).filter(_.nonEmpty)

    def bucketOrGlobal(bucketKey: String, globalKey: String): Option[String] =
      getTrimmed(bucketKey).orElse(getTrimmed(globalKey))

    val provider = bucketOrGlobal(
      s"$bucketPrefix.aws.credentials.provider",
      HadoopS3CredentialsProvider
    )
    if (!provider.exists(isAssumedRoleProvider)) return None

    val roleArn = bucketOrGlobal(
      s"$bucketPrefix.assumed.role.arn",
      HadoopS3AssumedRoleArn
    ).getOrElse {
      throw new IllegalArgumentException(
        s"Effective AWS S3A AssumeRole configuration for bucket '$bucketName' " +
          s"requires either $bucketPrefix.assumed.role.arn or " +
          s"$HadoopS3AssumedRoleArn"
      )
    }

    Some(
      ResolvedAssumeRole(
        roleArn = roleArn,
        sessionName = bucketOrGlobal(
          s"$bucketPrefix.assumed.role.session.name",
          HadoopS3AssumedRoleSessionName
        ),
        externalId = bucketOrGlobal(
          s"$bucketPrefix.assumed.role.external.id",
          HadoopS3AssumedRoleExternalId
        )
      )
    )
  }

  private[backfill] def normalizeRoleSessionName(value: String): String = {
    val normalized = Option(value)
      .getOrElse("")
      .replaceAll("[^A-Za-z0-9+=,.@-]", "-")
    val nonEmpty = if (normalized.nonEmpty) normalized else "spark-backfill"
    nonEmpty.take(64)
  }

  /** Create a minimal config for testing
    */
  def forTest(
      collectionName: String,
      milvusUri: String = "http://localhost:19530",
      milvusToken: String = "root:Milvus",
      s3Endpoint: String = "localhost:9000",
      s3BucketName: String = "a-bucket"
  ): BackfillConfig = {
    BackfillConfig(
      milvusUri = milvusUri,
      milvusToken = milvusToken,
      collectionName = collectionName,
      s3Endpoint = s3Endpoint,
      s3BucketName = s3BucketName,
      s3AccessKey = "minioadmin",
      s3SecretKey = "minioadmin"
    )
  }
}
