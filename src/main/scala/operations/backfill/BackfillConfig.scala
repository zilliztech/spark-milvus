package com.zilliz.spark.connector.operations.backfill

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
  * @param batchSize
  *   Batch size for writing data
  * @param customOutputPath
  *   Optional custom output path override
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
    customOutputPath: Option[String] = None
) {

  /** Validate S3 and writer configuration (always required)
    */
  def validate(): Either[String, Unit] = {
    if (s3Endpoint.isEmpty) {
      Left("s3Endpoint cannot be empty")
    } else if (s3BucketName.isEmpty) {
      Left("s3BucketName cannot be empty")
    } else if (batchSize <= 0) {
      Left("batchSize must be positive")
    } else if (!s3UseIam && (s3AccessKey.isEmpty || s3SecretKey.isEmpty)) {
      // Hard invariant: must use IAM or supply both AK and SK. Half-set
      // static credentials are never valid — they would silently fall back
      // to the default provider chain and mask config mistakes.
      Left(
        "s3AccessKey and s3SecretKey must both be set unless s3UseIam=true"
      )
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

  /** Get Milvus read options as a Map for DataSource Only reads the primary key
    * field to minimize data transfer for join operation
    */
  def getMilvusReadOptions: Map[String, String] = {
    var options = Map(
      "milvus.uri" -> milvusUri,
      "milvus.token" -> milvusToken,
      "milvus.database.name" -> databaseName,
      "milvus.collection.name" -> collectionName,
      "milvus.extra.columns" -> "segment_id,row_offset", // this is used to match with the original sequence of rows for each segment
      "fs.address" -> s3Endpoint,
      "fs.bucket_name" -> s3BucketName,
      "fs.root_path" -> s3RootPath,
      "fs.use_ssl" -> s3UseSSL.toString,
      "fs.use_iam" -> s3UseIam.toString
    )
    // Only inject static credentials when NOT in IAM mode. Under IRSA the FFI
    // must consult the AWS default credentials chain — passing fake AK/SK
    // (or the "minioadmin" defaults from Properties.scala) breaks signing.
    if (!s3UseIam) {
      options = options ++ Map(
        "fs.access_key_id" -> s3AccessKey,
        "fs.access_key_value" -> s3SecretKey
      )
    }

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
    var opts = Map(
      "fs.storage_type" -> "remote",
      "fs.address" -> s3Endpoint,
      "fs.bucket_name" -> s3BucketName,
      "fs.root_path" -> s3RootPath,
      "fs.use_ssl" -> s3UseSSL.toString,
      "fs.use_iam" -> s3UseIam.toString,
      "fs.region" -> s3Region,
      "milvus.collection.name" -> s"segment_${segmentId}_backfill",
      "milvus.writer.customPath" -> segmentBasePath,
      "milvus.writer.commitType" -> "addfield",
      "milvus.insertMaxBatchSize" -> batchSize.toString
    )
    // See getMilvusReadOptions: skip static credentials in IAM mode so the
    // FFI defers to the AWS default credentials chain instead of signing
    // with placeholder keys.
    if (!s3UseIam) {
      opts = opts ++ Map(
        "fs.access_key_id" -> s3AccessKey,
        "fs.access_key_value" -> s3SecretKey
      )
    }
    // Pass field name -> field ID mapping for correct column naming
    if (fieldNameToId.nonEmpty) {
      opts = opts + ("milvus.writer.fieldIds" -> fieldNameToId
        .map { case (k, v) => s"$k:$v" }
        .mkString(","))
    }
    opts
  }
}

object BackfillConfig {

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
