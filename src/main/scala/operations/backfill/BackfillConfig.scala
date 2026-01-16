package com.zilliz.spark.connector.operations.backfill

/**
 * Configuration for backfill operation
 *
 * @param milvusUri Milvus server URI (e.g., "http://localhost:19530")
 * @param milvusToken Authentication token in format "username:password"
 * @param databaseName Milvus database name
 * @param collectionName Milvus collection name to backfill
 * @param partitionName Optional specific partition name to backfill
 * @param s3Endpoint S3/Minio endpoint (e.g., "localhost:9000")
 * @param s3BucketName S3 bucket name
 * @param s3AccessKey S3 access key ID
 * @param s3SecretKey S3 secret access key
 * @param s3UseSSL Whether to use SSL for S3 connections
 * @param s3RootPath Root path in S3 bucket
 * @param s3Region S3 region
 * @param batchSize Batch size for writing data
 * @param customOutputPath Optional custom output path override
 */
case class BackfillConfig(
    // Milvus connection
    milvusUri: String,
    milvusToken: String = "",
    databaseName: String = "default",
    collectionName: String,
    partitionName: Option[String] = None,

    // S3 storage configuration
    s3Endpoint: String,
    s3BucketName: String,
    s3AccessKey: String,
    s3SecretKey: String,
    s3UseSSL: Boolean = false,
    s3RootPath: String = "files",
    s3Region: String = "us-east-1",

    // Writer configuration
    batchSize: Int = 1024,
    customOutputPath: Option[String] = None
) {
  /**
   * Validate that all required fields are set
   */
  def validate(): Either[String, Unit] = {
    if (milvusUri.isEmpty) {
      Left("milvusUri cannot be empty")
    } else if (collectionName.isEmpty) {
      Left("collectionName cannot be empty")
    } else if (s3Endpoint.isEmpty) {
      Left("s3Endpoint cannot be empty")
    } else if (s3BucketName.isEmpty) {
      Left("s3BucketName cannot be empty")
    } else if (s3AccessKey.isEmpty) {
      Left("s3AccessKey cannot be empty")
    } else if (s3SecretKey.isEmpty) {
      Left("s3SecretKey cannot be empty")
    } else if (batchSize <= 0) {
      Left("batchSize must be positive")
    } else {
      Right(())
    }
  }

  /**
   * Get Milvus read options as a Map for DataSource
   * Only reads the primary key field to minimize data transfer for join operation
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
      "fs.access_key_id" -> s3AccessKey,
      "fs.access_key_value" -> s3SecretKey,
      "fs.use_ssl" -> s3UseSSL.toString
    )

    // Add optional configurations
    partitionName.foreach(p => options = options + ("milvus.partition.name" -> p))

    options
  }

  /**
   * Get S3 write options as a Map for MilvusLoonWriter
   */
  def getS3WriteOptions(collectionId: Long, partitionId: Long, segmentId: Long): Map[String, String] = {
    // TODO: this should be changed to field id based on the collection schema once Milvus snapshot feature is ready
    // Note: bucket name is configured separately via fs.bucket_name, so outputPath should NOT include bucket
    val outputPath = customOutputPath.getOrElse(
      s"$s3RootPath/insert_log/$collectionId/$partitionId/$segmentId/new_field"
    )

    Map(
      "fs.storage_type" -> "remote",
      "fs.address" -> s3Endpoint,
      "fs.bucket_name" -> s3BucketName,
      "fs.root_path" -> s3RootPath,
      "fs.access_key_id" -> s3AccessKey,
      "fs.access_key_value" -> s3SecretKey,
      "fs.use_ssl" -> s3UseSSL.toString,
      "fs.region" -> s3Region,
      "milvus.collection.name" -> s"segment_${segmentId}_backfill",
      "milvus.writer.customPath" -> outputPath,
      "milvus.insertMaxBatchSize" -> batchSize.toString
    )
  }
}

object BackfillConfig {
  /**
   * Create a minimal config for testing
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
