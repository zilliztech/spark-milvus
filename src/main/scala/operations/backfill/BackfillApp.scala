package com.zilliz.spark.connector.operations.backfill

import org.apache.spark.sql.SparkSession

/** Spark application entry point for running backfill via spark-submit.
  *
  * Usage: spark-submit --class
  * com.zilliz.spark.connector.operations.backfill.BackfillApp \
  * spark-connector-assembly.jar \ --parquet <path> --snapshot <path> \
  * --s3-endpoint <endpoint> --s3-bucket <bucket> \ --s3-access-key <key>
  * --s3-secret-key <secret> \ [--s3-root-path <path>] [--s3-region <region>]
  * [--s3-use-ssl] \ [--batch-size <n>] [--output-result <path>]
  */
object BackfillApp {

  def main(args: Array[String]): Unit = {
    val parsed = parseArgs(args)

    val parquetPath = parsed.getOrElse(
      "parquet",
      throw new IllegalArgumentException("--parquet is required")
    )
    val snapshotPath = parsed.getOrElse(
      "snapshot",
      throw new IllegalArgumentException("--snapshot is required")
    )
    val s3Endpoint = parsed.getOrElse(
      "s3-endpoint",
      throw new IllegalArgumentException("--s3-endpoint is required")
    )
    val s3Bucket = parsed.getOrElse(
      "s3-bucket",
      throw new IllegalArgumentException("--s3-bucket is required")
    )
    // Optional in IAM/IRSA mode — when both empty, automatically enable use_iam so that
    // both Milvus FFI and Spark Hadoop S3A defer to the default credentials chain
    // (env vars, instance profile, web identity token, etc.).
    val s3AccessKey = parsed.getOrElse("s3-access-key", "")
    val s3SecretKey = parsed.getOrElse("s3-secret-key", "")
    val useIam =
      parsed.contains("use-iam") || (s3AccessKey.isEmpty && s3SecretKey.isEmpty)

    // Optional separate credentials for the backfill input (parquet) bucket.
    // When unset, the main s3-* credentials above are reused.
    val sourceUseIamFlag =
      if (parsed.contains("source-use-iam")) Some(true)
      else if (
        parsed.contains("source-s3-access-key") || parsed.contains(
          "source-s3-secret-key"
        )
      ) Some(false)
      else None

    val config = BackfillConfig(
      s3Endpoint = s3Endpoint,
      s3BucketName = s3Bucket,
      s3AccessKey = s3AccessKey,
      s3SecretKey = s3SecretKey,
      s3UseSSL = parsed.contains("s3-use-ssl"),
      s3RootPath = parsed.getOrElse("s3-root-path", "files"),
      s3Region = parsed.getOrElse("s3-region", "us-east-1"),
      s3UseIam = useIam,
      sourceS3Endpoint = parsed.get("source-s3-endpoint"),
      sourceS3AccessKey = parsed.get("source-s3-access-key"),
      sourceS3SecretKey = parsed.get("source-s3-secret-key"),
      sourceS3UseSSL =
        if (parsed.contains("source-s3-use-ssl")) Some(true) else None,
      sourceS3UseIam = sourceUseIamFlag,
      sourceS3Region = parsed.get("source-s3-region"),
      batchSize = parsed.getOrElse("batch-size", "1024").toInt
    )

    val spark = SparkSession.builder
      .appName("MilvusBackfill")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      MilvusBackfill.run(spark, parquetPath, snapshotPath, config) match {
        case Right(result) =>
          println(result.summary)
          println(result.segmentSummary)
          parsed.get("output-result").foreach { outputPath =>
            MilvusBackfill
              .writeResultJson(spark, result, outputPath, config) match {
              case Right(_) =>
                println(s"Result JSON written to: $outputPath")
              case Left(err) =>
                System.err.println(
                  s"Backfill succeeded but writing result JSON FAILED: ${err.message}"
                )
                System.exit(2)
            }
          }
        case Left(error) =>
          System.err.println(s"Backfill FAILED: ${error.message}")
          System.exit(1)
      }
    } finally {
      spark.stop()
    }
  }

  // Boolean flags do not consume the next token.
  private[backfill] val BoolFlags: Set[String] = Set(
    "s3-use-ssl",
    "use-iam",
    "source-s3-use-ssl",
    "source-use-iam"
  )

  // All accepted CLI keys. Validating against this whitelist surfaces typos
  // (e.g. `--s3access-key` instead of `--s3-access-key`) at parse time
  // instead of silently absorbing them and producing a confusing failure
  // later inside the AWS default provider chain.
  private[backfill] val KvFlags: Set[String] = Set(
    "parquet",
    "snapshot",
    "s3-endpoint",
    "s3-bucket",
    "s3-access-key",
    "s3-secret-key",
    "s3-root-path",
    "s3-region",
    "source-s3-endpoint",
    "source-s3-access-key",
    "source-s3-secret-key",
    "source-s3-region",
    "batch-size",
    "output-result"
  )

  private[backfill] val KnownFlags: Set[String] = BoolFlags ++ KvFlags

  private[backfill] def parseArgs(args: Array[String]): Map[String, String] = {
    var map = Map.empty[String, String]
    var i = 0
    while (i < args.length) {
      args(i) match {
        case flag if flag.startsWith("--") =>
          val key = flag.stripPrefix("--")
          if (!KnownFlags.contains(key)) {
            throw new IllegalArgumentException(
              s"Unknown argument: $flag. Known flags: ${KnownFlags.toSeq.sorted
                  .map("--" + _)
                  .mkString(", ")}"
            )
          }
          if (BoolFlags.contains(key)) {
            map += (key -> "true")
            i += 1
          } else if (i + 1 < args.length) {
            map += (key -> args(i + 1))
            i += 2
          } else {
            throw new IllegalArgumentException(s"Missing value for $flag")
          }
        case other =>
          throw new IllegalArgumentException(s"Unexpected argument: $other")
      }
    }
    map
  }
}
