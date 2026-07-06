package com.zilliz.spark.connector.tools

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import com.zilliz.spark.connector.read.{
  MilvusSnapshotReader,
  V2SegmentInfo,
  V2SegmentLoader
}
import com.zilliz.spark.connector.MilvusOption

/** Standalone reader-only verification app.
  *
  * Loads a Milvus snapshot and reads the PK column via the V2 packed reader —
  * but WITHOUT running any join / shuffle / write. Then it diagnoses the raw
  * output:
  *   - total row count
  *   - distinct PK count
  *   - specific `row_offset → pk` samples
  *   - (optional) IDs at boundary positions
  *
  * If distinct PK count != total row count (but pyarrow says they are equal at
  * the parquet level), the V2 reader has a read-side duplication bug.
  *
  * Usage:
  * {{{
  *   spark-submit --class com.zilliz.spark.connector.tools.ReadSourceOnlyApp \
  *     <jar> \
  *     --snapshot s3://.../metadata.json \
  *     --s3-endpoint 127.0.0.1:9000 \
  *     --s3-bucket a-bucket \
  *     --s3-access-key minioadmin \
  *     --s3-secret-key minioadmin \
  *     --s3-root-path files
  * }}}
  */
object ReadSourceOnlyApp {

  def main(args: Array[String]): Unit = {
    val parsed = parseArgs(args)
    val snapshotPath = required(parsed, "snapshot")
    val s3Endpoint = parsed.getOrElse("s3-endpoint", "")
    val s3Bucket = required(parsed, "s3-bucket")
    val s3AccessKey = parsed.getOrElse("s3-access-key", "")
    val s3SecretKey = parsed.getOrElse("s3-secret-key", "")
    val s3Region = parsed.getOrElse("s3-region", "us-east-1")
    val s3RootPath = parsed.getOrElse("s3-root-path", "files")

    val spark = SparkSession.builder
      .appName("ReadSourceOnly")
      .getOrCreate()

    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      configureBucket(
        hadoopConf,
        s3Bucket,
        s3Endpoint,
        s3AccessKey,
        s3SecretKey,
        s3Region
      )

      val snapshotJson = new String(
        readAllBytes(hadoopConf, normalizeS3(snapshotPath)),
        "UTF-8"
      )
      val metadata =
        MilvusSnapshotReader.parseSnapshotMetadata(snapshotJson) match {
          case Right(m) => m
          case Left(e) =>
            throw new RuntimeException(
              s"failed to parse snapshot metadata: ${e.getMessage}"
            )
        }

      println(s"snapshot.id = ${metadata.snapshotInfo.id}")
      println(s"collection  = ${metadata.collection.schema.name}")

      val v2Segments: Seq[V2SegmentInfo] =
        V2SegmentLoader.loadV2Segments(
          metadata.manifestList,
          s3Bucket,
          hadoopConf,
          manifestSchemaVersion = metadata.manifestSchemaVersion
        ) match {
          case Right(segs) => segs
          case Left(err) =>
            throw new RuntimeException(
              s"failed to load v2 segments: ${err.getMessage}"
            )
        }

      println(s"v2 segments: ${v2Segments.size}")
      v2Segments.foreach { seg =>
        println(
          s"  segment=${seg.segmentId}, numOfRows=${seg.numOfRows}, " +
            s"columnGroups=${seg.columnGroups.size}"
        )
      }

      val pkField = metadata.collection.schema.fields
        .find(_.isPrimaryKey.getOrElse(false))
        .getOrElse(throw new RuntimeException("no PK field in schema"))
      val pkFieldId = pkField.getFieldIDAsLong
      val pkName = pkField.name
      println(s"pk field: $pkName (id=$pkFieldId)")

      val segmentIdCol = MilvusOption.MilvusExtraColumnSegmentID
      val rowOffsetCol = MilvusOption.MilvusExtraColumnRowOffset
      var options = Map[String, String](
        MilvusOption.SnapshotMode -> "true",
        "milvus.uri" -> "dummy://snapshot-mode",
        "milvus.collection.name" -> metadata.collection.schema.name,
        MilvusOption.SnapshotCollectionId -> metadata.snapshotInfo.collectionId.toString,
        MilvusOption.SnapshotPartitionIds -> metadata.snapshotInfo.partitionIds
          .mkString(","),
        MilvusOption.ReaderFieldIDs -> pkFieldId.toString,
        MilvusOption.MilvusExtraColumns -> s"$segmentIdCol,$rowOffsetCol",
        "milvus.s3.endpoint" -> s3Endpoint,
        "milvus.s3.bucketName" -> s3Bucket,
        "milvus.s3.accessKey" -> s3AccessKey,
        "milvus.s3.secretKey" -> s3SecretKey,
        "milvus.s3.region" -> s3Region,
        "milvus.s3.rootPath" -> s3RootPath
      )

      val schemaBytes = MilvusSnapshotReader.toProtobufSchemaBytes(
        metadata.collection.schema
      )
      options = options + (MilvusOption.SnapshotSchemaBytes ->
        java.util.Base64.getEncoder.encodeToString(schemaBytes))

      metadata.storageV2ManifestList.foreach { manifestList =>
        if (manifestList.nonEmpty) {
          options = options + (MilvusOption.SnapshotManifests ->
            MilvusSnapshotReader.serializeManifestList(manifestList))
        }
      }

      if (v2Segments.nonEmpty) {
        options = options + (MilvusOption.SnapshotV2Segments ->
          MilvusSnapshotReader.serializeV2Segments(v2Segments))
      }

      import org.apache.spark.sql.types._
      val pkStructField = MilvusSnapshotReader.fieldToStructField(pkField)
      val readSchema = StructType(
        Seq(
          pkStructField,
          StructField(segmentIdCol, LongType, false),
          StructField(rowOffsetCol, LongType, false)
        )
      )

      val df = spark.read
        .schema(readSchema)
        .format("com.zilliz.spark.connector.sources.MilvusDataSource")
        .options(options)
        .load()

      println("\n=== Raw source DataFrame ===")
      val total = df.count()
      val distinctPk = df.select(col(pkName)).distinct().count()
      println(s"total rows:   $total")
      println(s"distinct $pkName: $distinctPk")

      if (total != distinctPk) {
        println(
          s"\n⚠️  DUPLICATION DETECTED: total($total) != distinct($distinctPk)"
        )
        println("=> V2 packed reader is producing repeated data.")

        println("\nTop duplicated PKs (appear count > 1):")
        df.groupBy(col(pkName))
          .count()
          .filter(col("count") > 1)
          .orderBy(col("count").desc)
          .show(20, truncate = false)
      }

      val probeOffsets = Seq(
        0L, 1L, 8191L, 8192L, 8193L, 10000L, 16383L, 16384L, 16385L, 20479L
      )
      println(
        s"\n=== Boundary samples ($rowOffsetCol, $pkName, $segmentIdCol) ==="
      )
      df.filter(
        col(rowOffsetCol).isin(probeOffsets.map(java.lang.Long.valueOf): _*)
      ).orderBy(segmentIdCol, rowOffsetCol)
        .show(100, truncate = false)

      println("\n=== Per-segment stats ===")
      df.groupBy(col(segmentIdCol))
        .agg(
          count(lit(1)).as("rows"),
          countDistinct(col(pkName)).as("distinct_pk"),
          min(col(rowOffsetCol)).as("min_row_offset"),
          max(col(rowOffsetCol)).as("max_row_offset")
        )
        .orderBy(col(segmentIdCol))
        .show(100, truncate = false)

      println("\n=== Done ===")
      println(s"snapshot=$snapshotPath")
      println(s"pk=$pkName")
      println(s"segments=${v2Segments.map(_.segmentId).mkString(",")}")
    } finally {
      spark.stop()
    }
  }

  private def required(
      parsed: Map[String, String],
      key: String
  ): String =
    parsed.getOrElse(
      key,
      throw new IllegalArgumentException(s"--$key is required")
    )

  private def parseArgs(args: Array[String]): Map[String, String] = {
    val m = scala.collection.mutable.Map.empty[String, String]
    var i = 0
    while (i < args.length) {
      val a = args(i)
      if (a.startsWith("--")) {
        val key = a.stripPrefix("--")
        if (i + 1 < args.length && !args(i + 1).startsWith("--")) {
          m(key) = args(i + 1)
          i += 2
        } else {
          m(key) = "true"
          i += 1
        }
      } else {
        i += 1
      }
    }
    m.toMap
  }

  private def configureBucket(
      hadoopConf: Configuration,
      bucket: String,
      endpoint: String,
      accessKey: String,
      secretKey: String,
      region: String
  ): Unit = {
    val prefix = s"fs.s3a.bucket.$bucket"
    if (endpoint.nonEmpty) hadoopConf.set(s"$prefix.endpoint", endpoint)
    if (region.nonEmpty) {
      hadoopConf.set(s"$prefix.endpoint.region", region)
      hadoopConf.set(s"$prefix.region", region)
    }
    hadoopConf.set(s"$prefix.path.style.access", "true")
    hadoopConf.set(s"$prefix.connection.ssl.enabled", "false")
    if (accessKey.nonEmpty) {
      hadoopConf.set(s"$prefix.access.key", accessKey)
      hadoopConf.set(s"$prefix.secret.key", secretKey)
      hadoopConf.set(
        s"$prefix.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
    }
  }

  private def normalizeS3(path: String): String =
    if (path.startsWith("s3://")) "s3a://" + path.stripPrefix("s3://") else path

  private def readAllBytes(
      conf: Configuration,
      path: String
  ): Array[Byte] = {
    import java.io.ByteArrayOutputStream
    import java.net.URI

    import org.apache.hadoop.fs.{FileSystem, Path => HPath}

    val uri = new URI(path)
    val fs = FileSystem.get(uri, conf)
    val in = fs.open(new HPath(uri))
    try {
      val out = new ByteArrayOutputStream()
      val buf = new Array[Byte](8192)
      var n = in.read(buf)
      while (n >= 0) {
        out.write(buf, 0, n)
        n = in.read(buf)
      }
      out.toByteArray
    } finally {
      in.close()
    }
  }
}
