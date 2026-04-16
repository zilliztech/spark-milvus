package com.zilliz.spark.connector.tools

import java.io.ByteArrayOutputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import com.zilliz.spark.connector.read.{
  MilvusParquetFooterReader,
  MilvusSegmentManifestReader,
  MilvusSnapshotReader,
  SnapshotMetadata,
  V2SegmentInfo
}

/** Standalone Spark app that reads a Milvus snapshot (local path or `s3a://`)
  * and prints the per-segment column-group layout recovered via the new
  * `MilvusSegmentManifestReader` + `MilvusParquetFooterReader`.
  *
  * Useful as a smoke test for phase-1 changes before wiring the v2 read path
  * into the DataSource.
  *
  * Usage (local):
  * {{{
  *   ./examples/list_v2_segments.sh s3a://a-bucket/files/snapshots/.../metadata/<id>.json
  * }}}
  *
  * CLI args:
  *   - `--snapshot <path>` metadata JSON path (local or s3a://)
  *   - `--s3-endpoint <host:port>`
  *   - `--s3-bucket <name>` bucket that holds the snapshot files
  *   - `--s3-access-key <key>` (omit for IAM/IRSA)
  *   - `--s3-secret-key <secret>` (omit for IAM/IRSA)
  *   - `--s3-region <region>` (default `us-east-1`)
  *   - `--s3-use-ssl` enable https
  *   - `--use-iam` enable IAM/IRSA credentials chain
  */
object ListV2SegmentsApp {

  def main(args: Array[String]): Unit = {
    val parsed = parseArgs(args)
    val snapshotPath = parsed.getOrElse(
      "snapshot",
      throw new IllegalArgumentException("--snapshot is required")
    )
    val s3Endpoint = parsed.getOrElse("s3-endpoint", "")
    val s3Bucket = parsed.getOrElse("s3-bucket", "")
    val s3AccessKey = parsed.getOrElse("s3-access-key", "")
    val s3SecretKey = parsed.getOrElse("s3-secret-key", "")
    val s3Region = parsed.getOrElse("s3-region", "us-east-1")
    val s3UseSSL = parsed.contains("s3-use-ssl")
    val useIam =
      parsed.contains("use-iam") || (s3AccessKey.isEmpty && s3SecretKey.isEmpty)

    val spark = SparkSession.builder
      .appName("ListV2Segments")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      // Register S3A credentials for the snapshot bucket (and, by default,
      // the same bucket holds all binlog files).
      if (s3Bucket.nonEmpty) {
        configureBucket(
          hadoopConf,
          s3Bucket,
          s3Endpoint,
          s3AccessKey,
          s3SecretKey,
          s3Region,
          s3UseSSL,
          useIam
        )
      }

      println("==============================================")
      println(s"  Milvus snapshot: $snapshotPath")
      println("==============================================")

      val resolvedSnapshotPath = resolvePath(snapshotPath, s3Bucket)
      val metadata = loadMetadata(hadoopConf, resolvedSnapshotPath)
      printHeader(metadata)

      // V3 segments come straight from the outer JSON — no AVRO needed.
      metadata.storageV2ManifestList match {
        case Some(items) if items.nonEmpty =>
          println(s"\n--- StorageV3 segments (${items.size}) ---")
          items.foreach { item =>
            println(s"  segmentID=${item.segmentID}  manifest=${item.manifest}")
          }
        case _ =>
          println("\n--- StorageV3 segments: (none) ---")
      }

      // V2 segments live inside the AVRO manifests — decode each.
      if (metadata.manifestList.isEmpty) {
        println("\n--- V2/V1 manifest_list: (none) ---")
      } else {
        println(s"\n--- manifest_list (${metadata.manifestList.size}) ---")
        metadata.manifestList.foreach { manifestPath =>
          processOneSegment(
            hadoopConf,
            resolvePath(manifestPath, s3Bucket),
            s3Bucket
          )
        }
      }
    } finally {
      spark.stop()
    }
  }

  // -------------------------------------------------------------------------
  // Per-segment processing.
  // -------------------------------------------------------------------------

  private def processOneSegment(
      hadoopConf: Configuration,
      manifestPath: String,
      bucket: String
  ): Unit = {
    println(s"\n  AVRO: $manifestPath")
    val avroBytes = readAllBytes(hadoopConf, manifestPath)
    println(s"    size: ${avroBytes.length} bytes")

    MilvusSegmentManifestReader.parse(avroBytes) match {
      case Left(err) =>
        println(s"    ERROR decoding AVRO: ${err.getMessage}")

      case Right(entry) =>
        println(
          s"    segmentId=${entry.segmentId}  partitionId=${entry.partitionId}" +
            s"  numOfRows=${entry.numOfRows}  storageVersion=${entry.storageVersion}"
        )
        entry.binlogFiles.foreach { afb =>
          println(
            s"      binlog slotFieldId=${afb.slotFieldId}  " +
              s"${afb.binlogs.size} file(s): " +
              afb.binlogs.map(_.logPath).mkString(", ")
          )
        }

        if (entry.storageVersion != 2L) {
          println(
            s"    skip toV2SegmentInfo: storage_version ${entry.storageVersion} != 2 (StorageV2)"
          )
        } else if (entry.binlogFiles.isEmpty) {
          println("    (empty segment — no column groups to materialize)")
        } else {
          // Use any one of the segment's parquet files' kv-metadata to learn
          // the real field IDs per column group. All files in the segment
          // carry the same group_field_id_list.
          val samplePath = entry.binlogFiles.head.binlogs.head.logPath
          val normalized = resolvePath(samplePath, bucket)
          MilvusParquetFooterReader.read(normalized, hadoopConf) match {
            case Left(err) =>
              println(
                s"    ERROR reading parquet footer at $normalized: ${err.getMessage}"
              )

            case Right(footer) =>
              println(
                s"    parquet kv: storage_version=${footer.storageVersion
                    .getOrElse("<absent>")}" +
                  s"  group_field_id_list=${footer.groupFieldIdList
                      .map(_.mkString(","))
                      .mkString(";")}"
              )
              MilvusSegmentManifestReader.toV2SegmentInfo(
                entry,
                footer.groupFieldIdList
              ) match {
                case Left(err) =>
                  println(s"    ERROR toV2SegmentInfo: ${err.getMessage}")
                case Right(seg) =>
                  printV2Segment(seg)
              }
          }
        }
    }
  }

  private def printV2Segment(seg: V2SegmentInfo): Unit = {
    println(s"    V2SegmentInfo:")
    println(s"      segmentId  = ${seg.segmentId}")
    println(s"      partitionId= ${seg.partitionId}")
    println(s"      numOfRows  = ${seg.numOfRows}")
    seg.columnGroups.zipWithIndex.foreach { case (cg, i) =>
      println(
        s"      columnGroup[$i]: fields=[${cg.fieldIds.mkString(",")}]  " +
          s"${cg.filePaths.size} file(s)"
      )
      cg.filePaths.foreach(p => println(s"        - $p"))
    }
  }

  private def printHeader(metadata: SnapshotMetadata): Unit = {
    val info = metadata.snapshotInfo
    println(s"  snapshot.id         = ${info.id}")
    println(s"  snapshot.name       = ${info.name}")
    println(s"  snapshot.collection = ${info.collectionId}")
    println(s"  snapshot.partitions = ${info.partitionIds.mkString(", ")}")
    println(s"  format_version      = ${metadata.formatVersion.getOrElse("?")}")
    println(s"  collection.name     = ${metadata.collection.schema.name}")
  }

  // -------------------------------------------------------------------------
  // Hadoop FS helpers.
  // -------------------------------------------------------------------------

  /** Read the full contents of a file into memory. Used for the small JSON
    * metadata file and for per-segment AVRO (< few KB each).
    *
    * Caller is expected to supply a fully-qualified path (`resolvePath` has
    * already been applied) — `FileSystem.get` uses the URI's scheme to pick the
    * right FS, so a scheme-less path would fall back to the local FS.
    */
  private def readAllBytes(conf: Configuration, path: String): Array[Byte] = {
    val uri = new URI(path)
    val fs = FileSystem.get(uri, conf)
    val in = fs.open(new Path(uri))
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

  private def loadMetadata(
      hadoopConf: Configuration,
      snapshotPath: String
  ): SnapshotMetadata = {
    val bytes = readAllBytes(hadoopConf, snapshotPath) // already resolved
    val json = new String(bytes, "UTF-8")
    MilvusSnapshotReader.parseSnapshotMetadata(json) match {
      case Right(m) => m
      case Left(e) =>
        throw new RuntimeException(
          s"failed to parse snapshot metadata at $snapshotPath: ${e.getMessage}",
          e
        )
    }
  }

  /** Normalize a path from the snapshot into something Hadoop FS can open.
    *
    *   - `s3a://...` — pass through.
    *   - `s3://...` — rewrite to `s3a://` (Hadoop 3.4+ dropped the `s3://`
    *     provider).
    *   - anything else is treated as **bucket-relative**, e.g. the
    *     `manifest_list` entries that milvus writes as `files/snapshots/...`,
    *     or `log_path` values inside the AVRO. These are prefixed with
    *     `s3a://{bucket}/` so they resolve to the snapshot bucket.
    *   - `null` is passed through (caller handles).
    *
    * If no bucket is known (`bucket.isEmpty`) and the path has no scheme,
    * Hadoop would default to the local filesystem — which is usually not what
    * we want, but is intentional for unit-test paths like `src/test/data/...`.
    */
  private def resolvePath(path: String, bucket: String): String = {
    if (path == null) path
    else if (path.startsWith("s3a://")) path
    else if (path.startsWith("s3://")) "s3a://" + path.stripPrefix("s3://")
    else if (bucket != null && bucket.nonEmpty)
      s"s3a://$bucket/${path.stripPrefix("/")}"
    else path
  }

  /** Register per-bucket S3A credentials / endpoint on the Hadoop config. */
  private def configureBucket(
      conf: Configuration,
      bucket: String,
      endpoint: String,
      accessKey: String,
      secretKey: String,
      region: String,
      useSSL: Boolean,
      useIam: Boolean
  ): Unit = {
    val prefix = s"fs.s3a.bucket.$bucket"
    if (endpoint != null && endpoint.nonEmpty) {
      conf.set(s"$prefix.endpoint", endpoint)
    }
    if (region != null && region.nonEmpty) {
      conf.set(s"$prefix.endpoint.region", region)
      conf.set(s"$prefix.region", region)
    }
    conf.set(s"$prefix.path.style.access", "true")
    conf.set(s"$prefix.connection.ssl.enabled", if (useSSL) "true" else "false")
    if (useIam) {
      conf.set(
        s"$prefix.aws.credentials.provider",
        Seq(
          "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
          "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
          "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
        ).mkString(",")
      )
    } else {
      conf.set(s"$prefix.access.key", accessKey)
      conf.set(s"$prefix.secret.key", secretKey)
      conf.set(
        s"$prefix.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
    }
  }

  /** Tiny `--key value` parser; boolean flags become `key -> ""`.
    */
  private def parseArgs(args: Array[String]): Map[String, String] = {
    val kv = scala.collection.mutable.Map.empty[String, String]
    var i = 0
    while (i < args.length) {
      val tok = args(i)
      if (tok.startsWith("--")) {
        val key = tok.drop(2)
        if (i + 1 < args.length && !args(i + 1).startsWith("--")) {
          kv(key) = args(i + 1)
          i += 2
        } else {
          kv(key) = ""
          i += 1
        }
      } else {
        i += 1
      }
    }
    kv.toMap
  }
}
