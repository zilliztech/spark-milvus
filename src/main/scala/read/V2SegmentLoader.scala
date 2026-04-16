package com.zilliz.spark.connector.read

import java.io.ByteArrayOutputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging

/** High-level loader for StorageV2 (non-manifest packed parquet) segments.
  *
  * Given the list of per-segment AVRO paths from
  * `SnapshotMetadata.manifestList` and the S3 bucket where those files live,
  * this object:
  *
  *   1. Fetches each AVRO via Hadoop FS. 2. Decodes with
  *      [[MilvusSegmentManifestReader]]. 3. Skips entries whose
  *      `storage_version != 2` (V1/V3 are handled elsewhere). 4. For each V2
  *      entry, reads exactly one parquet footer's `group_field_id_list`
  *      kv-metadata to recover the segment's column-group layout
  *      ([[MilvusParquetFooterReader]]). 5. Calls
  *      `MilvusSegmentManifestReader.toV2SegmentInfo` to join the two.
  *
  * The resulting `Seq[V2SegmentInfo]` is the runtime view consumed by
  * `MilvusPackedV2InputPartition` / `MilvusLoonV2PartitionReader`.
  *
  * Path resolution: AVRO and parquet paths that milvus writes are
  * bucket-relative (`files/snapshots/...`). When `bucket` is non-empty we
  * prefix `s3a://{bucket}/` so Hadoop picks the S3A filesystem. Fully qualified
  * `s3a://...` / `s3://...` URIs pass through.
  */
object V2SegmentLoader extends Logging {

  /** Load all V2 segments referenced by an AVRO manifest list.
    *
    * @param manifestPaths
    *   Bucket-relative (or fully-qualified) paths as they appear in
    *   `SnapshotMetadata.manifestList`.
    * @param bucket
    *   S3 bucket that holds both the AVRO files and the segment parquet files.
    *   Empty string is accepted for unit-test / local-FS usage.
    * @param hadoopConf
    *   Pre-configured Hadoop `Configuration` (per-bucket S3A creds/endpoint
    *   already set by the caller).
    * @return
    *   `Right(segments)` on success; `Left(firstError)` on the first
    *   unrecoverable failure.
    */
  def loadV2Segments(
      manifestPaths: Seq[String],
      bucket: String,
      hadoopConf: Configuration
  ): Either[Throwable, Seq[V2SegmentInfo]] = {
    try {
      val out = scala.collection.mutable.ArrayBuffer.empty[V2SegmentInfo]
      manifestPaths.foreach { rawPath =>
        val avroPath = resolvePath(rawPath, bucket)
        val avroBytes = readAllBytes(hadoopConf, avroPath)
        val entry = MilvusSegmentManifestReader.parse(avroBytes) match {
          case Right(e) => e
          case Left(err) =>
            throw new RuntimeException(
              s"failed to decode segment manifest $avroPath: ${err.getMessage}",
              err
            )
        }
        if (entry.storageVersion != 2L) {
          logInfo(
            s"skipping segment ${entry.segmentId}: storage_version=${entry.storageVersion} " +
              s"(!= 2); V2SegmentLoader only handles StorageV2"
          )
        } else if (
          entry.binlogFiles.isEmpty ||
          entry.binlogFiles.forall(_.binlogs.isEmpty)
        ) {
          logWarning(
            s"segment ${entry.segmentId} has no binlog files with entries; " +
              s"emitting as empty column-group list"
          )
          out += V2SegmentInfo(
            segmentId = entry.segmentId,
            partitionId = entry.partitionId,
            numOfRows = entry.numOfRows,
            storageVersion = entry.storageVersion,
            columnGroups = Seq.empty
          )
        } else {
          // Any one of this segment's parquet files carries the full
          // `group_field_id_list` in its kv-metadata. Skip any binlog_files
          // entry that happens to be empty.
          val sample = entry.binlogFiles
            .find(_.binlogs.nonEmpty)
            .get
            .binlogs
            .head
            .logPath
          val footerPath = resolvePath(sample, bucket)
          val footer =
            MilvusParquetFooterReader.read(footerPath, hadoopConf) match {
              case Right(f) => f
              case Left(err) =>
                throw new RuntimeException(
                  s"failed to read parquet footer $footerPath: ${err.getMessage}",
                  err
                )
            }
          MilvusSegmentManifestReader.toV2SegmentInfo(
            entry,
            footer.groupFieldIdList
          ) match {
            case Right(seg) => out += seg
            case Left(err) =>
              throw new RuntimeException(
                s"failed to build V2SegmentInfo for segment ${entry.segmentId}: " +
                  err.getMessage,
                err
              )
          }
        }
      }
      Right(out.toSeq)
    } catch {
      case e: Throwable => Left(e)
    }
  }

  /** Prefix `bucket` when `path` has no scheme; pass through s3a:// / s3://. */
  def resolvePath(path: String, bucket: String): String = {
    if (path == null) path
    else if (path.startsWith("s3a://")) path
    else if (path.startsWith("s3://")) "s3a://" + path.stripPrefix("s3://")
    else if (bucket != null && bucket.nonEmpty)
      s"s3a://$bucket/${path.stripPrefix("/")}"
    else path
  }

  private def readAllBytes(
      conf: Configuration,
      fullyQualifiedPath: String
  ): Array[Byte] = {
    val uri = new URI(fullyQualifiedPath)
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
}
