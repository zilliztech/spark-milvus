package com.zilliz.spark.connector.read

import java.io.ByteArrayInputStream
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.hadoop.conf.Configuration

object MilvusStorageV3ManifestReader {
  private val PrimaryKeyDeltaLogType = 0

  def loadDeltaLogs(
      basePath: String,
      readVersion: Long,
      bucket: String,
      hadoopConf: Configuration
  ): Either[Throwable, Seq[V2DeltaLogFile]] = {
    try {
      val manifestPath = V2SegmentLoader.resolvePath(
        manifestFilePath(basePath, readVersion),
        bucket
      )
      val bytes = V2SegmentLoader.readAllBytes(hadoopConf, manifestPath)
      parseDeltaLogs(bytes, basePath)
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  private[read] def manifestFilePath(
      basePath: String,
      readVersion: Long
  ): String = {
    if (readVersion <= 0) {
      throw new IllegalArgumentException(
        s"StorageV3 deltalog planning requires a positive manifest version, got $readVersion for $basePath"
      )
    }
    s"${basePath.stripSuffix("/")}/_metadata/manifest-$readVersion.avro"
  }

  private[read] def parseDeltaLogs(
      avroBytes: Array[Byte],
      basePath: String
  ): Either[Throwable, Seq[V2DeltaLogFile]] = {
    try {
      val reader = new DataFileStream[GenericRecord](
        new ByteArrayInputStream(avroBytes),
        new GenericDatumReader[GenericRecord]()
      )
      try {
        if (!reader.hasNext) {
          Right(Seq.empty)
        } else {
          val rec = reader.next()
          Right(projectDeltaLogs(rec, basePath))
        }
      } finally {
        reader.close()
      }
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  private def projectDeltaLogs(
      rec: GenericRecord,
      basePath: String
  ): Seq[V2DeltaLogFile] = {
    val raw = rec.get("delta_logs")
    if (raw == null) {
      Seq.empty
    } else {
      raw
        .asInstanceOf[java.util.List[GenericRecord]]
        .asScala
        .toSeq
        .filter(log => asInt(log.get("type")) == PrimaryKeyDeltaLogType)
        .filter(log => asLong(log.get("num_entries")) > 0L)
        .zipWithIndex
        .map { case (log, idx) =>
          V2DeltaLogFile(
            logId = idx.toLong,
            logPath =
              resolveManifestDeltaPath(basePath, asString(log.get("path"))),
            entriesNum = asLong(log.get("num_entries"))
          )
        }
    }
  }

  private[read] def resolveManifestDeltaPath(
      basePath: String,
      path: String
  ): String = {
    if (path == null || path.isEmpty) path
    else if (path.startsWith("s3a://")) path
    else if (path.startsWith("s3://")) "s3a://" + path.stripPrefix("s3://")
    else if (path.startsWith("_delta/"))
      s"${basePath.stripSuffix("/")}/$path"
    else if (path.startsWith("/"))
      s"${basePath.stripSuffix("/")}/_delta/${path.stripPrefix("/")}"
    else s"${basePath.stripSuffix("/")}/_delta/$path"
  }

  private def asString(v: Any): String = v match {
    case u: Utf8   => u.toString
    case s: String => s
    case null      => null
    case other =>
      throw new IllegalStateException(
        s"expected string, got ${other.getClass.getName}: $other"
      )
  }

  private def asInt(v: Any): Int = v match {
    case i: java.lang.Integer => i.intValue()
    case l: java.lang.Long    => l.intValue()
    case other =>
      throw new IllegalStateException(
        s"expected int, got ${other.getClass.getName}: $other"
      )
  }

  private def asLong(v: Any): Long = v match {
    case l: java.lang.Long    => l.longValue()
    case i: java.lang.Integer => i.longValue()
    case other =>
      throw new IllegalStateException(
        s"expected long, got ${other.getClass.getName}: $other"
      )
  }
}
