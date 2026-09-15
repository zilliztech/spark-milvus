package com.zilliz.milvus.storage.manifest

import java.io.ByteArrayInputStream
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.util.Utf8

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.DeltaLogFile

object V3ManifestReader {
  private val PrimaryKeyDeltaLogType = 0
  private val ManifestFileName = """manifest-(\d+)\.avro""".r

  def loadDeltaLogs(
      basePath: String,
      readVersion: Long,
      bucket: String,
      store: ObjectStore
  ): Either[Throwable, Seq[DeltaLogFile]] = {
    try {
      val at =
        StoragePath.parse(manifestFilePath(basePath, readVersion), bucket)
      parseDeltaLogs(store.readAll(at), basePath)
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  def manifestFilePath(
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

  def latestManifestVersion(
      basePath: String,
      bucket: String,
      store: ObjectStore
  ): Either[Throwable, Long] = {
    try {
      val metadata =
        StoragePath.parse(s"${basePath.stripSuffix("/")}/_metadata", bucket)
      if (!store.exists(metadata.key)) {
        Right(0L)
      } else {
        Right(
          store
            .list(metadata.key)
            .iterator
            .map(info => info.path.substring(info.path.lastIndexOf('/') + 1))
            .flatMap {
              case ManifestFileName(version) => Some(version.toLong)
              case _                         => None
            }
            .foldLeft(0L)(math.max)
        )
      }
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  def parseDeltaLogs(
      avroBytes: Array[Byte],
      basePath: String
  ): Either[Throwable, Seq[DeltaLogFile]] = {
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
  ): Seq[DeltaLogFile] = {
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
          DeltaLogFile(
            logId = idx.toLong,
            logPath =
              resolveManifestDeltaPath(basePath, asString(log.get("path"))),
            entriesNum = asLong(log.get("num_entries"))
          )
        }
    }
  }

  /** Places a delta log recorded in the manifest.
    *
    * `_delta/` is the only format knowledge here; joining and scheme handling
    * belong to [[StoragePath]].
    */
  def resolveManifestDeltaPath(
      basePath: String,
      path: String
  ): String = {
    if (path == null || path.isEmpty) return path
    val base = StoragePath.parse(basePath)
    val fragment =
      if (path.contains("://") || path.stripPrefix("/").startsWith("_delta/")) {
        path
      } else {
        s"_delta/${path.stripPrefix("/")}"
      }
    StoragePath.resolve(base, fragment).uri("s3a")
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
