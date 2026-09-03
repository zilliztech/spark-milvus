package com.zilliz.spark.connector.read

import java.net.URI
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.util.HadoopStreams
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.Type
import org.apache.spark.internal.Logging

/** Parquet kv-metadata produced by the milvus-storage packed writer
  * (StorageV2).
  *
  * Example for a segment with schema `{100:ID, 101:varchar, 102:embeddings,
  * 0:RowID, 1:Timestamp}`:
  * {{{
  *   ParquetFooterMetadata(
  *     storageVersion = Some("1.0.0"),
  *     groupFieldIdList = Seq(Seq(100L, 0L, 1L), Seq(101L), Seq(102L))
  *   )
  * }}}
  */
case class ParquetFooterMetadata(
    storageVersion: Option[String],
    groupFieldIdList: Seq[Seq[Long]]
)

/** Reads just the footer kv-metadata of a parquet file to recover the StorageV2
  * column-group layout.
  *
  * The AVRO manifest only tells us each column-group's file path(s); it does
  * NOT tell us which real field IDs a file carries (its `field_id` entry is the
  * column-group slot / directory name). The segment-level truth lives in the
  * parquet footer as two kv-metadata entries written by the milvus-storage
  * packed writer:
  *
  *   - `storage_version` — a string like `"1.0.0"` (parquet-level, distinct
  *     from the int64 `storage_version` in the AVRO manifest).
  *   - `group_field_id_list` — `"100,0,1;101;102"`, each `;`-separated chunk
  *     being one column group's field IDs in positional order matching the
  *     AVRO's `binlog_files` array.
  *
  * Reading just the footer is cheap — parquet-mr issues a `HEAD` + a single
  * range `GET` for the last few KB of the file.
  */
object MilvusParquetFooterReader extends Logging {

  /** Parquet kv-metadata key written by milvus-storage to identify the on-disk
    * storage format version. For StorageV2 segments the value is `"1.0.0"`.
    */
  val StorageVersionKey = "storage_version"

  /** Parquet kv-metadata key whose value encodes the segment's column-group
    * layout: `"{fid},{fid},...;{fid},{fid};..."`.
    */
  val GroupFieldIdListKey = "group_field_id_list"

  /** Open the parquet file at `path` and return its kv-metadata projection.
    *
    * The caller is responsible for handing in a `Configuration` that already
    * has any per-bucket S3 credentials / endpoint set (see
    * `MilvusBackfill.configureHadoopS3ForPath`). For local files a plain `new
    * Configuration()` works.
    *
    * @return
    *   `Right(meta)` on success. `Left(throwable)` on I/O or parse failure.
    */
  def read(
      path: String,
      hadoopConf: Configuration
  ): Either[Throwable, ParquetFooterMetadata] = {
    readWithFileSystem(path, hadoopConf) { inputFile =>
      val parquet = ParquetFileReader.open(inputFile)
      try {
        val kv = parquet.getFooter.getFileMetaData.getKeyValueMetaData
        ParquetFooterMetadata(
          storageVersion = Option(kv.get(StorageVersionKey)),
          groupFieldIdList = parseGroupFieldIdList(kv.get(GroupFieldIdListKey))
        )
      } finally {
        parquet.close()
      }
    }
  }

  /** Read the field IDs that this specific parquet file carries on its
    * top-level columns.
    *
    * In StorageV2 each parquet file holds exactly one column group, so the
    * file's own schema IS that group's field-id list. We prefer this over the
    * footer's `group_field_id_list` kv-metadata because that kv is per
    * write-session — after a backfill that appends new column groups to an
    * existing segment, the original parquets still advertise only the original
    * session's groups while the newly-written parquets advertise only the
    * backfill session's groups. Reading each file's own schema avoids that trap
    * entirely.
    *
    * milvus-storage writes the field id on each Arrow field as
    * `PARQUET:field_id` metadata; arrow-cpp's parquet writer translates that
    * into Parquet's native `SchemaElement.field_id`, which Parquet Java exposes
    * via `Type.getId`.
    *
    * @return
    *   `Right(fieldIds)` with one entry per top-level column, in schema order.
    *   `Left(throwable)` on any I/O or parse failure, or if a column is missing
    *   a field id (indicates a malformed parquet).
    */
  def readFieldIdsFromSchema(
      path: String,
      hadoopConf: Configuration
  ): Either[Throwable, Seq[Long]] = {
    readWithFileSystem(path, hadoopConf) { inputFile =>
      val parquet = ParquetFileReader.open(inputFile)
      try {
        fieldIdsFromMessageType(
          parquet.getFooter.getFileMetaData.getSchema,
          path
        )
      } finally {
        parquet.close()
      }
    }
  }

  /** Shared field-ID extraction from a parquet `MessageType`, keeping the
    * single-open callers (`readFieldIdsFromSchema`, `readFieldIdsAndRowCount`)
    * in lock-step.
    */
  private[read] def fieldIdsFromMessageType(
      schema: org.apache.parquet.schema.MessageType,
      path: String
  ): Seq[Long] = {
    schema.getFields.asScala.map { t: Type =>
      val id = t.getId
      if (id == null) {
        throw new IllegalStateException(
          s"parquet column '${t.getName}' in $path has no PARQUET:field_id " +
            s"metadata; cannot recover StorageV2 column-group field ids"
        )
      }
      id.intValue().toLong
    }.toSeq
  }

  private def readWithFileSystem[T](
      path: String,
      hadoopConf: Configuration
  )(read: InputFile => T): Either[Throwable, T] = {
    var uri: URI = null
    var fs: FileSystem = null
    try {
      uri = new URI(path)
      val hadoopPath = new Path(uri)
      fs = hadoopPath.getFileSystem(hadoopConf)
      readWithFileSystem(fs, path)(read)
    } catch {
      case NonFatal(e) => Left(e)
    } finally {
      Option(uri).flatMap(uri => Option(uri.getScheme)).foreach { scheme =>
        if (
          fs != null && hadoopConf
            .getBoolean(s"fs.$scheme.impl.disable.cache", false)
        ) {
          fs.close()
        }
      }
    }
  }

  /** Read with a caller-supplied `FileSystem` (reused across many footer reads,
    * e.g. all binlogs of a backup) instead of resolving a fresh one per file —
    * with `fs.s3a.impl.disable.cache=true` every `FileSystem.get` otherwise
    * constructs a whole S3A client + thread pool.
    */
  private[read] def readWithFileSystem[T](
      fs: FileSystem,
      path: String
  )(read: InputFile => T): Either[Throwable, T] = {
    try {
      val hadoopPath = new Path(path)
      val fileStatus = fs.getFileStatus(hadoopPath)
      val inputFile = new InputFile {
        override def getLength: Long = fileStatus.getLen

        override def newStream(): org.apache.parquet.io.SeekableInputStream =
          HadoopStreams.wrap(fs.open(hadoopPath))
      }
      Right(read(inputFile))
    } catch {
      case NonFatal(e) => Left(e)
    }
  }

  /** The projection of a parquet footer the backup datasource needs to build a
    * `V2ColumnGroup`: the file's own top-level field IDs plus its total row
    * count, recovered from a single footer read.
    */
  private[read] case class ParquetFooterInfo(
      fieldIds: Seq[Long],
      rowCount: Long
  )

  /** Read the total number of rows a parquet file contains by summing its row
    * groups' row counts from the footer.
    *
    * Used by the backup datasource to recover per-file row counts that
    * milvus-backup's meta does not persist (only `log_size` is recorded), which
    * the packed V2 reader requires to build valid per-file `[0, rc)` ranges.
    * Like the other footer reads this issues a `HEAD` + a single range `GET`
    * for the last few KB of the file.
    *
    * @return
    *   `Right(rowCount)` on success. `Left(throwable)` on I/O or parse failure.
    */
  def readRowCount(
      path: String,
      hadoopConf: Configuration
  ): Either[Throwable, Long] = {
    readWithFileSystem(path, hadoopConf) { inputFile =>
      sumRowGroups(inputFile)
    }
  }

  /** [[readRowCount]] with a caller-supplied `FileSystem` (reused across many
    * files, e.g. all binlogs of a backup read).
    */
  def readRowCount(
      fs: FileSystem,
      path: String
  ): Either[Throwable, Long] = {
    readWithFileSystem(fs, path) { inputFile =>
      sumRowGroups(inputFile)
    }
  }

  private def sumRowGroups(inputFile: InputFile): Long = {
    val parquet = ParquetFileReader.open(inputFile)
    try {
      parquet.getFooter.getBlocks.asScala.map(_.getRowCount).sum
    } finally {
      parquet.close()
    }
  }

  /** Read a parquet file's own field IDs and total row count with a single
    * footer open — the production path the backup datasource uses to recover
    * both the column-group field IDs and the per-file row count.
    */
  def readFieldIdsAndRowCount(
      path: String,
      hadoopConf: Configuration
  ): Either[Throwable, ParquetFooterInfo] = {
    readWithFileSystem(path, hadoopConf) { inputFile =>
      footerInfo(inputFile, path)
    }
  }

  /** [[readFieldIdsAndRowCount]] with a caller-supplied `FileSystem`. */
  def readFieldIdsAndRowCount(
      fs: FileSystem,
      path: String
  ): Either[Throwable, ParquetFooterInfo] = {
    readWithFileSystem(fs, path) { inputFile =>
      footerInfo(inputFile, path)
    }
  }

  private def footerInfo(
      inputFile: InputFile,
      path: String
  ): ParquetFooterInfo = {
    val parquet = ParquetFileReader.open(inputFile)
    try {
      val footer = parquet.getFooter
      val fieldIds = fieldIdsFromMessageType(
        footer.getFileMetaData.getSchema,
        path
      )
      val rowCount = footer.getBlocks.asScala.map(_.getRowCount).sum
      ParquetFooterInfo(fieldIds, rowCount)
    } finally {
      parquet.close()
    }
  }

  /** Parse the kv-metadata string `"100,0,1;101;102"` into `Seq(Seq(100, 0, 1),
    * Seq(101), Seq(102))`.
    *
    * A missing or empty value returns `Seq.empty`. Individual groups must be
    * non-empty (would indicate a malformed footer).
    */
  def parseGroupFieldIdList(raw: String): Seq[Seq[Long]] = {
    if (raw == null || raw.isEmpty) Seq.empty
    else {
      raw
        .split(";", -1)
        // drop a trailing empty chunk if the string ends in ';'
        .filter(_.nonEmpty)
        .map { group =>
          val fids =
            group.split(",", -1).filter(_.nonEmpty).map(_.trim.toLong).toSeq
          require(
            fids.nonEmpty,
            s"empty column group in group_field_id_list=[$raw]"
          )
          fids
        }
        .toSeq
    }
  }
}
