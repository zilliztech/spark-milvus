package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.ParquetFileReader
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
    try {
      val inputFile =
        HadoopInputFile.fromPath(new Path(path), hadoopConf)
      val parquet = ParquetFileReader.open(inputFile)
      try {
        val kv = parquet.getFooter.getFileMetaData.getKeyValueMetaData
        Right(
          ParquetFooterMetadata(
            storageVersion = Option(kv.get(StorageVersionKey)),
            groupFieldIdList =
              parseGroupFieldIdList(kv.get(GroupFieldIdListKey))
          )
        )
      } finally {
        parquet.close()
      }
    } catch {
      case e: Throwable => Left(e)
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
