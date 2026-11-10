package com.zilliz.milvus.storage.write

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import io.milvus.storage.MilvusStorageColumnGroups

/** The column groups a V3 writer produced: for each group, its parquet files
  * and their row counts. Wraps the C-allocated handle until closed, because a
  * [[ManifestTransaction]] hands the same handle back to the C layer.
  */
final class WrittenColumnGroups private[write] (handle: Long)
    extends AutoCloseable {
  private var released = false

  private[write] def nativeHandle: Long = {
    if (released) {
      throw new IllegalStateException("column groups already released")
    }
    handle
  }

  def size: Int = MilvusStorageColumnGroups.count(nativeHandle)

  /** Parquet files of group `index`, in row order. */
  def files(index: Int): Seq[String] =
    MilvusStorageColumnGroups.files(nativeHandle, index).toSeq

  /** Row count of each file of group `index`, positionally. */
  def rowCounts(index: Int): Seq[Long] =
    MilvusStorageColumnGroups.fileRowCounts(nativeHandle, index).toSeq

  /** Column names of group `index`, as the writer's Arrow schema named them. */
  def columns(index: Int): Seq[String] =
    MilvusStorageColumnGroups.columns(nativeHandle, index).toSeq

  /** Physical format reported by the native writer for this group. */
  def format(index: Int): String =
    MilvusStorageColumnGroups.format(nativeHandle, index)

  /** Rows of the segment: every group carries the same total. */
  def rows: Long =
    if (size == 0) 0L else rowCounts(0).sum

  /** Every group as plain values, in group order, for a process other than the
    * writer's to commit through [[WrittenColumnGroups.of]].
    */
  def describe: Seq[ColumnGroupFiles] =
    (0 until size).map(index =>
      ColumnGroupFiles(
        columns(index),
        files(index),
        rowCounts(index),
        format(index)
      )
    )

  override def close(): Unit = synchronized {
    if (!released) {
      released = true
      MilvusStorageColumnGroups.destroy(handle)
    }
  }
}

object WrittenColumnGroups {

  /** Native column groups rebuilt from the descriptions a writer produced, so
    * that the driver can commit files that tasks wrote. Each file is recorded
    * with its whole row range, which is what a writer's own groups describe.
    * The native builder only produces parquet groups, so any other format is
    * refused rather than recorded under the wrong format.
    */
  def of(groups: Seq[ColumnGroupFiles]): WrittenColumnGroups = {
    require(
      groups.nonEmpty,
      "rebuilding column groups needs at least one group"
    )
    val formats = groups.map(_.format).distinct
    require(
      formats == Seq(ColumnGroupFiles.Parquet),
      s"only parquet column groups can be rebuilt, got ${formats.mkString(", ")}"
    )
    NativeStorageLibrary.load()
    new WrittenColumnGroups(
      MilvusStorageColumnGroups.createFromGroups(
        groups.map(_.columns.toArray).toArray,
        groups.map(_.files.toArray).toArray,
        groups.map(_.rowCounts.toArray).toArray
      )
    )
  }
}

/** One written column group as plain values: the manifest column names (field
  * ids), its files in row order and each file's row count.
  */
final case class ColumnGroupFiles(
    columns: Seq[String],
    files: Seq[String],
    rowCounts: Seq[Long],
    format: String
) {
  require(columns.nonEmpty, "a column group names at least one column")
  require(
    files.nonEmpty && files.size == rowCounts.size,
    s"a column group needs one row count per file, got ${files.size} file(s) and ${rowCounts.size} count(s)"
  )
  require(rowCounts.forall(_ > 0L), "every file of a column group holds rows")

  def rows: Long = rowCounts.sum
}

object ColumnGroupFiles {
  val Parquet = "parquet"
}
