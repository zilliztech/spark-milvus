package com.zilliz.milvus.storage.write.exec

import com.zilliz.milvus.jni.storage.StorageNative

/** The column groups a V3 writer produced: for each group, its parquet files
  * and their row counts. Wraps the C-allocated handle until closed, because a
  * [[ManifestTransaction]] hands the same handle back to the C layer.
  */
final class WrittenColumnGroups private[exec] (handle: Long)
    extends AutoCloseable {
  private var released = false

  private[exec] def nativeHandle: Long = {
    if (released) {
      throw new IllegalStateException("column groups already released")
    }
    handle
  }

  def size: Int = StorageNative.nativeColumnGroupsCount(nativeHandle)

  /** Parquet files of group `index`, in row order. */
  def files(index: Int): Seq[String] =
    StorageNative.nativeColumnGroupFiles(nativeHandle, index).toSeq

  /** Row count of each file of group `index`, positionally. */
  def rowCounts(index: Int): Seq[Long] =
    StorageNative.nativeColumnGroupRowCounts(nativeHandle, index).toSeq

  /** Column names of group `index`, as the writer's Arrow schema named them. */
  def columns(index: Int): Seq[String] =
    StorageNative.nativeColumnGroupColumns(nativeHandle, index).toSeq

  /** Rows of the segment: every group carries the same total. */
  def rows: Long =
    if (size == 0) 0L else rowCounts(0).sum

  override def close(): Unit = synchronized {
    if (!released) {
      released = true
      StorageNative.nativeColumnGroupsDestroy(handle)
    }
  }
}
