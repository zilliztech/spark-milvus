package com.zilliz.milvus.storage.write.exec

import scala.collection.JavaConverters._

import com.zilliz.milvus.jni.storage.StorageNative

/** Records written column groups in a V3 segment's manifest: one transaction on
  * the latest manifest version, committed as the next version.
  */
object ManifestTransaction {

  sealed trait Change

  /** New files of existing columns: an append to the segment. */
  case object AppendFiles extends Change

  /** Backfill: the groups replace `columns` (manifest column names, which are
    * Milvus field ids). Each column is dropped and the new groups added in the
    * same transaction, so the swap is atomic per column.
    */
  final case class ReplaceColumns(columns: Seq[String]) extends Change

  /** @return the committed manifest version. */
  def commit(
      basePath: String,
      properties: Map[String, String],
      groups: WrittenColumnGroups,
      change: Change
  ): Long = {
    // -1 reads the latest version; 0 fails on a conflicting commit; one retry.
    val transaction =
      StorageNative.transactionBegin(basePath, properties.asJava, -1L, 0, 1)
    val version =
      try {
        change match {
          case AppendFiles =>
            StorageNative.transactionAppendFiles(
              transaction,
              groups.nativeHandle
            )
          case ReplaceColumns(columns) =>
            columns.foreach(StorageNative.transactionDropColumn(transaction, _))
            StorageNative.transactionAddColumnGroups(
              transaction,
              groups.nativeHandle
            )
        }
        StorageNative.transactionCommit(transaction)
      } finally StorageNative.transactionDestroy(transaction)
    if (version < 0) {
      throw new IllegalStateException(
        s"committing the manifest at $basePath failed (version $version)"
      )
    }
    version
  }
}
