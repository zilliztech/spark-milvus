package com.zilliz.milvus.storage.write

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import io.milvus.storage.{
  MilvusStorageColumnGroups,
  MilvusStorageManifest,
  MilvusStorageProperties,
  MilvusStorageTransaction
}

/** Records written column groups in a V3 segment's manifest: one transaction on
  * the latest revision by default, or an explicitly pinned revision for legacy
  * column replacement. Concurrent advancement fails instead of replacing a
  * newer base.
  */
object ManifestTransaction {

  sealed trait Change

  /** New files of existing columns: an append to the segment. */
  case object AppendFiles extends Change

  /** Column replacement: the groups replace `columns` (manifest column names,
    * which are Milvus field ids). Each column is dropped and the new groups
    * added in the same transaction, so the swap is atomic per column.
    */
  final case class ReplaceColumns(columns: Seq[String]) extends Change

  /** A statistics entry of the manifest: `key` (`bloom_filter.<field id>`), the
    * files under the segment's `_stats/` that hold it, and metadata such as
    * `memory_size`. Committing it replaces any entry of the same key.
    */
  final case class Stat(
      key: String,
      files: Seq[String],
      metadata: Map[String, String]
  )

  /** @return the committed manifest version. */
  def commit(
      basePath: String,
      properties: Map[String, String],
      groups: WrittenColumnGroups,
      change: Change,
      stats: Seq[Stat] = Seq.empty,
      readVersion: Long = -1L
  ): Long =
    withPrepared(basePath, properties, groups, change, stats, readVersion)(
      submit => submit()
    )

  /** Submits a fully staged native transaction and returns the acknowledged
    * manifest revision. Valid only inside the callback to [[withPrepared]].
    */
  private[zilliz] type PreparedCommit = () => Long

  /** Stages all changes before handing the caller the commit operation. The
    * FAIL resolver rejects concurrent advancement of the selected revision. All
    * resources stay open until the callback returns, so an acknowledgement can
    * be recorded before cleanup.
    */
  private[zilliz] def withPrepared(
      basePath: String,
      properties: Map[String, String],
      groups: WrittenColumnGroups,
      change: Change,
      stats: Seq[Stat] = Seq.empty,
      readVersion: Long = -1L
  )(submit: PreparedCommit => Long): Long = {
    require(
      readVersion == -1L || readVersion > 0L,
      "readVersion must be latest (-1) or a committed manifest version"
    )
    NativeStorageLibrary.load()
    val nativeProperties = new MilvusStorageProperties()
    WriterResources.closing(nativeProperties.free()) {
      nativeProperties.create(properties)
      val transaction = new MilvusStorageTransaction()
      WriterResources.closing(transaction.destroy()) {
        transaction.begin(basePath, nativeProperties.getPtr, readVersion, 0, 1)
        change match {
          case AppendFiles =>
            transaction.appendFiles(groups.nativeHandle)
          case ReplaceColumns(columns) =>
            columns.foreach(transaction.dropColumn)
            transaction.addColumnGroups(groups.nativeHandle)
        }
        stats.foreach { stat =>
          transaction.updateStat(
            stat.key,
            stat.files.toArray,
            stat.metadata
          )
        }
        submit { () =>
          val version = transaction.commit()
          if (version < 0) {
            throw new IllegalStateException(
              s"committing the manifest at $basePath failed (version $version)"
            )
          }
          version
        }
      }
    }
  }

  /** One committed revision of a segment manifest and the data files its column
    * groups reference.
    */
  final case class Revision(version: Long, files: Seq[String])

  /** Revision `version` of the manifest at `basePath`, or the latest one for
    * -1. The data file list can rule out a commit. It does not identify the
    * transaction that produced the revision.
    */
  def revision(
      basePath: String,
      properties: Map[String, String],
      version: Long = -1L
  ): Revision = {
    require(
      version == -1L || version > 0L,
      "a revision is the latest (-1) or a committed manifest version"
    )
    NativeStorageLibrary.load()
    val nativeProperties = new MilvusStorageProperties()
    try {
      nativeProperties.create(properties)
      val manifest =
        MilvusStorageManifest.open(basePath, nativeProperties, version)
      try {
        val groups = manifest.columnGroupsPtr
        Revision(
          manifest.readVersion,
          (0 until MilvusStorageColumnGroups.count(groups)).flatMap(index =>
            MilvusStorageColumnGroups.files(groups, index).toSeq
          )
        )
      } finally manifest.close()
    } finally nativeProperties.free()
  }
}
