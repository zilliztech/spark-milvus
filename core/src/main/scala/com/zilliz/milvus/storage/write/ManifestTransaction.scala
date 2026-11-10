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
  * newer base. [[ManifestTransaction.createFirst]] uses the same conditional
  * revision commit to claim an otherwise empty path exactly once.
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

  /** Commits revision 1 of the manifest at `basePath`, holding only `stats`,
    * and fails once any revision exists there. Revision files are created with
    * the store's conditional write, so of concurrent callers exactly one
    * succeeds; on a store without conditional writes the native layer falls
    * back to an existence check, as for every other manifest commit.
    *
    * A failure does not say whether another caller won: inspect the path's
    * latest revision to tell a lost race from an unreachable store.
    */
  def createFirst(
      basePath: String,
      properties: Map[String, String],
      stats: Seq[Stat]
  ): Long = {
    require(stats.nonEmpty, "the first revision needs at least one statistic")
    run(basePath, properties, readVersion = 0L) { transaction =>
      stats.foreach { stat =>
        transaction.updateStat(stat.key, stat.files.toArray, stat.metadata)
      }
    }
  }

  /** @return the committed manifest version. */
  def commit(
      basePath: String,
      properties: Map[String, String],
      groups: WrittenColumnGroups,
      change: Change,
      stats: Seq[Stat] = Seq.empty,
      readVersion: Long = -1L
  ): Long = {
    require(
      readVersion == -1L || readVersion > 0L,
      "readVersion must be latest (-1) or a committed manifest version"
    )
    run(basePath, properties, readVersion) { transaction =>
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
    }
  }

  /** One committed revision of a segment manifest and the data files its column
    * groups reference.
    */
  final case class Revision(version: Long, files: Seq[String])

  /** Revision `version` of the manifest at `basePath`, or the latest one for
    * -1. A caller that lost the outcome of a commit reads the revision the
    * commit would have produced and looks for its own files in it.
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

  /** One transaction from `readVersion` with the FAIL resolver: any revision
    * committed after `readVersion` rejects the commit instead of rebasing a
    * stale result onto it. Ordinary writes read latest; legacy column
    * replacement pins the snapshot revision; [[createFirst]] reads the empty
    * revision 0.
    */
  private def run(
      basePath: String,
      properties: Map[String, String],
      readVersion: Long
  )(record: MilvusStorageTransaction => Unit): Long = {
    NativeStorageLibrary.load()
    val nativeProperties = new MilvusStorageProperties()
    var transaction: MilvusStorageTransaction = null
    val version =
      try {
        nativeProperties.create(properties)
        transaction = new MilvusStorageTransaction()
        transaction.begin(basePath, nativeProperties.getPtr, readVersion, 0, 1)
        record(transaction)
        transaction.commit()
      } finally {
        try if (transaction != null) transaction.destroy()
        finally nativeProperties.free()
      }
    if (version < 0) {
      throw new IllegalStateException(
        s"committing the manifest at $basePath failed (version $version)"
      )
    }
    version
  }
}
