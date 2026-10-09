package com.zilliz.milvus.storage.snapshot

import scala.util.control.NonFatal

/** One read entry point, already bound to where its snapshot is: a location in
  * the snapshot directory, a milvus-backup export, the 1.x option strings.
  *
  * `getTable` builds one for the read's `ReadMode` and calls it once; the
  * `Snapshot` it returns is carried from the table to the scan, so no stage
  * after it opens storage to find out what the collection looks like.
  * Implementations: `SnapshotCatalog.at`, `latestOf` and `named` (core),
  * `BackupSnapshotSource` (compat.backup), and the client and option-string
  * sources in `spark.options`, which need types core does not have.
  */
trait SnapshotSource {
  def snapshot(): Either[Throwable, Snapshot]
}

object SnapshotSource {

  /** A source from an expression that builds the snapshot or throws. */
  def apply(build: => Snapshot): SnapshotSource = new SnapshotSource {
    def snapshot(): Either[Throwable, Snapshot] =
      try Right(build)
      catch { case NonFatal(e) => Left(e) }
  }
}
