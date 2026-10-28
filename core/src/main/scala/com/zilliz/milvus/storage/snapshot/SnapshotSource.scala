package com.zilliz.milvus.storage.snapshot

import scala.util.control.NonFatal

/** One fixed file input, resolved once before partition planning. */
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
