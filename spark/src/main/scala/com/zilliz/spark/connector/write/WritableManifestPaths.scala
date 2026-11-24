package com.zilliz.spark.connector.write

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.snapshot.{SnapshotCatalog, SnapshotLayout}

/** The driver binds writable paths to the caller's snapshot location. Keys in
  * the materialized snapshot have already had their bucket resolved.
  */
private[write] final class WritableManifestPaths private (
    root: Vector[String],
    collectionId: Long,
    storage: Map[String, String]
) {
  def validate(
      segments: Seq[PlannedSegment],
      layout: Option[SnapshotLayout]
  ): Unit = {
    val roots = layout match {
      case Some(SnapshotLayout.Referenced)    => Vector(root)
      case Some(SnapshotLayout.SelfContained) => Vector(root :+ "files")
      case Some(SnapshotLayout.Unsupported(value)) =>
        throw new IllegalArgumentException(
          s"Column replacement does not support snapshot layout '$value'"
        )
      case None => Vector(root, root :+ "files")
    }
    val used = segments.iterator.map { segment =>
      val key = NativeObjectStore.filesystemKey(storage, segment.basePath)
      // Store adapters and native transactions must resolve the key identically
      // when a relative local root is also a leading key component.
      require(
        NativeObjectStore.filesystemKey(storage, key) == key,
        s"Segment ${segment.id} manifest base '${segment.basePath}' repeats the configured storage root"
      )
      val suffix =
        s"insert_log/$collectionId/${segment.partitionId}/${segment.id}"
      roots.find(prefix => (prefix :+ suffix).mkString("/") == key).getOrElse {
        throw new IllegalArgumentException(
          s"Segment ${segment.id} manifest base '${segment.basePath}' does not match its writable location for '$suffix'"
        )
      }
    }.toSet
    require(
      used.size <= 1,
      "Column replacement requires a single data root for all segments in the snapshot"
    )
  }
}

private[write] object WritableManifestPaths {
  def apply(
      source: SourceSnapshot,
      storage: Map[String, String]
  ): WritableManifestPaths = {
    val bucket = storage.getOrElse(StorageProperties.BucketName, "")
    val located = SnapshotCatalog.metadataLocation(
      source.path,
      bucket,
      storage.getOrElse(StorageProperties.Address, "")
    )
    val key = NativeObjectStore.filesystemKey(storage, located.key)
    val parts = key.split("/", -1).toVector
    require(
      parts.forall(p => p.nonEmpty && p != "." && p != "..") && !key.contains(
        '\\'
      ) && !key.contains('\u0000'),
      s"Column replacement needs a canonical snapshot location: ${source.path}"
    )
    val root = parts match {
      case prefix :+ "snapshots" :+ collection :+ "metadata" :+ filename =>
        require(
          collection == source.collectionId.toString && filename == s"${source.id}.json",
          s"Snapshot location '${source.path}' does not match collection ${source.collectionId} and snapshot ${source.id}"
        )
        prefix
      case prefix :+ "snapshots" :+ _ => prefix
      case _                          => parts.init
    }
    new WritableManifestPaths(root, source.collectionId, storage)
  }

}
