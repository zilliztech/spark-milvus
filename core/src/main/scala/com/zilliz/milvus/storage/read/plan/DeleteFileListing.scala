package com.zilliz.milvus.storage.read.plan

import scala.util.control.NonFatal

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.manifest.V3ManifestReader
import com.zilliz.milvus.storage.snapshot.{
  DeleteFiles,
  DeltaLogFile,
  SegmentLayout,
  Snapshot
}

/** The delete files a snapshot names, listed on the driver and read nowhere on
  * the driver.
  *
  * A V2 segment lists its delete files in the snapshot itself
  * (`DeleteFiles.Listed`). A V3 segment lists them in its manifest at the read
  * version, so listing means opening that manifest, and resolving the version
  * first when the snapshot entry carries none; the versions resolved that way
  * are returned so the tasks are pinned to them. An L0 segment holds only
  * delete files, which apply to every segment of its partition (partition `-1`
  * meaning all).
  *
  * The files themselves are read on the executor by
  * `core.read.exec.DeletePlans`; the driver never holds a primary-key map.
  */
final case class DeleteFileListing(
    v3BySegment: Map[Long, Seq[DeltaLogFile]],
    v2BySegment: Map[Long, Seq[DeltaLogFile]],
    inheritedByPartition: Map[Long, Seq[DeltaLogFile]],
    v3ReadVersions: Map[Long, Long]
) {

  /** Every file a segment has to apply: the collection-wide L0 files, then its
    * partition's, then its own.
    */
  def filesFor(segmentId: Long, partitionId: Long): Seq[DeltaLogFile] =
    inheritedByPartition.getOrElse(
      DeleteFileListing.AllPartitions,
      Seq.empty
    ) ++
      inheritedByPartition.getOrElse(partitionId, Seq.empty) ++
      v3BySegment.getOrElse(segmentId, Seq.empty) ++
      v2BySegment.getOrElse(segmentId, Seq.empty)
}

object DeleteFileListing {

  /** The partition id an L0 segment carries when its deletes apply to every
    * partition.
    */
  val AllPartitions: Long = -1L

  val empty: DeleteFileListing =
    DeleteFileListing(Map.empty, Map.empty, Map.empty, Map.empty)

  /** Lists the delete files of `snapshot`. With `applyDeletes` false, or a
    * schema without a primary key, nothing is opened and the listing is empty.
    * A manifest that cannot be read is the read's failure, not an empty list.
    */
  def of(
      snapshot: Snapshot,
      applyDeletes: Boolean,
      bucket: String,
      store: ObjectStore
  ): Either[Throwable, DeleteFileListing] =
    // Without a primary key nothing can be deleted, so there is nothing to
    // list and no manifest to open.
    if (!applyDeletes || snapshot.primaryKeyField.isEmpty) Right(empty)
    else
      try Right(list(snapshot, bucket, store))
      catch { case NonFatal(e) => Left(e) }

  private def list(
      snapshot: Snapshot,
      bucket: String,
      store: ObjectStore
  ): DeleteFileListing = {
    val v3 = snapshot.v3Segments.flatMap { seg =>
      val (basePath, listedVersion) = seg.layout match {
        case SegmentLayout.Manifest(path, version) => (path, version)
        case SegmentLayout.ColumnGroups(_) =>
          throw new IllegalStateException(
            s"segment ${seg.id} is storage_version 3 but carries a column-group layout"
          )
      }
      val readVersion =
        if (listedVersion > 0L) listedVersion
        else
          V3ManifestReader
            .latestManifestVersion(basePath, bucket, store)
            .fold(
              e =>
                throw new IllegalStateException(
                  s"cannot resolve the latest manifest version of segment ${seg.id} at $basePath: ${e.getMessage}",
                  e
                ),
              identity
            )
      if (readVersion <= 0L) None
      else {
        val files = V3ManifestReader
          .loadDeltaLogs(basePath, readVersion, bucket, store)
          .fold(
            e =>
              throw new IllegalStateException(
                s"cannot list the delete files of segment ${seg.id} from manifest $readVersion at $basePath: ${e.getMessage}",
                e
              ),
            identity
          )
        Some((seg.id, readVersion, files))
      }
    }
    val v2 = snapshot.v2Segments.collect {
      case seg if seg.hasData =>
        seg.deletes match {
          case DeleteFiles.Listed(files) => seg.id -> files
          case _                         => seg.id -> Seq.empty[DeltaLogFile]
        }
    }
    val inherited = snapshot.deleteOnlySegments
      .groupBy(_.partitionId)
      .map { case (partitionId, segs) =>
        partitionId -> segs.flatMap(_.deltaLogs)
      }
    DeleteFileListing(
      v3BySegment = v3.map { case (id, _, files) => id -> files }.toMap,
      v2BySegment = v2.filter(_._2.nonEmpty).toMap,
      inheritedByPartition = inherited.filter(_._2.nonEmpty),
      v3ReadVersions = v3.map { case (id, version, _) => id -> version }.toMap
    )
  }
}
