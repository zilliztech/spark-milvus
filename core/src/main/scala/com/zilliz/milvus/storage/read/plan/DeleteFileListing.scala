package com.zilliz.milvus.storage.read.plan

import scala.util.control.NonFatal

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.manifest.{ManifestFacts, V3ManifestReader}
import com.zilliz.milvus.storage.path.StoragePath
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
    v3ReadVersions: Map[Long, Long],
    /** Rows of the V3 segments whose manifest was opened, by segment id. A
      * snapshot that states its own row counts does not need these; a snapshot
      * that lists manifests alone has no other source for them.
      */
    v3Rows: Map[Long, Long] = Map.empty
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

  /** Whether listing has to open object storage. V2 delete files are already
    * named by the snapshot; V3 needs its manifest only to resolve an unpinned
    * version or to list enabled delete files.
    */
  def requiresStore(snapshot: Snapshot, applyDeletes: Boolean): Boolean = {
    val listsV3Deletes = applyDeletes && snapshot.primaryKeyField.nonEmpty
    snapshot.v3Segments.exists { segment =>
      val version = segment.layout match {
        case SegmentLayout.Manifest(_, readVersion) => readVersion
        case SegmentLayout.ColumnGroups(_)          => -1L
      }
      version <= 0L || listsV3Deletes
    }
  }

  /** Resolves every V3 manifest version and lists the applicable delete files
    * of `snapshot`.
    *
    * Version resolution is unconditional: a task with an unpinned manifest
    * would otherwise choose whatever is latest when the executor opens it,
    * which is no longer the snapshot the driver planned. Delta logs are read
    * from a manifest only when deletes are enabled and the schema has a primary
    * key. A manifest that has to be read and cannot be is the read's failure,
    * not an empty list.
    */
  def of(
      snapshot: Snapshot,
      applyDeletes: Boolean,
      bucket: String,
      store: ObjectStore,
      endpoint: String = ""
  ): Either[Throwable, DeleteFileListing] =
    try {
      if (applyDeletes) validateV2DeleteState(snapshot)
      Right(
        list(
          snapshot,
          listDeleteFiles = applyDeletes && snapshot.primaryKeyField.nonEmpty,
          bucket,
          store,
          endpoint
        )
      )
    } catch { case NonFatal(e) => Left(e) }

  private def list(
      snapshot: Snapshot,
      listDeleteFiles: Boolean,
      bucket: String,
      store: ObjectStore,
      endpoint: String
  ): DeleteFileListing = {
    val v3 = snapshot.v3Segments.map { seg =>
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
      if (readVersion <= 0L) {
        throw new IllegalStateException(
          s"cannot pin V3 segment ${seg.id} at $basePath: " +
            s"latest manifest version must be positive, got $readVersion"
        )
      }
      // The manifest is read for the delete files; the segment's rows come
      // out of the same record, so a snapshot that did not carry a row count
      // has one here at no further cost. A read that lists no delete files
      // opens no manifest and learns nothing, which is why the row count is
      // an Option all the way up.
      val files =
        if (!listDeleteFiles) ManifestFacts(Seq.empty, scala.None)
        else
          V3ManifestReader
            .load(basePath, readVersion, bucket, store)
            .fold(
              e =>
                throw new IllegalStateException(
                  s"cannot list the delete files of segment ${seg.id} from manifest $readVersion at $basePath: ${e.getMessage}",
                  e
                ),
              identity
            )
      val deltaLogs = files.deltaLogs.map(file =>
        file.copy(logPath = deleteLogKey(file.logPath, bucket, endpoint))
      )
      (seg.id, readVersion, deltaLogs, files.rows)
    }
    val v2 =
      if (!listDeleteFiles) Seq.empty[(Long, Seq[DeltaLogFile])]
      else
        snapshot.v2Segments.collect {
          case seg if seg.hasData =>
            seg.deletes match {
              case DeleteFiles.Listed(files) => seg.id -> files
              case _ => seg.id -> Seq.empty[DeltaLogFile]
            }
        }
    val inherited =
      if (!listDeleteFiles) Map.empty[Long, Seq[DeltaLogFile]]
      else
        snapshot.deleteOnlySegments
          .groupBy(_.partitionId)
          .map { case (partitionId, segs) =>
            partitionId -> segs.flatMap(_.deltaLogs)
          }
    DeleteFileListing(
      v3BySegment = v3.collect {
        case (id, _, files, _) if files.nonEmpty => id -> files
      }.toMap,
      v2BySegment = v2.filter(_._2.nonEmpty).toMap,
      inheritedByPartition = inherited.filter(_._2.nonEmpty),
      v3ReadVersions = v3.map { case (id, version, _, _) =>
        id -> version
      }.toMap,
      v3Rows = v3.collect { case (id, _, _, Some(rows)) => id -> rows }.toMap
    )
  }

  private def validateV2DeleteState(snapshot: Snapshot): Unit =
    snapshot.v2Segments.find(_.deletes == DeleteFiles.Unknown).foreach { seg =>
      throw new IllegalStateException(
        s"cannot plan deletes for V2 segment ${seg.id} in snapshot '${snapshot.name}' " +
          s"from ${snapshot.origin}: its delete-file state is unknown"
      )
    }

  private def deleteLogKey(
      path: String,
      bucket: String,
      endpoint: String
  ): String = {
    val located = StoragePath.parseMilvus(path, bucket, endpoint)
    if (bucket.nonEmpty && located.hasBucket && located.bucket != bucket) {
      throw new IllegalArgumentException(
        s"delete log is in bucket '${located.bucket}' but the read is bound to '$bucket': $path"
      )
    }
    located.key
  }
}
