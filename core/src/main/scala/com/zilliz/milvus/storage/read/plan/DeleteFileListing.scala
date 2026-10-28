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
  * A V3 segment lists its delete files in its manifest at the read version, so
  * listing means opening that manifest, and resolving the version first when
  * the snapshot entry carries none; the versions resolved that way are returned
  * so the tasks are pinned to them. An L0 segment holds only delete files,
  * which apply within the same source channel and partition (partition `-1`
  * meaning all partitions of that channel).
  *
  * The files themselves are read on the executor by
  * `core.read.exec.DeletePlans`; the driver never holds a primary-key map.
  */
final case class DeleteFileListing(
    v3BySegment: Map[Long, Seq[DeltaLogFile]],
    v3ReadVersions: Map[Long, Long],
    /** Rows of the V3 segments whose manifest was opened, by segment id. A
      * snapshot that states its own row counts does not need these; a snapshot
      * that lists manifests alone has no other source for them.
      */
    v3Rows: Map[Long, Long] = Map.empty,
    /** L0 files resolved by source channel and partition for each data segment.
      */
    inheritedBySegment: Map[Long, Seq[DeltaLogFile]] = Map.empty
) {

  /** Every file a segment has to apply: its resolved L0 files, then its own. */
  def filesFor(segmentId: Long): Seq[DeltaLogFile] =
    inheritedBySegment.getOrElse(segmentId, Seq.empty) ++
      v3BySegment.getOrElse(segmentId, Seq.empty)

}

object DeleteFileListing {

  /** The partition id an L0 segment carries when its deletes apply to every
    * partition.
    */
  val AllPartitions: Long = -1L

  val empty: DeleteFileListing =
    DeleteFileListing(Map.empty, Map.empty)

  /** Whether listing has to open object storage. Data segments need their
    * unpinned versions resolved; enabled deletes also need the data and L0
    * manifests. Disabled deletes never inspect an L0 manifest.
    */
  def requiresStore(snapshot: Snapshot, applyDeletes: Boolean): Boolean = {
    val listsV3Deletes = applyDeletes && snapshot.primaryKeyField.nonEmpty
    snapshot.v3Segments.exists { segment =>
      val version = segment.layout match {
        case SegmentLayout.Manifest(_, readVersion) => readVersion
        case _                                      => -1L
      }
      (segment.hasData || listsV3Deletes) && (version <= 0L || listsV3Deletes)
    }
  }

  /** Resolves V3 data manifest versions and lists the applicable delete files
    * of `snapshot`.
    *
    * Data version resolution is unconditional: a task with an unpinned manifest
    * would otherwise choose whatever is latest when the executor opens it,
    * which is no longer the snapshot the driver planned. L0 versions and delta
    * logs are resolved only when deletes are enabled and the schema has a
    * primary key. A required manifest that cannot be read is the read's
    * failure, not an empty list.
    */
  def of(
      snapshot: Snapshot,
      applyDeletes: Boolean,
      bucket: String,
      store: ObjectStore,
      endpoint: String = ""
  ): Either[Throwable, DeleteFileListing] =
    try {
      require(
        snapshot.segments.forall(_.storageVersion == 3),
        "public reads require Storage V3 segments"
      )
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
    val segments =
      snapshot.v3Segments.filter(seg => listDeleteFiles || seg.hasData)
    val v3 = segments.map { seg =>
      val (basePath, listedVersion) = seg.layout match {
        case SegmentLayout.Manifest(path, version) => (path, version)
        case _ =>
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
    val manifestFiles = v3.map { case (id, _, files, _) => id -> files }.toMap
    val deleteSegments = snapshot.deleteOnlySegments
    val inherited =
      if (!listDeleteFiles) Map.empty[Long, Seq[DeltaLogFile]]
      else
        snapshot.dataSegments
          .map { data =>
            val applicable = deleteSegments.filter { delta =>
              val samePartition = delta.partitionId == AllPartitions ||
                delta.partitionId == data.partitionId
              if (samePartition) {
                require(
                  data.channelName.nonEmpty && delta.channelName.nonEmpty,
                  s"L0 delete matching requires source channels for segments ${data.id} and ${delta.id}"
                )
              }
              samePartition && data.channelName == delta.channelName
            }
            data.id -> applicable
              .flatMap(delta => manifestFiles(delta.id))
              .distinct
          }
          .toMap
          .filter(_._2.nonEmpty)
    DeleteFileListing(
      v3BySegment = v3.collect {
        case (id, _, files, _) if files.nonEmpty => id -> files
      }.toMap,
      v3ReadVersions = v3.map { case (id, version, _, _) =>
        id -> version
      }.toMap,
      v3Rows = v3.collect { case (id, _, _, Some(rows)) => id -> rows }.toMap,
      inheritedBySegment = inherited
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
