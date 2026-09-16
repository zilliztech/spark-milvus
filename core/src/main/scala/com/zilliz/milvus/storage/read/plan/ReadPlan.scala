package com.zilliz.milvus.storage.read.plan

import com.zilliz.milvus.storage.snapshot.{SegmentLayout, Snapshot}
import com.zilliz.milvus.storage.Logging

/** A whole read: one [[SegmentReadTask]] per partition, plus what planning
  * already knows about the total.
  *
  * The totals are what a Spark `Statistics` reports (capability R13). They are
  * options because some sources provide neither snapshot Avro row counts nor
  * materialized column groups. A missing count must not be guessed.
  */
final case class ReadPlan(specs: Seq[SegmentReadTask]) extends Serializable {

  def isEmpty: Boolean = specs.isEmpty

  /** Sum of the per-partition expectations, or nothing when any partition
    * cannot state one. A partial sum would read as the table's size.
    */
  def totalRows: Option[Long] =
    if (specs.isEmpty) Some(0L)
    else {
      val counts = specs.map(_.expectedRows)
      if (counts.forall(_.isDefined)) Some(counts.flatten.sum) else scala.None
    }

  def partitionsApplyingDeletes: Int = specs.count(_.appliesDeletes)
}

/** How a snapshot becomes a plan: one task per data segment, in the order the
  * snapshot lists them, V3 first. This is the driver's part of a read; the task
  * is what an executor opens.
  */
object ReadPlan extends Logging {

  /** @param properties
    *   the parsed `fs.*` map a task of the given `storage_version` carries. A
    *   function, and called at most once per version present, because the two
    *   lines are configured differently: a backup read has a bucket derived
    *   from `milvus.backup.dir` and no `fs.bucket_name`, produces V2 tasks
    *   only, and must not be made to validate the V3 configuration no task
    *   would use.
    * @param applyDeletes
    *   false turns every task's deletes into `DeleteSource.None`.
    * @param deletes
    *   the delete files the driver listed. Every task carries the files it has
    *   to apply as `DeleteSource.Files`: the L0 files of its partition and of
    *   the whole collection, then its own. The executor reads them.
    */
  def of(
      snapshot: Snapshot,
      properties: Int => Map[String, String],
      applyDeletes: Boolean,
      deletes: DeleteFileListing = DeleteFileListing.empty,
      neededFieldIds: Seq[Long] = Seq.empty
  ): ReadPlan = {
    val propertiesByVersion =
      collection.mutable.Map.empty[Int, Map[String, String]]
    def propertiesFor(version: Int): Map[String, String] =
      propertiesByVersion.getOrElseUpdate(version, properties(version))
    def deleteSourceFor(segmentId: Long, partitionId: Long): DeleteSource = {
      val files =
        if (applyDeletes) deletes.filesFor(segmentId, partitionId)
        else Seq.empty
      if (files.isEmpty) DeleteSource.None else DeleteSource.Files(files)
    }
    val schemaBytes = snapshot.schemaBytes

    val v3Tasks = snapshot.v3Segments.map { seg =>
      val (basePath, listedVersion) = seg.layout match {
        case SegmentLayout.Manifest(path, version) => (path, version)
        case SegmentLayout.ColumnGroups(_) =>
          throw new IllegalStateException(
            s"segment ${seg.id} is storage_version 3 but carries a column-group layout"
          )
      }
      val readVersion = deletes.v3ReadVersions.getOrElse(seg.id, listedVersion)
      if (readVersion <= 0L) {
        throw new IllegalStateException(
          s"cannot plan V3 segment ${seg.id} at $basePath without a positive manifest version; " +
            "resolve and pin the version before building the read plan"
        )
      }
      logInfo(
        s"Planning segment ${seg.id} of partition ${seg.partitionId}: manifest $basePath at version $readVersion"
      )
      SegmentReadTask(
        segmentId = seg.id,
        partitionId = seg.partitionId,
        layout = SegmentLayout.Manifest(basePath, readVersion),
        schemaBytes = schemaBytes,
        properties = propertiesFor(3),
        neededFieldIds = neededFieldIds,
        deletes = deleteSourceFor(seg.id, seg.partitionId),
        indexes = seg.indexes,
        snapshotRows = seg.rows
      )
    }

    val v2Tasks = snapshot.v2Segments.filter(_.hasData).map { seg =>
      val groups = seg.layout match {
        case SegmentLayout.ColumnGroups(gs) => gs
        case SegmentLayout.Manifest(_, _) =>
          throw new IllegalStateException(
            s"segment ${seg.id} is storage_version 2 but carries a manifest layout"
          )
      }
      SegmentReadTask(
        segmentId = seg.id,
        partitionId = seg.partitionId,
        layout = SegmentLayout.ColumnGroups(groups),
        schemaBytes = schemaBytes,
        properties = propertiesFor(2),
        neededFieldIds = neededFieldIds,
        deletes = deleteSourceFor(seg.id, seg.partitionId),
        indexes = seg.indexes,
        snapshotRows = seg.rows
      )
    }

    val deleteOnly = snapshot.v2Segments.count(!_.hasData)
    if (deleteOnly > 0) {
      logInfo(s"Skipped $deleteOnly delete-only StorageV2 segment(s)")
    }
    logInfo(
      s"Planned ${v3Tasks.size} V3 and ${v2Tasks.size} V2 task(s) of ${snapshot.name}"
    )
    ReadPlan(v3Tasks ++ v2Tasks)
  }
}
