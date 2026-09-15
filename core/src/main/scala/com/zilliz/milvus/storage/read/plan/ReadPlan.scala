package com.zilliz.milvus.storage.read.plan

import com.zilliz.milvus.storage.delete.{DeletePlan, DeltaLogReader}
import com.zilliz.milvus.storage.snapshot.{SegmentLayout, Snapshot}
import com.zilliz.milvus.storage.Logging

/** A whole read: one [[SegmentReadTask]] per partition, plus what planning
  * already knows about the total.
  *
  * The totals are what a Spark `Statistics` reports (capability R13). They are
  * options because a manifest layout does not reveal its row count on the
  * driver without opening the manifest, and guessing a number that feeds the
  * optimizer is worse than admitting there is none.
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

  /** The delete plans the driver has already read, keyed by segment id for a
    * segment's own delete files and by partition id for the L0 delete-only
    * segments that apply to every segment of a partition (`-1` for all).
    * Reading them on the executor instead is `DeleteSource.Files`, which no
    * planner emits yet.
    */
  final case class Deletes(
      v3BySegment: Map[Long, DeletePlan],
      v2BySegment: Map[Long, DeletePlan],
      inheritedByPartition: Map[Long, DeletePlan]
  )

  object Deletes {
    val none: Deletes = Deletes(Map.empty, Map.empty, Map.empty)
  }

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
    *   what the driver read. A V3 task carries the union of its partition's
    *   inherited plan and its own; a V2 task carries only its own, because the
    *   inherited plan is not shipped per partition but resolved once by the
    *   reader factory from the same map.
    * @param readVersions
    *   a manifest version per V3 segment that overrides the one the layout
    *   names, for a segment whose snapshot entry carried no version.
    */
  def of(
      snapshot: Snapshot,
      properties: Int => Map[String, String],
      applyDeletes: Boolean,
      deletes: Deletes = Deletes.none,
      readVersions: Map[Long, Long] = Map.empty
  ): ReadPlan = {
    val propertiesByVersion =
      collection.mutable.Map.empty[Int, Map[String, String]]
    def propertiesFor(version: Int): Map[String, String] =
      propertiesByVersion.getOrElseUpdate(version, properties(version))
    def deleteSourceFor(plan: DeletePlan): DeleteSource =
      if (!applyDeletes || plan.isEmpty) DeleteSource.None
      else DeleteSource.Materialized(plan)
    val schemaBytes = snapshot.schemaBytes

    val v3Tasks = snapshot.v3Segments.map { seg =>
      val (basePath, listedVersion) = seg.layout match {
        case SegmentLayout.Manifest(path, version) => (path, version)
        case SegmentLayout.ColumnGroups(_) =>
          throw new IllegalStateException(
            s"segment ${seg.id} is storage_version 3 but carries a column-group layout"
          )
      }
      val readVersion = readVersions.getOrElse(seg.id, listedVersion)
      logInfo(
        s"Planning segment ${seg.id} of partition ${seg.partitionId}: manifest $basePath at version $readVersion"
      )
      val plan = DeletePlan.union(
        DeltaLogReader.effectiveInheritedDeletePlan(
          seg.partitionId,
          deletes.inheritedByPartition
        ),
        deletes.v3BySegment.getOrElse(seg.id, DeletePlan.empty)
      )
      SegmentReadTask(
        segmentId = seg.id,
        partitionId = seg.partitionId,
        layout = SegmentLayout.Manifest(basePath, readVersion),
        schemaBytes = schemaBytes,
        properties = propertiesFor(3),
        deletes = deleteSourceFor(plan)
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
        deletes = deleteSourceFor(
          deletes.v2BySegment.getOrElse(seg.id, DeletePlan.empty)
        )
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
