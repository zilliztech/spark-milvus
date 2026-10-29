package com.zilliz.milvus.storage.snapshot

import com.zilliz.milvus.storage.read.exec.PhysicalColumnGroup

/** One segment of a fixed snapshot. Format adapters can supply resolved
  * physical column groups without exposing their format-specific metadata.
  */
final case class Segment(
    id: Long,
    partitionId: Long,
    storageVersion: Int,
    rows: Option[Long],
    layout: SegmentLayout,
    deletes: DeleteFiles,
    indexes: SegmentIndexes = SegmentIndexes.Unknown,
    statistics: SegmentStatistics = SegmentStatistics.Unknown,
    isDeleteOnly: Boolean = false,
    channelName: String = ""
) {

  /** L0 segments carry only deletes, including when they have a V3 manifest. */
  def hasData: Boolean = !isDeleteOnly && (layout match {
    case SegmentLayout.Manifest(_, _) => true
    case physical: SegmentLayout.PhysicalColumnGroups =>
      physical.groups.nonEmpty
  })

}

/** Where a segment's auxiliary statistics are described. Unlike
  * [[DeleteFiles]], missing statistics are an optimization gap, not a
  * correctness error: a pruner must retain the segment when this is
  * [[SegmentStatistics.Unknown]] or when a listed statistic cannot be read.
  */
sealed trait SegmentStatistics extends Serializable

object SegmentStatistics {

  /** The source cannot describe statistics for this segment. */
  case object Unknown extends SegmentStatistics

  /** An input adapter supplied statistics files by field id. */
  final case class Listed(filesByField: Map[Long, Seq[String]])
      extends SegmentStatistics

  /** StorageV3: the pinned segment manifest contains the statistics map. */
  case object InManifest extends SegmentStatistics
}

/** A pinned manifest or already-resolved physical files. */
sealed trait SegmentLayout extends Serializable

object SegmentLayout {

  /** `storage_version = 3`: the native library reads the layout out of the
    * manifest at `basePath`.
    *
    * @param readVersion
    *   the manifest version to read, zero for the initial empty state, or -1
    *   for the latest. A planner that resolved a version pins it, so every task
    *   in the job reads the same one even if a compaction commits a newer
    *   manifest meanwhile.
    */
  final case class Manifest(basePath: String, readVersion: Long = -1L)
      extends SegmentLayout

  /** A format adapter has already resolved physical files and column names.
    * This carries no snapshot-version, footer or field-slot interpretation.
    * Arrow schema JSON and maps survive transport without open resources.
    */
  final case class PhysicalColumnGroups(
      groups: Seq[PhysicalColumnGroup],
      arrowSchemaJson: String,
      fieldNames: Map[Long, String],
      outputColumns: Map[String, String]
  ) extends SegmentLayout {
    require(
      groups != null && groups.nonEmpty && groups.forall(_ != null),
      "physical column groups must not be empty or contain null"
    )
    require(
      arrowSchemaJson != null && arrowSchemaJson.nonEmpty,
      "physical column groups require an Arrow schema"
    )
    require(
      fieldNames != null && fieldNames.nonEmpty &&
        fieldNames.values.forall(name => name != null && name.nonEmpty),
      "physical column groups require field names"
    )
    require(
      outputColumns != null && outputColumns.forall { case (name, column) =>
        name != null && name.nonEmpty && column != null && column.nonEmpty
      },
      "physical output column names must not be blank"
    )
  }
}

/** Where a segment's delete files are. Two of the three states carry
  * information; `Unknown` is the state a source that cannot say must use, so
  * that a read with deletes on fails at planning instead of returning deleted
  * rows.
  */
sealed trait DeleteFiles

object DeleteFiles {

  /** The source confirmed there are no deletes. */
  case object Empty extends DeleteFiles

  /** V3: the manifest at the segment's read version lists them. */
  case object InManifest extends DeleteFiles

  /** The source cannot tell. */
  case object Unknown extends DeleteFiles
}
