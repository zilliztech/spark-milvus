package com.zilliz.milvus.storage.snapshot

/** The collection's index definition in the snapshot JSON. Engine parameters
  * are kept separate from the parameters originally supplied by the user.
  */
final case class CollectionIndex(
    collectionId: Long,
    fieldId: Long,
    indexId: Long,
    name: String,
    typeParameters: Map[String, String],
    indexParameters: Map[String, String],
    userIndexParameters: Map[String, String]
) extends Serializable

/** One persisted index build pinned by a segment's snapshot Avro record. Paths
  * are exact object keys in the snapshot bucket. `indexVersion` is the
  * build/path version, `currentIndexVersion` is the vector serialization
  * version, and `indexStorePathVersion` describes the path layout. An absent
  * version remains absent; none of these versions can substitute for another.
  */
final case class SegmentIndex(
    collectionId: Long,
    partitionId: Long,
    segmentId: Long,
    fieldId: Long,
    indexId: Long,
    buildId: Long,
    name: String,
    parameters: Map[String, String],
    filePaths: Vector[String],
    rowCount: Long,
    serializedSize: Long,
    indexVersion: Long,
    currentIndexVersion: Option[Int],
    indexStorePathVersion: Option[Int]
) extends Serializable {
  def indexType: Option[String] = parameters.get("index_type")
  def metricType: Option[String] = parameters.get("metric_type")
}

/** What the snapshot source actually knows about persisted segment indexes. */
sealed trait SegmentIndexes extends Serializable

object SegmentIndexes {

  /** The source did not supply segment index metadata. */
  case object Unknown extends SegmentIndexes

  /** The authoritative metadata lists no persisted index files. */
  case object Unindexed extends SegmentIndexes

  /** The exact builds and files named by the snapshot. */
  final case class Available(indexes: Vector[SegmentIndex])
      extends SegmentIndexes {
    require(indexes.nonEmpty, "available segment indexes must not be empty")
    require(
      indexes.forall(_.filePaths.nonEmpty),
      "available indexes must name their files"
    )
  }
}
