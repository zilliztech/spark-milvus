package com.zilliz.milvus.storage.snapshot

import io.milvus.grpc.schema.{CollectionSchema => ProtoSchema, FieldSchema}

/** One fixed view of a collection: what a read plans against.
  *
  * Every read entry point produces one of these and nothing downstream looks
  * at the entry point's own format again. It stays on the driver; the
  * per-partition projection that reaches executors is
  * `core.read.plan.SegmentReadTask`.
  *
  * Fields a source cannot supply are `Option` or carry an explicit state, so a
  * missing value is visible to the planner instead of defaulting to "none".
  * `docs/design/architecture/snapshot.html` is the design.
  */
final case class Snapshot(
    name: String,
    collectionId: Long,
    createdAt: Option[Long],
    schema: ProtoSchema,
    partitionIds: Seq[Long],
    segments: Seq[Segment],
    origin: SnapshotOrigin
) {

  /** The schema as the bytes every `SegmentReadTask` carries. */
  def schemaBytes: Array[Byte] = schema.toByteArray

  def primaryKeyField: Option[FieldSchema] = schema.fields.find(_.isPrimaryKey)

  /** Segments that hold rows and become partitions. */
  def dataSegments: Seq[Segment] = segments.filter(_.hasData)

  /** Segments that carry only deletes (Milvus L0 segments): their delete files
    * apply to every data segment of the same partition.
    */
  def deleteOnlySegments: Seq[Segment] =
    segments.filter(s => !s.hasData && s.deletes != DeleteFiles.Empty)

  def v3Segments: Seq[Segment] = segments.filter(_.storageVersion == 3)
  def v2Segments: Seq[Segment] = segments.filter(_.storageVersion == 2)
}

/** Which entry point produced a `Snapshot`; for errors and logs. */
sealed trait SnapshotOrigin

object SnapshotOrigin {

  /** Read from a snapshot JSON at `location` in the snapshot directory. */
  final case class Catalog(location: String) extends SnapshotOrigin

  /** Translated from a milvus-backup export at `dir`. */
  final case class Backup(dir: String) extends SnapshotOrigin

  /** Deserialized from the 1.x option strings. Goes with capability K2. */
  case object Options extends SnapshotOrigin
}
