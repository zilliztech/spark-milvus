package com.zilliz.spark.connector.read

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnVector

import com.zilliz.milvus.storage.schema.{FieldMetadata, SchemaMapper}
import com.zilliz.spark.connector.types.{
  ConstantColumn,
  RowIdColumn,
  RowIdentity,
  RowOffsetColumn,
  SparkTypes
}
import io.milvus.grpc.schema.{CollectionSchema, DataType => MilvusDataType}

/** Metadata synthesized from the task's Milvus partition, segment and physical
  * row offset, including the row identity bound to the snapshot that was read.
  *
  * `_timestamp` is also a hidden column at the Spark API, but it is stored data
  * (Milvus field id 1). It therefore passes through the normal column binding
  * instead of being synthesized here.
  */
object MetadataColumns {

  val PartitionId = "_partition_id"
  val SegmentId = "_segment_id"
  val RowOffset = "_row_offset"
  val Timestamp = "_timestamp"

  val partitionIdField: StructField =
    StructField(PartitionId, LongType, nullable = false)

  private val positionFields = Seq(
    partitionIdField,
    StructField(SegmentId, LongType, nullable = false),
    StructField(RowOffset, LongType, nullable = false)
  )

  /** Spark resolves ordinary columns first. A visible collection field with the
    * same name (including case variants) shadows the metadata column.
    */
  def hiddenFields(
      visible: StructType,
      collection: CollectionSchema,
      physicalRead: Boolean = false
  ): Seq[StructField] = {
    def available(name: String): Boolean =
      !visible.fieldNames.exists(_.equalsIgnoreCase(name))
    val positions = positionFields.filter(field => available(field.name))
    val timestamps =
      if (available(Timestamp)) Seq(timestampField(collection)) else Nil
    val identity =
      if (physicalRead && available(RowIdentity.ColumnName))
        Seq(RowIdentity.field)
      else Nil
    positions ++ timestamps ++ identity
  }

  private def timestampField(collection: CollectionSchema): StructField = {
    val timestamp =
      (collection.fields ++ SchemaMapper.missingSystemFields(collection))
        .find(_.fieldID == SchemaMapper.TimestampFieldId)
        .get
    require(
      timestamp.dataType == MilvusDataType.Int64,
      s"Snapshot field id 1 must be the Int64 timestamp, got ${timestamp.dataType}"
    )
    SparkTypes
      .toStructField(timestamp, rawVectors = false)
      .copy(name = Timestamp)
  }

  private val syntheticNames =
    positionFields.map(_.name).toSet + RowIdentity.ColumnName

  /** A stored collection field with the same name carries its own field ID. */
  def isSynthetic(field: StructField): Boolean =
    syntheticNames(field.name) && !field.metadata.contains(
      FieldMetadata.MilvusFieldIdMetadataKey
    )

  /** The column for `name`, or nothing when it is a real field the data
    * carries.
    *
    * @param startOffset
    *   the row offset the batch starts at, since `_row_offset` counts from the
    *   segment and not from the batch.
    */
  def columnFor(
      name: String,
      partitionId: Long,
      segmentId: Long,
      startOffset: Long,
      snapshotId: Long = 0L
  ): Option[ColumnVector] = name match {
    case PartitionId =>
      Some(ConstantColumn.ofLong(partitionId))
    case SegmentId =>
      Some(ConstantColumn.ofLong(segmentId))
    case RowOffset =>
      Some(new RowOffsetColumn(startOffset))
    case RowIdentity.ColumnName =>
      Some(
        new RowIdColumn(
          requireSnapshotId(snapshotId, segmentId),
          segmentId,
          startOffset
        )
      )
    case _ => None
  }

  /** The snapshot id a row identity carries. A read that requests the identity
    * is planned with the snapshot's id, so zero here is a planning defect.
    */
  private def requireSnapshotId(snapshotId: Long, segmentId: Long): Long = {
    if (snapshotId <= 0L) {
      throw new IllegalStateException(
        s"segment $segmentId was planned without a snapshot id, so it cannot produce '${RowIdentity.ColumnName}'"
      )
    }
    snapshotId
  }

}
