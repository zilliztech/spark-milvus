package com.zilliz.spark.connector.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnVector

import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.{
  ConstantColumn,
  RowIdColumn,
  RowOffsetColumn
}

/** The columns synthesized from the physical read position: which segment
  * produced a row, its offset within that segment, and `_milvus_row_id`, which
  * binds both to the snapshot that was read.
  *
  * `_timestamp` is also a metadata extra column at the Spark API, but it is
  * stored data (Milvus field id 1). It therefore passes through the normal
  * column binding instead of being synthesized here.
  */
object MetadataColumns {

  private val SyntheticColumns = Set(
    MilvusOption.MilvusExtraColumnSegmentID,
    MilvusOption.MilvusExtraColumnRowOffset,
    MilvusOption.MilvusRowIdColumn
  )

  def isSyntheticColumn(name: String): Boolean = SyntheticColumns(name)

  /** Whether this requested column is synthesized rather than stored. */
  def isMetadataColumn(name: String, requested: Set[String]): Boolean =
    requested.contains(name) && isSyntheticColumn(name)

  /** The column for `name`, or nothing when it is a real field the data
    * carries.
    *
    * @param startOffset
    *   the row offset the batch starts at, since `_row_offset` counts from the
    *   segment and not from the batch.
    */
  def columnFor(
      name: String,
      segmentId: Long,
      startOffset: Long,
      snapshotId: Long = 0L
  ): Option[ColumnVector] = name match {
    case MilvusOption.MilvusExtraColumnSegmentID =>
      Some(ConstantColumn.ofLong(segmentId))
    case MilvusOption.MilvusExtraColumnRowOffset =>
      Some(new RowOffsetColumn(startOffset))
    case MilvusOption.MilvusRowIdColumn =>
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
        s"segment $segmentId was planned without a snapshot id, so it cannot produce '${MilvusOption.MilvusRowIdColumn}'"
      )
    }
    snapshotId
  }

  /** Splices the metadata columns into a row reader's output.
    *
    * The reader underneath produces the data columns in `schema` order with the
    * metadata ones absent, so the two walk together: a metadata column is
    * filled in from the partition, anything else is taken from the next value
    * the reader produced.
    *
    * Returns the reader unchanged when the schema asks for none of them, which
    * is the common case and should cost nothing.
    */
  def wrapRows(
      underlying: RowOffsetReader,
      schema: StructType,
      requested: Set[String],
      segmentId: Long,
      snapshotId: Long = 0L
  ): PartitionReader[InternalRow] = {
    val anyRequested =
      schema.fieldNames.exists(name => isMetadataColumn(name, requested))
    if (!anyRequested) return underlying

    new PartitionReader[InternalRow] {
      override def next(): Boolean = underlying.next()

      override def get(): InternalRow = {
        val row = underlying.get()
        val values = new Array[Any](schema.fields.length)
        var readIndex = 0
        schema.fields.zipWithIndex.foreach { case (field, writeIndex) =>
          field.name match {
            case MilvusOption.MilvusExtraColumnSegmentID
                if requested.contains(field.name) =>
              values(writeIndex) = segmentId
            case MilvusOption.MilvusExtraColumnRowOffset
                if requested.contains(field.name) =>
              values(writeIndex) = underlying.lastReturnedRowOffset
            case MilvusOption.MilvusRowIdColumn
                if requested.contains(field.name) =>
              values(writeIndex) = InternalRow(
                requireSnapshotId(snapshotId, segmentId),
                segmentId,
                underlying.lastReturnedRowOffset
              )
            case _ =>
              values(writeIndex) = row.get(readIndex, field.dataType)
              readIndex += 1
          }
        }
        InternalRow.fromSeq(values.toSeq)
      }

      override def close(): Unit = underlying.close()

      override def currentMetricsValues(): Array[CustomTaskMetric] =
        underlying.currentMetricsValues()
    }
  }
}

/** A row reader that can say where the row it just returned sat in the segment.
  *
  * Only `_row_offset` needs it, and only the reader knows it: it is the batch's
  * start offset plus the row's index within the batch, and both are the
  * reader's own bookkeeping.
  */
trait RowOffsetReader extends PartitionReader[InternalRow] {
  def lastReturnedRowOffset: Long
}
