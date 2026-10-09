package com.zilliz.spark.connector.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnVector

import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.{ConstantColumn, RowOffsetColumn}

/** The two columns synthesized from the physical read position: which segment
  * produced a row and its offset within that segment.
  *
  * `_timestamp` is also a metadata extra column at the Spark API, but it is
  * stored data (Milvus field id 1). It therefore passes through the normal
  * column binding instead of being synthesized here.
  */
object MetadataColumns {

  private val SyntheticColumns = Set(
    MilvusOption.MilvusExtraColumnSegmentID,
    MilvusOption.MilvusExtraColumnRowOffset
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
      partitionName: String,
      segmentId: Long,
      startOffset: Long
  ): Option[ColumnVector] = name match {
    case MilvusOption.MilvusExtraColumnSegmentID =>
      Some(ConstantColumn.ofLong(segmentId))
    case MilvusOption.MilvusExtraColumnRowOffset =>
      Some(new RowOffsetColumn(startOffset))
    case _ => None
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
      partitionName: String,
      segmentId: Long
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
