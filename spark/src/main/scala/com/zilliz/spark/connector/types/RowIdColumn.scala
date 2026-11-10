package com.zilliz.spark.connector.types

import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.vectorized.{
  ColumnVector,
  ColumnarArray,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

/** The `_milvus_row_id` struct of one batch: the snapshot and segment are the
  * same for every row, and the offset counts up from the batch's start.
  */
final class RowIdColumn(snapshotId: Long, segmentId: Long, startOffset: Long)
    extends ColumnVector(RowIdentity.dataType) {

  private val children: Array[ColumnVector] = Array(
    ConstantColumn.ofLong(snapshotId),
    ConstantColumn.ofLong(segmentId),
    new RowOffsetColumn(startOffset)
  )

  override def close(): Unit = ()
  override def hasNull: Boolean = false
  override def numNulls: Int = 0
  override def isNullAt(rowId: Int): Boolean = false
  override def getChild(ordinal: Int): ColumnVector = children(ordinal)

  override def getBoolean(rowId: Int): Boolean = unsupported
  override def getByte(rowId: Int): Byte = unsupported
  override def getShort(rowId: Int): Short = unsupported
  override def getInt(rowId: Int): Int = unsupported
  override def getLong(rowId: Int): Long = unsupported
  override def getFloat(rowId: Int): Float = unsupported
  override def getDouble(rowId: Int): Double = unsupported
  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    unsupported
  override def getUTF8String(rowId: Int): UTF8String = unsupported
  override def getBinary(rowId: Int): Array[Byte] = unsupported
  override def getArray(rowId: Int): ColumnarArray = unsupported
  override def getMap(rowId: Int): ColumnarMap = unsupported

  private def unsupported: Nothing =
    throw new UnsupportedOperationException(
      "a row identity is a struct; read its fields"
    )
}
