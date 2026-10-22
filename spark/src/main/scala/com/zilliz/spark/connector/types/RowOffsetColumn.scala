package com.zilliz.spark.connector.types

import org.apache.spark.sql.types.{Decimal, LongType}
import org.apache.spark.sql.vectorized.{
  ColumnVector,
  ColumnarArray,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

/** A column of consecutive longs: `base`, `base + 1`, and so on.
  *
  * What `_row_offset` is — the row's position in the segment — which is the
  * batch's own start offset plus the row's index within it.
  */
final class RowOffsetColumn(base: Long) extends ColumnVector(LongType) {

  override def close(): Unit = ()
  override def hasNull: Boolean = false
  override def numNulls: Int = 0
  override def isNullAt(rowId: Int): Boolean = false
  override def getLong(rowId: Int): Long = base + rowId

  override def getChild(ordinal: Int): ColumnVector =
    throw new UnsupportedOperationException(
      "a row offset column has no children"
    )
  override def getBoolean(rowId: Int): Boolean = unsupported
  override def getByte(rowId: Int): Byte = unsupported
  override def getShort(rowId: Int): Short = unsupported
  override def getInt(rowId: Int): Int = unsupported
  override def getFloat(rowId: Int): Float = unsupported
  override def getDouble(rowId: Int): Double = unsupported
  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    unsupported
  override def getUTF8String(rowId: Int): UTF8String = unsupported
  override def getBinary(rowId: Int): Array[Byte] = unsupported
  override def getArray(rowId: Int): ColumnarArray = unsupported
  override def getMap(rowId: Int): ColumnarMap = unsupported

  private def unsupported: Nothing =
    throw new UnsupportedOperationException("a row offset is a long")
}
