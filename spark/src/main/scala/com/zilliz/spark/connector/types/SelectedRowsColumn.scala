package com.zilliz.spark.connector.types

import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{
  ColumnVector,
  ColumnarArray,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

/** Presents another column's rows in a different order, or a subset of them.
  *
  * This is how a batch with deletes is delivered. Spark's `ColumnarBatch` has
  * no way to say "these rows are valid" — it has a row count and nothing else —
  * so the surviving rows have to become a batch of their own. Doing it by
  * remapping indices rather than copying buffers keeps the cost to one int per
  * surviving row, whatever the columns hold; a vector column would otherwise be
  * copied in full.
  *
  * @param rows
  *   positions in `underlying`, in the order they are to be delivered
  */
final class SelectedRowsColumn(
    underlying: ColumnVector,
    rows: Array[Int],
    dataType: DataType
) extends ColumnVector(dataType) {

  override def close(): Unit = underlying.close()

  override def hasNull: Boolean = underlying.hasNull

  override def numNulls: Int = rows.count(underlying.isNullAt)

  override def isNullAt(rowId: Int): Boolean = underlying.isNullAt(rows(rowId))

  override def getBoolean(rowId: Int): Boolean =
    underlying.getBoolean(rows(rowId))
  override def getByte(rowId: Int): Byte = underlying.getByte(rows(rowId))
  override def getShort(rowId: Int): Short = underlying.getShort(rows(rowId))
  override def getInt(rowId: Int): Int = underlying.getInt(rows(rowId))
  override def getLong(rowId: Int): Long = underlying.getLong(rows(rowId))
  override def getFloat(rowId: Int): Float = underlying.getFloat(rows(rowId))
  override def getDouble(rowId: Int): Double = underlying.getDouble(rows(rowId))
  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    underlying.getDecimal(rows(rowId), precision, scale)
  override def getUTF8String(rowId: Int): UTF8String =
    underlying.getUTF8String(rows(rowId))
  override def getBinary(rowId: Int): Array[Byte] =
    underlying.getBinary(rows(rowId))
  override def getArray(rowId: Int): ColumnarArray =
    underlying.getArray(rows(rowId))
  override def getMap(rowId: Int): ColumnarMap = underlying.getMap(rows(rowId))
  override def getChild(ordinal: Int): ColumnVector =
    underlying.getChild(ordinal)
}
