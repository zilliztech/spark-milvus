package com.zilliz.spark.connector.types

import org.apache.spark.sql.types.{Decimal, LongType, StringType}
import org.apache.spark.sql.vectorized.{
  ColumnVector,
  ColumnarArray,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

/** A column whose every row holds the same value.
  *
  * The metadata extra columns are like this within one partition: `_partition`
  * and `_segment_id` come from the partition itself, not from the data. Storing
  * one value rather than a buffer of copies is the point.
  */
object ConstantColumn {

  def ofLong(value: Long): ColumnVector = new ConstantLongColumn(value)

  def ofString(value: String): ColumnVector =
    new ConstantStringColumn(UTF8String.fromString(value))
}

private final class ConstantLongColumn(value: Long)
    extends ColumnVector(LongType) {

  override def close(): Unit = ()
  override def hasNull: Boolean = false
  override def numNulls: Int = 0
  override def isNullAt(rowId: Int): Boolean = false
  override def getLong(rowId: Int): Long = value

  override def getChild(ordinal: Int): ColumnVector =
    throw new UnsupportedOperationException("a constant column has no children")
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
    throw new UnsupportedOperationException("this constant column holds a long")
}

private final class ConstantStringColumn(value: UTF8String)
    extends ColumnVector(StringType) {

  override def close(): Unit = ()
  override def hasNull: Boolean = false
  override def numNulls: Int = 0
  override def isNullAt(rowId: Int): Boolean = false
  override def getUTF8String(rowId: Int): UTF8String = value

  override def getChild(ordinal: Int): ColumnVector =
    throw new UnsupportedOperationException("a constant column has no children")
  override def getBoolean(rowId: Int): Boolean = unsupported
  override def getByte(rowId: Int): Byte = unsupported
  override def getShort(rowId: Int): Short = unsupported
  override def getInt(rowId: Int): Int = unsupported
  override def getLong(rowId: Int): Long = unsupported
  override def getFloat(rowId: Int): Float = unsupported
  override def getDouble(rowId: Int): Double = unsupported
  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    unsupported
  override def getBinary(rowId: Int): Array[Byte] = unsupported
  override def getArray(rowId: Int): ColumnarArray = unsupported
  override def getMap(rowId: Int): ColumnarMap = unsupported

  private def unsupported: Nothing =
    throw new UnsupportedOperationException(
      "this constant column holds a string"
    )
}
