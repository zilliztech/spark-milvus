package com.zilliz.spark.connector.types

import org.apache.spark.sql.types.{DataType, Decimal, LongType, StringType}
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
