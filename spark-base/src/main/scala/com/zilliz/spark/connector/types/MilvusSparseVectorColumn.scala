package com.zilliz.spark.connector.types

import org.apache.arrow.vector.VarBinaryVector
import org.apache.spark.sql.types.{Decimal, FloatType, LongType, MapType}
import org.apache.spark.sql.vectorized.{
  ColumnVector,
  ColumnarArray,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

/** Presents a Milvus sparse float vector to Spark as a map column.
  *
  * A sparse vector is stored as variable-width binary: eight bytes an entry, a
  * uint32 index then a float32 value, little endian. Arrow lays every row's
  * bytes end to end in one buffer, so entry `e` of the whole column sits at
  * byte `e * 8` and a row's entries are the window its offsets describe. That
  * means the keys and the values are two views onto the buffer and nothing is
  * decoded until something asks for an entry.
  *
  * The presented type is `MapType(LongType, FloatType)`, which is what the row
  * path produces, so a scan reads the same either way. The key widens to Long
  * because the stored index is unsigned and would not survive an Int.
  */
object MilvusSparseVectorColumn {

  private val EntryWidth = 8

  val sparkType: MapType =
    MapType(LongType, FloatType, valueContainsNull = false)

  def apply(vector: VarBinaryVector): ColumnVector =
    new MilvusSparseMapColumn(vector)
}

private final class MilvusSparseMapColumn(vector: VarBinaryVector)
    extends ColumnVector(MilvusSparseVectorColumn.sparkType) {

  private val buffer = vector.getDataBuffer
  private val keys = new MilvusSparseEntryColumn(buffer, LongType)
  private val values = new MilvusSparseEntryColumn(buffer, FloatType)

  override def close(): Unit = {
    keys.close()
    values.close()
  }

  override def hasNull: Boolean = vector.getNullCount > 0

  override def numNulls: Int = vector.getNullCount

  override def isNullAt(rowId: Int): Boolean = vector.isNull(rowId)

  override def getMap(rowId: Int): ColumnarMap = {
    val start = vector.getStartOffset(rowId)
    val end = vector.getEndOffset(rowId)
    val width = end - start
    if (width % 8 != 0) {
      // Eight bytes an entry is the format. A row that is not a multiple of it
      // is corrupt, and reading it as floor(width / 8) entries would hand back
      // a vector that looks plausible and is wrong.
      throw new IllegalStateException(
        s"sparse vector at row $rowId is $width bytes, not a multiple of 8"
      )
    }
    new ColumnarMap(keys, values, start / 8, width / 8)
  }

  override def getChild(ordinal: Int): ColumnVector =
    if (ordinal == 0) keys else values

  override def getBoolean(rowId: Int): Boolean = unsupported("boolean")
  override def getByte(rowId: Int): Byte = unsupported("byte")
  override def getShort(rowId: Int): Short = unsupported("short")
  override def getInt(rowId: Int): Int = unsupported("int")
  override def getLong(rowId: Int): Long = unsupported("long")
  override def getFloat(rowId: Int): Float = unsupported("float")
  override def getDouble(rowId: Int): Double = unsupported("double")
  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    unsupported("decimal")
  override def getUTF8String(rowId: Int): UTF8String = unsupported("string")
  override def getBinary(rowId: Int): Array[Byte] = unsupported("binary")
  override def getArray(rowId: Int): ColumnarArray = unsupported("array")

  private def unsupported(what: String): Nothing =
    throw new UnsupportedOperationException(
      s"a sparse vector column is a map, not a $what"
    )
}

/** One half of every entry in the column: the indices, or the values.
  *
  * Entry `e` occupies bytes `[e * 8, e * 8 + 8)`; the index is the first four,
  * the value the last four, both little endian.
  */
private final class MilvusSparseEntryColumn(
    buffer: org.apache.arrow.memory.ArrowBuf,
    part: org.apache.spark.sql.types.DataType
) extends ColumnVector(part) {

  override def close(): Unit = ()

  // An entry inside a present vector is never null; the map column answers for
  // the row.
  override def hasNull: Boolean = false
  override def numNulls: Int = 0
  override def isNullAt(index: Int): Boolean = false

  /** The stored index, a uint32, widened so the top bit does not become a
    * negative Long.
    */
  override def getLong(index: Int): Long =
    if (part == LongType) buffer.getInt(index.toLong * 8) & 0xffffffffL
    else unsupported("long")

  override def getFloat(index: Int): Float =
    if (part == FloatType) buffer.getFloat(index.toLong * 8 + 4)
    else unsupported("float")

  override def getChild(ordinal: Int): ColumnVector =
    throw new UnsupportedOperationException("a sparse entry has no children")

  override def getBoolean(index: Int): Boolean = unsupported("boolean")
  override def getByte(index: Int): Byte = unsupported("byte")
  override def getShort(index: Int): Short = unsupported("short")
  override def getInt(index: Int): Int = unsupported("int")
  override def getDouble(index: Int): Double = unsupported("double")
  override def getDecimal(index: Int, precision: Int, scale: Int): Decimal =
    unsupported("decimal")
  override def getUTF8String(index: Int): UTF8String = unsupported("string")
  override def getBinary(index: Int): Array[Byte] = unsupported("binary")
  override def getArray(index: Int): ColumnarArray = unsupported("array")
  override def getMap(index: Int): ColumnarMap = unsupported("map")

  private def unsupported(what: String): Nothing =
    throw new UnsupportedOperationException(
      s"a sparse vector $part entry is not a $what"
    )
}
