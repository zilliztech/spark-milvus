package com.zilliz.spark.connector.types

import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}

import org.apache.arrow.vector.VarBinaryVector
import org.apache.spark.sql.types.{Decimal, StringType}
import org.apache.spark.sql.vectorized.{
  ColumnVector,
  ColumnarArray,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

/** Presents an Arrow `VarBinary` column as the string Spark's schema says it
  * is.
  *
  * A Milvus JSON field is declared `StringType` by `SparkTypes` and stored as
  * Arrow `Binary` by `ArrowTypes`, so the declared type and the physical type
  * differ. Spark's own `ArrowColumnVector` picks its accessor from the physical
  * type and its binary accessor answers `getBinary` only, so a `StringType`
  * column read through it throws on the first `getUTF8String`.
  *
  * The bytes are validated as UTF-8 on the way out, the same as
  * `ArrowConverter.utf8StringFromVariableWidth` does on the row path, so the
  * two paths reject the same values.
  */
final class Utf8FromBinaryColumn(vector: VarBinaryVector)
    extends ColumnVector(StringType) {

  override def close(): Unit = ()

  override def hasNull: Boolean = vector.getNullCount > 0

  override def numNulls: Int = vector.getNullCount

  override def isNullAt(rowId: Int): Boolean = vector.isNull(rowId)

  override def getUTF8String(rowId: Int): UTF8String = {
    val bytes = vector.get(rowId)
    try {
      StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
    } catch {
      case e: CharacterCodingException =>
        throw new IllegalArgumentException(
          s"Arrow VarBinary value in column ${vector.getName} at row " +
            s"$rowId is not valid UTF-8",
          e
        )
    }
    UTF8String.fromBytes(bytes)
  }

  override def getBinary(rowId: Int): Array[Byte] = vector.get(rowId)

  override def getChild(ordinal: Int): ColumnVector =
    throw new UnsupportedOperationException("a string column has no children")

  override def getBoolean(rowId: Int): Boolean = unsupported("boolean")
  override def getByte(rowId: Int): Byte = unsupported("byte")
  override def getShort(rowId: Int): Short = unsupported("short")
  override def getInt(rowId: Int): Int = unsupported("int")
  override def getLong(rowId: Int): Long = unsupported("long")
  override def getFloat(rowId: Int): Float = unsupported("float")
  override def getDouble(rowId: Int): Double = unsupported("double")
  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    unsupported("decimal")
  override def getArray(rowId: Int): ColumnarArray = unsupported("array")
  override def getMap(rowId: Int): ColumnarMap = unsupported("map")

  private def unsupported(what: String): Nothing =
    throw new UnsupportedOperationException(
      s"column ${vector.getName} is a string, not a $what"
    )
}
