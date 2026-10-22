package com.zilliz.spark.connector.types

import org.apache.arrow.vector.VarBinaryVector
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{
  ColumnVector,
  ColumnarArray,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.milvus.storage.codec.ArrayCodec

/** Presents a Milvus Array column, stored as one serialized `ScalarField` per
  * row in an Arrow `VarBinary` column, as the Spark array column the schema
  * declares.
  *
  * Spark's `ColumnarArray` needs its elements in a column vector, so the whole
  * batch is decoded once here into one element vector plus an offset per row;
  * that decode is the cost the stored encoding imposes, and it happens once per
  * batch rather than once per access.
  */
final class MilvusArrayColumn(vector: VarBinaryVector, elementType: DataType)
    extends ColumnVector(ArrayType(elementType)) {

  private val rows = vector.getValueCount
  private val offsets = new Array[Int](rows)
  private val lengths = new Array[Int](rows)
  private val elements: OnHeapColumnVector = {
    val decoded = new Array[Seq[Any]](rows)
    var total = 0
    var i = 0
    while (i < rows) {
      if (!vector.isNull(i)) {
        decoded(i) = ArrayCodec.elements(vector.get(i))
        offsets(i) = total
        lengths(i) = decoded(i).size
        total += lengths(i)
      }
      i += 1
    }
    val out = new OnHeapColumnVector(math.max(total, 1), elementType)
    var at = 0
    i = 0
    while (i < rows) {
      if (decoded(i) != null) {
        decoded(i).foreach { e =>
          put(out, at, e)
          at += 1
        }
      }
      i += 1
    }
    out
  }

  private def put(out: OnHeapColumnVector, at: Int, e: Any): Unit =
    elementType match {
      case BooleanType => out.putBoolean(at, e.asInstanceOf[Boolean])
      case ByteType    => out.putByte(at, e.asInstanceOf[Int].toByte)
      case ShortType   => out.putShort(at, e.asInstanceOf[Int].toShort)
      case IntegerType => out.putInt(at, e.asInstanceOf[Int])
      case LongType    => out.putLong(at, e.asInstanceOf[Long])
      case FloatType   => out.putFloat(at, e.asInstanceOf[Float])
      case DoubleType  => out.putDouble(at, e.asInstanceOf[Double])
      case StringType =>
        val bytes = UTF8String.fromString(e.asInstanceOf[String]).getBytes
        out.putByteArray(at, bytes)
      case other =>
        throw new IllegalArgumentException(
          s"a Milvus Array cannot have elements of Spark type $other"
        )
    }

  override def close(): Unit = elements.close()
  override def hasNull: Boolean = vector.getNullCount > 0
  override def numNulls: Int = vector.getNullCount
  override def isNullAt(rowId: Int): Boolean = vector.isNull(rowId)
  override def getArray(rowId: Int): ColumnarArray =
    new ColumnarArray(elements, offsets(rowId), lengths(rowId))
  override def getChild(ordinal: Int): ColumnVector = elements
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
  override def getMap(rowId: Int): ColumnarMap = unsupported("map")
  private def unsupported(what: String): Nothing =
    throw new UnsupportedOperationException(
      s"column ${vector.getName} is an array, not a $what"
    )
}
