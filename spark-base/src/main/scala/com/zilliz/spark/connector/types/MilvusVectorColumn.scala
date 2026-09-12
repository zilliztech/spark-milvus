package com.zilliz.spark.connector.types

import org.apache.arrow.vector.FixedSizeBinaryVector
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  DataType,
  Decimal,
  FloatType,
  ShortType
}
import org.apache.spark.sql.vectorized.{
  ColumnVector,
  ColumnarArray,
  ColumnarMap
}
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.milvus.storage.codec.FloatConverter
import io.milvus.grpc.schema.{DataType => MilvusDataType}
import io.milvus.grpc.schema.DataType.{
  BFloat16Vector,
  BinaryVector,
  Float16Vector,
  FloatVector,
  Int8Vector
}

/** Presents a Milvus vector column to Spark.
  *
  * Milvus stores every vector type as Arrow `FixedSizeBinary` — one fixed-width
  * blob per row — and Spark's `ArrowColumnVector` has no accessor for that
  * type. So passing the Arrow vector through is not an option and never was;
  * the column has to be presented as something Spark's type system has. Which
  * one is decision 6:
  *
  *   - `FloatVector`, `Float16Vector`, `BFloat16Vector` become
  *     `ArrayType(FloatType)`
  *   - `Int8Vector` becomes `ArrayType(ShortType)`
  *   - `BinaryVector` stays `BinaryType`, and so does everything else when the
  *     read asks for raw bytes
  *
  * Nothing is copied up front. `getArray` hands back a window onto
  * [[MilvusVectorElementColumn]], which decodes one element at a time straight
  * out of the Arrow buffer, so a half-precision column costs no more memory
  * than it does on disk until something downstream materializes it.
  */
object MilvusVectorColumn {

  /** The Spark type a Milvus vector column is presented as.
    *
    * @param raw
    *   the `milvus.read.vector.raw` option: hand over the stored bytes instead
    *   of decoding them.
    */
  def sparkType(milvusType: MilvusDataType, raw: Boolean): DataType =
    if (raw) BinaryType
    else
      milvusType match {
        case FloatVector | Float16Vector | BFloat16Vector =>
          ArrayType(FloatType, containsNull = false)
        case Int8Vector   => ArrayType(ShortType, containsNull = false)
        case BinaryVector => BinaryType
        case other =>
          throw new IllegalArgumentException(
            s"$other is not a vector type this column can present"
          )
      }

  /** Wraps one Arrow vector column.
    *
    * @param dimension
    *   elements per row, which the element width and the blob width together
    *   have to agree with — a mismatch means the schema and the data disagree
    *   about the column, and reading on would hand back silently wrong vectors.
    */
  def apply(
      vector: FixedSizeBinaryVector,
      milvusType: MilvusDataType,
      dimension: Int,
      raw: Boolean
  ): ColumnVector = {
    val blobWidth = vector.getByteWidth
    if (raw || milvusType == BinaryVector) {
      new MilvusBinaryColumn(vector)
    } else {
      val elementWidth = milvusType match {
        case FloatVector                    => 4
        case Float16Vector | BFloat16Vector => 2
        case Int8Vector                     => 1
        case other =>
          throw new IllegalArgumentException(
            s"$other is not a vector type this column can present"
          )
      }
      val expected = dimension * elementWidth
      if (expected != blobWidth) {
        throw new IllegalArgumentException(
          s"schema says $milvusType of dimension $dimension, which is " +
            s"$expected bytes a row, but the column stores $blobWidth"
        )
      }
      new MilvusVectorArrayColumn(
        vector,
        sparkType(milvusType, raw = false),
        new MilvusVectorElementColumn(vector, milvusType, elementWidth),
        dimension
      )
    }
  }
}

/** A vector column seen as an array column: row `i` is the window `[i *
  * dimension, i * dimension + dimension)` of the element column.
  */
private final class MilvusVectorArrayColumn(
    vector: FixedSizeBinaryVector,
    arrayType: DataType,
    elements: ColumnVector,
    dimension: Int
) extends ColumnVector(arrayType) {

  override def close(): Unit = elements.close()

  override def hasNull: Boolean = vector.getNullCount > 0

  override def numNulls: Int = vector.getNullCount

  override def isNullAt(rowId: Int): Boolean = vector.isNull(rowId)

  override def getArray(rowId: Int): ColumnarArray =
    new ColumnarArray(elements, rowId * dimension, dimension)

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
      s"a vector column is an array, not a $what"
    )
}

/** The elements of every vector in the column, laid end to end.
  *
  * Index `i` is element `i % dimension` of row `i / dimension`, which is where
  * it already sits in the Arrow buffer, so nothing is moved. Half precision is
  * widened here, one element per call, rather than by materializing the column.
  */
private final class MilvusVectorElementColumn(
    vector: FixedSizeBinaryVector,
    milvusType: MilvusDataType,
    elementWidth: Int
) extends ColumnVector(
      if (milvusType == Int8Vector) ShortType else FloatType
    ) {

  private val buffer = vector.getDataBuffer

  override def close(): Unit = ()

  // Nulls are a property of the row, and the array column answers for those. An
  // element inside a present vector is never null.
  override def hasNull: Boolean = false
  override def numNulls: Int = 0
  override def isNullAt(index: Int): Boolean = false

  override def getShort(index: Int): Short =
    if (milvusType == Int8Vector) buffer.getByte(index.toLong).toShort
    else unsupported("short")

  override def getFloat(index: Int): Float = {
    val at = index.toLong * elementWidth
    milvusType match {
      case FloatVector => buffer.getFloat(at)
      case Float16Vector =>
        FloatConverter.float16BitsToFloat(littleEndianShort(at))
      case BFloat16Vector =>
        FloatConverter.bfloat16BitsToFloat(littleEndianShort(at))
      case _ => unsupported("float")
    }
  }

  /** The two bytes at `at`, low byte first, which is how Milvus writes them. */
  private def littleEndianShort(at: Long): Int =
    ((buffer.getByte(at + 1) & 0xff) << 8) | (buffer.getByte(at) & 0xff)

  override def getChild(ordinal: Int): ColumnVector =
    throw new UnsupportedOperationException("a vector element has no children")

  override def getBoolean(index: Int): Boolean = unsupported("boolean")
  override def getByte(index: Int): Byte = unsupported("byte")
  override def getInt(index: Int): Int = unsupported("int")
  override def getLong(index: Int): Long = unsupported("long")
  override def getDouble(index: Int): Double = unsupported("double")
  override def getDecimal(index: Int, precision: Int, scale: Int): Decimal =
    unsupported("decimal")
  override def getUTF8String(index: Int): UTF8String = unsupported("string")
  override def getBinary(index: Int): Array[Byte] = unsupported("binary")
  override def getArray(index: Int): ColumnarArray = unsupported("array")
  override def getMap(index: Int): ColumnarMap = unsupported("map")

  private def unsupported(what: String): Nothing =
    throw new UnsupportedOperationException(
      s"a $milvusType element is not a $what"
    )
}

/** A vector column handed over as the bytes Milvus stored.
  *
  * What `milvus.read.vector.raw` asks for, and the only shape `BinaryVector`
  * has ever had. Each row is copied out on read, the same as the row path does.
  */
private final class MilvusBinaryColumn(vector: FixedSizeBinaryVector)
    extends ColumnVector(BinaryType) {

  override def close(): Unit = ()

  override def hasNull: Boolean = vector.getNullCount > 0

  override def numNulls: Int = vector.getNullCount

  override def isNullAt(rowId: Int): Boolean = vector.isNull(rowId)

  override def getBinary(rowId: Int): Array[Byte] = vector.get(rowId)

  override def getChild(ordinal: Int): ColumnVector =
    throw new UnsupportedOperationException("a binary column has no children")

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
  override def getArray(rowId: Int): ColumnarArray = unsupported("array")
  override def getMap(rowId: Int): ColumnarMap = unsupported("map")

  private def unsupported(what: String): Nothing =
    throw new UnsupportedOperationException(
      s"a raw vector column is binary, not a $what"
    )
}
