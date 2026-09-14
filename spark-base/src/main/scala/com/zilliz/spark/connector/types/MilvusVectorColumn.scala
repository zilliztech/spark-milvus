package com.zilliz.spark.connector.types

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.{FieldVector, FixedSizeBinaryVector, VarBinaryVector}
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
  * Milvus stores a vector as one blob per row, in Arrow `FixedSizeBinary` when
  * the field is not nullable and `VarBinary` when it is — a nullable field's
  * null row then carries no payload, which is `SchemaMapper.physicalTypeOf`.
  * Spark's `ArrowColumnVector` has an accessor for neither shape as a vector,
  * so passing the Arrow vector through is not an option and never was; the
  * column has to be presented as something Spark's type system has. Which one
  * is decision 6:
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

  /** Wraps one Arrow vector column, in either of the two layouts a vector is
    * stored in.
    *
    * @param dimension
    *   elements per row, which the element width and the blob width together
    *   have to agree with — a mismatch means the schema and the data disagree
    *   about the column, and reading on would hand back silently wrong vectors.
    */
  def apply(
      vector: FieldVector,
      milvusType: MilvusDataType,
      dimension: Int,
      raw: Boolean
  ): ColumnVector = {
    val blobs = blobsOf(vector, milvusType)
    if (raw || milvusType == BinaryVector) {
      new MilvusBinaryColumn(blobs)
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
      // A fixed-width column declares one width for every row, so the
      // disagreement shows up here. A variable-width column can only be checked
      // row by row, which MilvusVectorArrayColumn does as each row is read.
      blobs.declaredWidth.foreach { width =>
        if (expected != width) {
          throw new IllegalArgumentException(
            s"schema says $milvusType of dimension $dimension, which is " +
              s"$expected bytes a row, but the column stores $width"
          )
        }
      }
      new MilvusVectorArrayColumn(
        blobs,
        sparkType(milvusType, raw = false),
        new MilvusVectorElementColumn(blobs, milvusType, elementWidth, dimension),
        dimension,
        expected,
        milvusType
      )
    }
  }

  /** The layout `vector` is in.
    *
    * `SchemaMapper` picks FixedSizeBinary for a dense vector and VarBinary for
    * a nullable one; a sparse vector is always VarBinary. Anything else means
    * the Arrow schema and the Milvus schema disagree about the column.
    */
  private def blobsOf(
      vector: FieldVector,
      milvusType: MilvusDataType
  ): VectorBlobs = vector match {
    case v: FixedSizeBinaryVector => new FixedWidthBlobs(v)
    case v: VarBinaryVector       => new VariableWidthBlobs(v)
    case other =>
      throw new IllegalArgumentException(
        s"a $milvusType column is stored as FixedSizeBinary or VarBinary, " +
          s"but this batch holds it as ${other.getClass.getSimpleName}"
      )
  }
}

/** One vector blob per row, whichever Arrow type holds it.
  *
  * Everything above reads a row by its byte offset and width, and both layouts
  * answer that. It exists so the column classes below are written once instead
  * of once per layout.
  */
private trait VectorBlobs {
  def isNull(rowId: Int): Boolean
  def nullCount: Int
  def buffer: ArrowBuf

  /** Byte offset of row `rowId`'s blob within [[buffer]]. */
  def startOf(rowId: Int): Long

  /** Width in bytes of row `rowId`'s blob. */
  def widthOf(rowId: Int): Int

  /** The width every row has, when the layout fixes one. */
  def declaredWidth: Option[Int]

  /** Row `rowId`'s bytes, copied out. */
  def bytes(rowId: Int): Array[Byte]
}

private final class FixedWidthBlobs(vector: FixedSizeBinaryVector)
    extends VectorBlobs {
  private val width = vector.getByteWidth
  override def isNull(rowId: Int): Boolean = vector.isNull(rowId)
  override def nullCount: Int = vector.getNullCount
  override def buffer: ArrowBuf = vector.getDataBuffer
  override def startOf(rowId: Int): Long = rowId.toLong * width
  override def widthOf(rowId: Int): Int = width
  override def declaredWidth: Option[Int] = Some(width)
  override def bytes(rowId: Int): Array[Byte] = vector.get(rowId)
}

private final class VariableWidthBlobs(vector: VarBinaryVector)
    extends VectorBlobs {
  override def isNull(rowId: Int): Boolean = vector.isNull(rowId)
  override def nullCount: Int = vector.getNullCount
  override def buffer: ArrowBuf = vector.getDataBuffer
  override def startOf(rowId: Int): Long = vector.getStartOffset(rowId).toLong
  override def widthOf(rowId: Int): Int =
    vector.getEndOffset(rowId) - vector.getStartOffset(rowId)
  override def declaredWidth: Option[Int] = None
  override def bytes(rowId: Int): Array[Byte] = vector.get(rowId)
}

/** A vector column seen as an array column: row `i` is the window `[i *
  * dimension, i * dimension + dimension)` of the element column.
  */
private final class MilvusVectorArrayColumn(
    blobs: VectorBlobs,
    arrayType: DataType,
    elements: ColumnVector,
    dimension: Int,
    expectedWidth: Int,
    milvusType: MilvusDataType
) extends ColumnVector(arrayType) {

  override def close(): Unit = elements.close()

  override def hasNull: Boolean = blobs.nullCount > 0

  override def numNulls: Int = blobs.nullCount

  override def isNullAt(rowId: Int): Boolean = blobs.isNull(rowId)

  override def getArray(rowId: Int): ColumnarArray = {
    // A variable-width column can hold a row of any length, so the row the
    // caller is reading is the first place its width can be checked against the
    // dimension the schema declares. Reading on would hand back a vector made
    // partly of the next row.
    val width = blobs.widthOf(rowId)
    if (width != expectedWidth) {
      throw new IllegalArgumentException(
        s"schema says $milvusType of dimension $dimension, which is " +
          s"$expectedWidth bytes a row, but row $rowId stores $width"
      )
    }
    new ColumnarArray(elements, rowId * dimension, dimension)
  }

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
    blobs: VectorBlobs,
    milvusType: MilvusDataType,
    elementWidth: Int,
    dimension: Int
) extends ColumnVector(
      if (milvusType == Int8Vector) ShortType else FloatType
    ) {

  private val buffer = blobs.buffer

  /** Where element `index` sits in the buffer.
    *
    * Index `index` is element `index % dimension` of row `index / dimension`,
    * and a row starts wherever its layout says — `rowId * width` when the width
    * is fixed, the offset buffer's entry when it is not.
    */
  private def byteOffsetOf(index: Int): Long =
    blobs.startOf(index / dimension) +
      (index % dimension).toLong * elementWidth

  override def close(): Unit = ()

  // Nulls are a property of the row, and the array column answers for those. An
  // element inside a present vector is never null.
  override def hasNull: Boolean = false
  override def numNulls: Int = 0
  override def isNullAt(index: Int): Boolean = false

  override def getShort(index: Int): Short =
    if (milvusType == Int8Vector) buffer.getByte(byteOffsetOf(index)).toShort
    else unsupported("short")

  override def getFloat(index: Int): Float = {
    val at = byteOffsetOf(index)
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
private final class MilvusBinaryColumn(blobs: VectorBlobs)
    extends ColumnVector(BinaryType) {

  override def close(): Unit = ()

  override def hasNull: Boolean = blobs.nullCount > 0

  override def numNulls: Int = blobs.nullCount

  override def isNullAt(rowId: Int): Boolean = blobs.isNull(rowId)

  override def getBinary(rowId: Int): Array[Byte] = blobs.bytes(rowId)

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
