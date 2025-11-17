package com.zilliz.spark.connector.serde

import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.JavaConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Utilities for converting between Spark InternalRow and Arrow vectors
 */
object ArrowConverter extends Logging {

  /**
   * Convert Float16 or BFloat16 bytes to Float32 array
   * Note: Currently treats all 2-byte formats as IEEE 754 half-precision (Float16)
   * TODO: Add BFloat16 detection and conversion if needed
   *
   * @param bytes Byte array containing float16/bfloat16 data
   * @return Array of float32 values
   */
  private def convertFloat16OrBFloat16ToFloat32(bytes: Array[Byte]): Array[Float] = {
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    val numFloats = bytes.length / 2
    val floats = new Array[Float](numFloats)

    for (i <- 0 until numFloats) {
      val half = buffer.getShort() & 0xFFFF
      floats(i) = convertFloat16ToFloat32(half)
    }

    floats
  }

  /**
   * Convert a single Float16 (IEEE 754 half-precision) value to Float32
   *
   * @param half 16-bit half-precision float as Int
   * @return 32-bit single-precision float
   */
  private def convertFloat16ToFloat32(half: Int): Float = {
    // Extract sign, exponent, and mantissa
    val sign = (half >> 15) & 0x1
    val exponent = (half >> 10) & 0x1F
    val mantissa = half & 0x3FF

    // Handle special cases
    if (exponent == 0) {
      if (mantissa == 0) {
        // Zero (positive or negative)
        return if (sign == 1) -0.0f else 0.0f
      } else {
        // Subnormal number
        val result = Math.pow(2, -14).toFloat * (mantissa.toFloat / 1024.0f)
        return if (sign == 1) -result else result
      }
    } else if (exponent == 31) {
      if (mantissa == 0) {
        // Infinity
        return if (sign == 1) Float.NegativeInfinity else Float.PositiveInfinity
      } else {
        // NaN
        return Float.NaN
      }
    }

    // Normal number
    // Convert exponent from biased (15) to biased (127) for float32
    val float32Exponent = exponent - 15 + 127
    // Extend mantissa from 10 bits to 23 bits
    val float32Mantissa = mantissa << 13

    // Construct float32 bits: sign(1) + exponent(8) + mantissa(23)
    val float32Bits = (sign << 31) | (float32Exponent << 23) | float32Mantissa

    java.lang.Float.intBitsToFloat(float32Bits)
  }

  /**
   * Convert an Arrow VectorSchemaRoot row to Spark InternalRow
   *
   * @param root Arrow VectorSchemaRoot containing the data
   * @param rowIndex Index of the row to convert
   * @param sparkSchema Spark schema for the target InternalRow
   * @return Spark InternalRow
   */
  def arrowToInternalRow(
      root: VectorSchemaRoot,
      rowIndex: Int,
      sparkSchema: StructType
  ): InternalRow = {
    val values = new Array[Any](sparkSchema.fields.length)

    sparkSchema.fields.zipWithIndex.foreach { case (field, index) =>
      val vector = root.getVector(field.name)

      if (vector == null) {
        values(index) = null
      } else if (vector.isNull(rowIndex)) {
        values(index) = null
      } else {
        values(index) = arrowValueToSparkValue(vector, rowIndex, field.dataType)
      }
    }

    InternalRow.fromSeq(values)
  }

  /**
   * Convert a value from an Arrow vector to a Spark value
   *
   * @param vector Arrow FieldVector containing the value
   * @param rowIndex Index of the row to extract
   * @param sparkType Target Spark data type
   * @return Spark value
   */
  def arrowValueToSparkValue(
      vector: FieldVector,
      rowIndex: Int,
      sparkType: DataType
  ): Any = {
    sparkType match {
      case LongType =>
        vector.asInstanceOf[BigIntVector].get(rowIndex)

      case IntegerType =>
        vector.asInstanceOf[IntVector].get(rowIndex)

      case ShortType =>
        vector.asInstanceOf[SmallIntVector].get(rowIndex)

      case ByteType =>
        vector.asInstanceOf[TinyIntVector].get(rowIndex)

      case FloatType =>
        vector.asInstanceOf[Float4Vector].get(rowIndex)

      case DoubleType =>
        vector.asInstanceOf[Float8Vector].get(rowIndex)

      case BooleanType =>
        vector.asInstanceOf[BitVector].get(rowIndex) != 0

      case StringType =>
        vector match {
          case vc: VarCharVector =>
            // Regular string fields
            val bytes = vc.get(rowIndex)
            UTF8String.fromBytes(bytes)
          case vb: VarBinaryVector =>
            // JSON fields stored as binary
            val bytes = vb.get(rowIndex)
            UTF8String.fromBytes(bytes)
        }

      case ArrayType(FloatType, _) =>
        vector match {
          case fsb: FixedSizeBinaryVector =>
            // FloatVector, Float16Vector, or BFloat16Vector stored as FixedSizeBinaryVector
            val bytes = fsb.get(rowIndex)
            val byteWidth = fsb.getByteWidth

            // Heuristic to determine vector type:
            // - If byteWidth % 4 != 0: definitely Float16/BFloat16 (2 bytes per element)
            // - If byteWidth % 8 != 0 (but % 4 == 0): likely Float16 with even dimensions like 2, 6, 10
            //   because dim*2 = 4, 12, 20 (multiples of 4 but not 8)
            // - If byteWidth % 8 == 0: likely FloatVector (4 bytes per element)
            // Exception: very small vectors (byteWidth < 32) are more likely Float16

            val isFloat16 = if (byteWidth % 4 != 0) {
              true  // Can't be FloatVector
            } else if (byteWidth % 8 != 0) {
              true  // Likely Float16 with even dim (e.g., dim=2 -> 4 bytes)
            } else if (byteWidth < 32) {
              // Small vectors: try to distinguish by checking if values look like valid float32
              // For now, assume small multiples of 8 could be either
              false  // Default to FloatVector for byteWidth = 8, 16, 24
            } else {
              false  // Larger vectors, likely FloatVector
            }

            if (isFloat16) {
              // Float16Vector or BFloat16Vector: 2 bytes per float
              val floats = convertFloat16OrBFloat16ToFloat32(bytes)
              ArrayData.toArrayData(floats)
            } else {
              // FloatVector: 4 bytes per float
              val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
              val floats = (0 until (byteWidth / 4)).map(_ => buffer.getFloat()).toArray
              ArrayData.toArrayData(floats)
            }

          case listVector: ListVector =>
            // Generic array handling for non-vector types
            val dataVector = listVector.getDataVector
            val startIndex = listVector.getElementStartIndex(rowIndex)
            val endIndex = listVector.getElementEndIndex(rowIndex)
            val length = endIndex - startIndex

            val arrayElements = (0 until length).map { i =>
              val elemIndex = startIndex + i
              if (dataVector.isNull(elemIndex)) {
                null
              } else {
                arrowValueToSparkValue(dataVector, elemIndex, FloatType)
              }
            }.toArray

            ArrayData.toArrayData(arrayElements)
        }

      case ArrayType(ShortType, _) =>
        vector match {
          case fsb: FixedSizeBinaryVector =>
            // Int8Vector: stored as FixedSizeBinary with 1 byte per element
            // Convert bytes to Short array (Spark doesn't have ByteType arrays)
            val bytes = fsb.get(rowIndex)
            val shorts = bytes.map(b => b.toShort)
            ArrayData.toArrayData(shorts)

          case listVector: ListVector =>
            // Generic Short array handling
            val dataVector = listVector.getDataVector
            val startIndex = listVector.getElementStartIndex(rowIndex)
            val endIndex = listVector.getElementEndIndex(rowIndex)
            val length = endIndex - startIndex

            val arrayElements = (0 until length).map { i =>
              val elemIndex = startIndex + i
              if (dataVector.isNull(elemIndex)) {
                null
              } else {
                arrowValueToSparkValue(dataVector, elemIndex, ShortType)
              }
            }.toArray

            ArrayData.toArrayData(arrayElements)
        }

      case ArrayType(elementType, _) =>
        // Generic array handling
        val listVector = vector.asInstanceOf[ListVector]
        val dataVector = listVector.getDataVector
        val startIndex = listVector.getElementStartIndex(rowIndex)
        val endIndex = listVector.getElementEndIndex(rowIndex)
        val length = endIndex - startIndex

        val arrayElements = (0 until length).map { i =>
          val elemIndex = startIndex + i
          if (dataVector.isNull(elemIndex)) {
            null
          } else {
            arrowValueToSparkValue(dataVector, elemIndex, elementType)
          }
        }.toArray

        ArrayData.toArrayData(arrayElements)

      case BinaryType =>
        vector match {
          case vb: VarBinaryVector =>
            // Variable-length binary (JSON, Array, etc.)
            vb.get(rowIndex)
          case fsb: FixedSizeBinaryVector =>
            // Fixed-size binary (BinaryVector)
            fsb.get(rowIndex)
        }

      case MapType(keyType, valueType, _) =>
        val mapVector = vector.asInstanceOf[MapVector]
        val dataVector = mapVector.getDataVector.asInstanceOf[StructVector]
        val startIndex = mapVector.getElementStartIndex(rowIndex)
        val endIndex = mapVector.getElementEndIndex(rowIndex)
        val length = endIndex - startIndex

        val keys = new Array[Any](length)
        val values = new Array[Any](length)

        (0 until length).foreach { i =>
          val elemIndex = startIndex + i
          keys(i) = arrowValueToSparkValue(dataVector.getChild("key"), elemIndex, keyType)
          values(i) = arrowValueToSparkValue(dataVector.getChild("value"), elemIndex, valueType)
        }

        ArrayBasedMapData(keys, values)

      case _ =>
        logWarning(s"Unsupported Spark type: $sparkType, returning null")
        null
    }
  }

  /**
   * Set a value in an Arrow vector from a Spark InternalRow
   *
   * @param vector Arrow FieldVector to write to
   * @param rowIndex Index of the row to write
   * @param record Spark InternalRow containing the data
   * @param colIndex Column index in the InternalRow
   * @param sparkType Spark data type of the column
   */
  def sparkValueToArrowValue(
      vector: FieldVector,
      rowIndex: Int,
      record: InternalRow,
      colIndex: Int,
      sparkType: DataType
  ): Unit = {
    if (record.isNullAt(colIndex)) {
      vector.setNull(rowIndex)
      return
    }

    sparkType match {
      case LongType =>
        vector.asInstanceOf[BigIntVector].set(rowIndex, record.getLong(colIndex))

      case IntegerType =>
        vector.asInstanceOf[IntVector].set(rowIndex, record.getInt(colIndex))

      case ShortType =>
        vector.asInstanceOf[SmallIntVector].set(rowIndex, record.getShort(colIndex))

      case ByteType =>
        vector.asInstanceOf[TinyIntVector].set(rowIndex, record.getByte(colIndex))

      case FloatType =>
        vector.asInstanceOf[Float4Vector].set(rowIndex, record.getFloat(colIndex))

      case DoubleType =>
        vector.asInstanceOf[Float8Vector].set(rowIndex, record.getDouble(colIndex))

      case BooleanType =>
        vector.asInstanceOf[BitVector].set(rowIndex, if (record.getBoolean(colIndex)) 1 else 0)

      case StringType =>
        val str = record.getUTF8String(colIndex)
        vector.asInstanceOf[VarCharVector].set(rowIndex, str.getBytes)

      case ArrayType(FloatType, _) =>
        // FloatVector stored as FixedSizeBinaryVector
        val arrayData = record.getArray(colIndex)
        val floats = (0 until arrayData.numElements()).map(i => arrayData.getFloat(i)).toArray
        val bytes = new Array[Byte](floats.length * 4)
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        floats.foreach(buffer.putFloat)
        vector.asInstanceOf[FixedSizeBinaryVector].set(rowIndex, bytes)

      case ArrayType(elementType, _) =>
        // Generic array handling
        val listVector = vector.asInstanceOf[ListVector]
        val dataVector = listVector.getDataVector
        val arrayData = record.getArray(colIndex)

        listVector.startNewValue(rowIndex)
        val startIndex = listVector.getOffsetBuffer.getInt(rowIndex * 4)

        (0 until arrayData.numElements()).foreach { i =>
          val elemIndex = startIndex + i
          if (arrayData.isNullAt(i)) {
            dataVector.setNull(elemIndex)
          } else {
            // Recursively set element value
            val elemRow = InternalRow(arrayData.get(i, elementType))
            sparkValueToArrowValue(dataVector, elemIndex, elemRow, 0, elementType)
          }
        }

        listVector.endValue(rowIndex, arrayData.numElements())

      case BinaryType =>
        val bytes = record.getBinary(colIndex)
        vector.asInstanceOf[VarBinaryVector].set(rowIndex, bytes)

      case MapType(keyType, valueType, _) =>
        val mapVector = vector.asInstanceOf[MapVector]
        val structVector = mapVector.getDataVector.asInstanceOf[StructVector]
        val mapData = record.getMap(colIndex)

        mapVector.startNewValue(rowIndex)
        val startIndex = mapVector.getOffsetBuffer.getInt(rowIndex * 4)

        val keys = mapData.keyArray()
        val values = mapData.valueArray()

        (0 until mapData.numElements()).foreach { i =>
          val elemIndex = startIndex + i
          val keyRow = InternalRow(keys.get(i, keyType))
          val valueRow = InternalRow(values.get(i, valueType))

          sparkValueToArrowValue(structVector.getChild("key"), elemIndex, keyRow, 0, keyType)
          sparkValueToArrowValue(structVector.getChild("value"), elemIndex, valueRow, 0, valueType)
        }

        mapVector.endValue(rowIndex, mapData.numElements())

      case _ =>
        logWarning(s"Unsupported Spark type for writing: $sparkType")
    }
  }

  /**
   * Add a Spark InternalRow to an Arrow VectorSchemaRoot
   *
   * @param root Arrow VectorSchemaRoot to write to
   * @param rowIndex Index of the row to write
   * @param record Spark InternalRow to convert
   * @param sparkSchema Spark schema of the InternalRow
   */
  def internalRowToArrow(
      root: VectorSchemaRoot,
      rowIndex: Int,
      record: InternalRow,
      sparkSchema: StructType
  ): Unit = {
    sparkSchema.fields.zipWithIndex.foreach { case (field, colIndex) =>
      val vector = root.getVector(field.name)
      sparkValueToArrowValue(vector, rowIndex, record, colIndex, field.dataType)
    }
  }
}
