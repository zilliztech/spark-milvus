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
   * Convert an Arrow VectorSchemaRoot row to Spark InternalRow
   *
   * @param root Arrow VectorSchemaRoot containing the data
   * @param rowIndex Index of the row to convert
   * @param sparkSchema Spark schema for the target InternalRow
   * @param fieldNameMapping Optional mapping from Spark field name to Arrow column name
   *                         (e.g., "id" -> "100" when Arrow uses field IDs as column names)
   * @return Spark InternalRow
   */
  def arrowToInternalRow(
      root: VectorSchemaRoot,
      rowIndex: Int,
      sparkSchema: StructType,
      fieldNameMapping: Map[String, String] = Map.empty
  ): InternalRow = {
    val values = new Array[Any](sparkSchema.fields.length)

    sparkSchema.fields.zipWithIndex.foreach { case (field, index) =>
      // Use mapping if provided, otherwise use the field name directly
      val arrowColumnName = fieldNameMapping.getOrElse(field.name, field.name)
      val vector = root.getVector(arrowColumnName)

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

      case FloatType =>
        vector.asInstanceOf[Float4Vector].get(rowIndex)

      case DoubleType =>
        vector.asInstanceOf[Float8Vector].get(rowIndex)

      case BooleanType =>
        vector.asInstanceOf[BitVector].get(rowIndex) != 0

      case StringType =>
        val bytes = vector.asInstanceOf[VarCharVector].get(rowIndex)
        UTF8String.fromBytes(bytes)

      case ArrayType(FloatType, _) =>
        // FloatVector stored as FixedSizeBinaryVector
        val bytes = vector.asInstanceOf[FixedSizeBinaryVector].get(rowIndex)
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        val floats = (0 until (bytes.length / 4)).map(_ => buffer.getFloat()).toArray
        ArrayData.toArrayData(floats)

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
        val bytes = vector.asInstanceOf[VarBinaryVector].get(rowIndex)
        bytes

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

      case FloatType =>
        vector.asInstanceOf[Float4Vector].set(rowIndex, record.getFloat(colIndex))

      case DoubleType =>
        vector.asInstanceOf[Float8Vector].set(rowIndex, record.getDouble(colIndex))

      case BooleanType =>
        vector.asInstanceOf[BitVector].set(rowIndex, if (record.getBoolean(colIndex)) 1 else 0)

      case StringType =>
        val str = record.getUTF8String(colIndex)
        if (str == null) {
          vector.setNull(rowIndex)
        } else {
          vector.asInstanceOf[VarCharVector].set(rowIndex, str.getBytes)
        }

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
        if (bytes == null) {
          vector.setNull(rowIndex)
        } else {
          vector.asInstanceOf[VarBinaryVector].set(rowIndex, bytes)
        }

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
      val vector = root.getVector(colIndex)
      sparkValueToArrowValue(vector, rowIndex, record, colIndex, field.dataType)
    }
  }
}
