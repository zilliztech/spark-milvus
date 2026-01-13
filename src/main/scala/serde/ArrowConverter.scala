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
   * Create a field name → Milvus DataType lookup map from CollectionSchema
   * This enables accurate type detection during Arrow ↔ Spark conversion
   */
  private def createFieldTypeMap(milvusSchema: io.milvus.grpc.schema.CollectionSchema): Map[String, io.milvus.grpc.schema.DataType] = {
    milvusSchema.fields.map(field => field.name -> field.dataType).toMap
  }

  /**
   * Convert an Arrow VectorSchemaRoot row to Spark InternalRow
   *
   * @param root Arrow VectorSchemaRoot containing the data
   * @param rowIndex Index of the row to convert
   * @param sparkSchema Spark schema for the target InternalRow
   * @param milvusSchemaOpt Optional Milvus CollectionSchema for accurate type detection
   * @return Spark InternalRow
   */
  def arrowToInternalRow(
      root: VectorSchemaRoot,
      rowIndex: Int,
      sparkSchema: StructType,
      milvusSchemaOpt: Option[io.milvus.grpc.schema.CollectionSchema] = None
  ): InternalRow = {
    // Create field type map for lookups (only if schema provided)
    val fieldTypeMap = milvusSchemaOpt.map(createFieldTypeMap).getOrElse(Map.empty)

    val values = new Array[Any](sparkSchema.fields.length)

    sparkSchema.fields.zipWithIndex.foreach { case (field, index) =>
      val vector = root.getVector(field.name)

      if (vector == null) {
        values(index) = null
      } else if (vector.isNull(rowIndex)) {
        values(index) = null
      } else {
        values(index) = arrowValueToSparkValue(vector, rowIndex, field.dataType, field.name, fieldTypeMap)
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
   * @param fieldName Name of the field for schema lookup
   * @param fieldTypeMap Map of field name to Milvus DataType for accurate type detection
   * @return Spark value
   */
  def arrowValueToSparkValue(
      vector: FieldVector,
      rowIndex: Int,
      sparkType: DataType,
      fieldName: String,
      fieldTypeMap: Map[String, io.milvus.grpc.schema.DataType]
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
        val bytes = vector.asInstanceOf[VarCharVector].get(rowIndex)
        UTF8String.fromBytes(bytes)

      case ArrayType(FloatType, _) =>
        // Detect actual vector type from Milvus schema
        fieldTypeMap.get(fieldName) match {
          case Some(io.milvus.grpc.schema.DataType.FloatVector) =>
            // FloatVector: 4 bytes per element
            val bytes = vector.asInstanceOf[FixedSizeBinaryVector].get(rowIndex)
            val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
            val floats = (0 until (bytes.length / 4)).map(_ => buffer.getFloat()).toArray
            ArrayData.toArrayData(floats)

          case Some(io.milvus.grpc.schema.DataType.Float16Vector) =>
            // Float16Vector: 2 bytes per element, convert to float
            val bytes = vector.asInstanceOf[FixedSizeBinaryVector].get(rowIndex)
            val dim = bytes.length / 2
            val floats = new Array[Float](dim)
            for (i <- 0 until dim) {
              val float16Bytes = bytes.slice(i * 2, i * 2 + 2)
              floats(i) = com.zilliz.spark.connector.FloatConverter.fromFloat16Bytes(float16Bytes.toSeq)
            }
            ArrayData.toArrayData(floats)

          case Some(io.milvus.grpc.schema.DataType.BFloat16Vector) =>
            // BFloat16Vector: 2 bytes per element, convert to float
            val bytes = vector.asInstanceOf[FixedSizeBinaryVector].get(rowIndex)
            val dim = bytes.length / 2
            val floats = new Array[Float](dim)
            for (i <- 0 until dim) {
              val bfloat16Bytes = bytes.slice(i * 2, i * 2 + 2)
              floats(i) = com.zilliz.spark.connector.FloatConverter.fromBFloat16Bytes(bfloat16Bytes.toSeq)
            }
            ArrayData.toArrayData(floats)

          case _ =>
            // Fallback: assume FloatVector for backward compatibility
            logWarning(s"No Milvus schema available for field '$fieldName', assuming FloatVector")
            val bytes = vector.asInstanceOf[FixedSizeBinaryVector].get(rowIndex)
            val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
            val floats = (0 until (bytes.length / 4)).map(_ => buffer.getFloat()).toArray
            ArrayData.toArrayData(floats)
        }

      case ArrayType(ByteType, _) =>
        // BinaryVector or byte array - detect based on Milvus schema and vector type
        fieldTypeMap.get(fieldName) match {
          case Some(io.milvus.grpc.schema.DataType.BinaryVector) =>
            // BinaryVector: stored as FixedSizeBinary (bit-packed)
            val bytes = vector.asInstanceOf[FixedSizeBinaryVector].get(rowIndex)
            ArrayData.toArrayData(bytes)

          case _ =>
            // Generic byte array: stored as ListVector
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
                arrowValueToSparkValue(dataVector, elemIndex, ByteType, fieldName, fieldTypeMap)
              }
            }.toArray

            ArrayData.toArrayData(arrayElements)
        }

      case ArrayType(ShortType, _) =>
        // Int8Vector or short array - detect based on Milvus schema
        fieldTypeMap.get(fieldName) match {
          case Some(io.milvus.grpc.schema.DataType.Int8Vector) =>
            // Int8Vector: stored as FixedSizeBinary (1 byte per element), expose as Array[Short]
            val bytes = vector.asInstanceOf[FixedSizeBinaryVector].get(rowIndex)
            val shorts = bytes.map(_.toShort)
            ArrayData.toArrayData(shorts)

          case _ =>
            // Generic short array: stored as ListVector
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
                arrowValueToSparkValue(dataVector, elemIndex, ShortType, fieldName, fieldTypeMap)
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
            arrowValueToSparkValue(dataVector, elemIndex, elementType, fieldName, fieldTypeMap)
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
          keys(i) = arrowValueToSparkValue(dataVector.getChild("key"), elemIndex, keyType, fieldName, fieldTypeMap)
          values(i) = arrowValueToSparkValue(dataVector.getChild("value"), elemIndex, valueType, fieldName, fieldTypeMap)
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
   * @param fieldName Name of the field for schema lookup
   * @param fieldTypeMap Map of field name to Milvus DataType for accurate type detection
   */
  def sparkValueToArrowValue(
      vector: FieldVector,
      rowIndex: Int,
      record: InternalRow,
      colIndex: Int,
      sparkType: DataType,
      fieldName: String,
      fieldTypeMap: Map[String, io.milvus.grpc.schema.DataType]
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
        if (str == null) {
          vector.setNull(rowIndex)
        } else {
          vector.asInstanceOf[VarCharVector].set(rowIndex, str.getBytes)
        }

      case ArrayType(FloatType, _) =>
        val arrayData = record.getArray(colIndex)
        val floats = (0 until arrayData.numElements()).map(i => arrayData.getFloat(i)).toArray

        fieldTypeMap.get(fieldName) match {
          case Some(io.milvus.grpc.schema.DataType.FloatVector) =>
            // FloatVector: 4 bytes per element
            val bytes = new Array[Byte](floats.length * 4)
            val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
            floats.foreach(buffer.putFloat)
            vector.asInstanceOf[FixedSizeBinaryVector].set(rowIndex, bytes)

          case Some(io.milvus.grpc.schema.DataType.Float16Vector) =>
            // Float16Vector: 2 bytes per element
            val bytes = floats.flatMap { f =>
              com.zilliz.spark.connector.FloatConverter.toFloat16Bytes(f)
            }.toArray
            vector.asInstanceOf[FixedSizeBinaryVector].set(rowIndex, bytes)

          case Some(io.milvus.grpc.schema.DataType.BFloat16Vector) =>
            // BFloat16Vector: 2 bytes per element
            val bytes = floats.flatMap { f =>
              com.zilliz.spark.connector.FloatConverter.toBFloat16Bytes(f)
            }.toArray
            vector.asInstanceOf[FixedSizeBinaryVector].set(rowIndex, bytes)

          case _ =>
            // Fallback: assume FloatVector
            logWarning(s"No Milvus schema available for field '$fieldName', assuming FloatVector")
            val bytes = new Array[Byte](floats.length * 4)
            val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
            floats.foreach(buffer.putFloat)
            vector.asInstanceOf[FixedSizeBinaryVector].set(rowIndex, bytes)
        }

      case ArrayType(ByteType, _) =>
        // BinaryVector or byte array - detect based on Milvus schema
        val arrayData = record.getArray(colIndex)

        fieldTypeMap.get(fieldName) match {
          case Some(io.milvus.grpc.schema.DataType.BinaryVector) =>
            // BinaryVector: stored as FixedSizeBinary (bit-packed)
            val bytes = (0 until arrayData.numElements()).map(i => arrayData.getByte(i)).toArray
            vector.asInstanceOf[FixedSizeBinaryVector].set(rowIndex, bytes)

          case _ =>
            // Generic byte array: stored as ListVector
            val listVector = vector.asInstanceOf[ListVector]
            val dataVector = listVector.getDataVector

            listVector.startNewValue(rowIndex)
            val startIndex = listVector.getOffsetBuffer.getInt(rowIndex * 4)

            (0 until arrayData.numElements()).foreach { i =>
              val elemIndex = startIndex + i
              if (arrayData.isNullAt(i)) {
                dataVector.setNull(elemIndex)
              } else {
                val elemRow = InternalRow(arrayData.getByte(i))
                sparkValueToArrowValue(dataVector, elemIndex, elemRow, 0, ByteType, fieldName, fieldTypeMap)
              }
            }

            listVector.endValue(rowIndex, arrayData.numElements())
        }

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
            sparkValueToArrowValue(dataVector, elemIndex, elemRow, 0, elementType, fieldName, fieldTypeMap)
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

          sparkValueToArrowValue(structVector.getChild("key"), elemIndex, keyRow, 0, keyType, fieldName, fieldTypeMap)
          sparkValueToArrowValue(structVector.getChild("value"), elemIndex, valueRow, 0, valueType, fieldName, fieldTypeMap)
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
   * @param milvusSchemaOpt Optional Milvus CollectionSchema for accurate type detection
   */
  def internalRowToArrow(
      root: VectorSchemaRoot,
      rowIndex: Int,
      record: InternalRow,
      sparkSchema: StructType,
      milvusSchemaOpt: Option[io.milvus.grpc.schema.CollectionSchema] = None
  ): Unit = {
    val fieldTypeMap = milvusSchemaOpt.map(createFieldTypeMap).getOrElse(Map.empty)

    sparkSchema.fields.zipWithIndex.foreach { case (field, colIndex) =>
      val vector = root.getVector(field.name)
      sparkValueToArrowValue(vector, rowIndex, record, colIndex, field.dataType, field.name, fieldTypeMap)
    }
  }
}
