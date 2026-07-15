package com.zilliz.spark.connector.serde

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.{
  CharacterCodingException,
  CodingErrorAction,
  StandardCharsets
}
import scala.collection.JavaConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{
  ArrayBasedMapData,
  ArrayData,
  MapData
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.spark.connector.{FloatConverter, SparseFloatVectorConverter}
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Utilities for converting between Spark InternalRow and Arrow vectors
  */
object ArrowConverter extends Logging {

  private val DenseVectorTypes: Set[MilvusDataType] = Set(
    MilvusDataType.FloatVector,
    MilvusDataType.BinaryVector,
    MilvusDataType.Float16Vector,
    MilvusDataType.BFloat16Vector,
    MilvusDataType.Int8Vector
  )

  private val ArrowVectorDimensionMetadataKey = "dim"

  /** Convert an Arrow VectorSchemaRoot row to Spark InternalRow
    *
    * @param root
    *   Arrow VectorSchemaRoot containing the data
    * @param rowIndex
    *   Index of the row to convert
    * @param sparkSchema
    *   Spark schema for the target InternalRow
    * @param fieldNameMapping
    *   Optional mapping from Spark field name to Arrow column name (e.g., "id"
    *   -> "100" when Arrow uses field IDs as column names)
    * @return
    *   Spark InternalRow
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
        values(index) = arrowValueToSparkValue(
          vector,
          rowIndex,
          field.dataType,
          milvusDataType(field)
        )
      }
    }

    InternalRow.fromSeq(values)
  }

  /** Convert a value from an Arrow vector to a Spark value
    *
    * @param vector
    *   Arrow FieldVector containing the value
    * @param rowIndex
    *   Index of the row to extract
    * @param sparkType
    *   Target Spark data type
    * @return
    *   Spark value
    */
  def arrowValueToSparkValue(
      vector: FieldVector,
      rowIndex: Int,
      sparkType: DataType
  ): Any = {
    arrowValueToSparkValue(
      vector,
      rowIndex,
      sparkType,
      None,
      allowLegacyFloatVector = true
    )
  }

  private def arrowValueToSparkValue(
      vector: FieldVector,
      rowIndex: Int,
      sparkType: DataType,
      milvusType: Option[MilvusDataType],
      allowLegacyFloatVector: Boolean = false
  ): Any = {
    sparkType match {
      case LongType =>
        vector.asInstanceOf[BigIntVector].get(rowIndex)

      case IntegerType =>
        vector.asInstanceOf[IntVector].get(rowIndex)

      case ByteType =>
        vector.asInstanceOf[TinyIntVector].get(rowIndex)

      case ShortType =>
        vector.asInstanceOf[SmallIntVector].get(rowIndex)

      case FloatType =>
        vector.asInstanceOf[Float4Vector].get(rowIndex)

      case DoubleType =>
        vector.asInstanceOf[Float8Vector].get(rowIndex)

      case BooleanType =>
        vector.asInstanceOf[BitVector].get(rowIndex) != 0

      case StringType =>
        utf8StringFromVariableWidth(vector, rowIndex)

      case ArrayType(ByteType, _) if isBinaryBackedVector(vector) =>
        val bytes = binaryVectorBytes(vector, rowIndex)
        milvusType match {
          case Some(MilvusDataType.BinaryVector) =>
            ArrayData.toArrayData(bytes)
          case Some(other) =>
            throw new IllegalArgumentException(
              s"Cannot decode binary-backed vector as Array[Byte] for Milvus type $other"
            )
          case None =>
            throw new IllegalArgumentException(
              s"Binary-backed byte vectors require ${MilvusDataTypeMetadataKey} metadata for BinaryVector"
            )
        }

      case ArrayType(ShortType, _) if isBinaryBackedVector(vector) =>
        val bytes = binaryVectorBytes(vector, rowIndex)
        milvusType match {
          case Some(MilvusDataType.Int8Vector) =>
            ArrayData.toArrayData(bytes.map(_.toShort))
          case Some(other) =>
            throw new IllegalArgumentException(
              s"Cannot decode binary-backed vector as Array[Short] for Milvus type $other"
            )
          case None =>
            throw new IllegalArgumentException(
              s"Binary-backed short vectors require ${MilvusDataTypeMetadataKey} metadata"
            )
        }

      case ArrayType(FloatType, _) if isBinaryBackedVector(vector) =>
        val bytes = binaryVectorBytes(vector, rowIndex)
        ArrayData.toArrayData(
          decodeBinaryFloats(
            bytes,
            milvusType,
            allowLegacyFloatVector
          )
        )

      case ArrayType(elementType, _) =>
        decodeListArray(vector, rowIndex, elementType)

      case BinaryType =>
        vector match {
          case v: VarBinaryVector =>
            v.get(rowIndex)
          case v: FixedSizeBinaryVector =>
            milvusType match {
              case Some(
                    MilvusDataType.BinaryVector | MilvusDataType.FloatVector |
                    MilvusDataType.Float16Vector |
                    MilvusDataType.BFloat16Vector | MilvusDataType.Int8Vector
                  ) =>
                v.get(rowIndex)
              case Some(other) =>
                throw new IllegalArgumentException(
                  s"Cannot decode FixedSizeBinary as BinaryType for Milvus type $other"
                )
              case None =>
                throw new IllegalArgumentException(
                  s"FixedSizeBinary vectors require ${MilvusDataTypeMetadataKey} metadata"
                )
            }
          case other =>
            throw new IllegalArgumentException(
              s"Cannot decode ${other.getClass.getSimpleName} as BinaryType"
            )
        }

      case MapType(keyType, valueType, _) if isBinaryBackedVector(vector) =>
        milvusType match {
          case Some(MilvusDataType.SparseFloatVector) =>
            requireSparseSparkTypes(keyType, valueType)
            decodeSparseMap(binaryVectorBytes(vector, rowIndex))
          case Some(other) =>
            throw new IllegalArgumentException(
              s"Cannot decode binary-backed vector as MapType for Milvus type $other"
            )
          case None =>
            throw new IllegalArgumentException(
              s"Binary-backed maps require ${MilvusDataTypeMetadataKey} metadata for SparseFloatVector"
            )
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
          keys(i) = arrowValueToSparkValue(
            dataVector.getChild("key"),
            elemIndex,
            keyType,
            None
          )
          values(i) = arrowValueToSparkValue(
            dataVector.getChild("value"),
            elemIndex,
            valueType,
            None
          )
        }

        ArrayBasedMapData(keys, values)

      case _ =>
        logWarning(s"Unsupported Spark type: $sparkType, returning null")
        null
    }
  }

  private def milvusDataType(field: StructField): Option[MilvusDataType] = {
    Option(field.metadata)
      .filter(_.contains(MilvusDataTypeMetadataKey))
      .map(_.getLong(MilvusDataTypeMetadataKey).toInt)
      .map(MilvusDataType.fromValue)
  }

  private def isBinaryBackedVector(vector: FieldVector): Boolean =
    vector.isInstanceOf[FixedSizeBinaryVector] ||
      vector.isInstanceOf[VarBinaryVector]

  private def binaryVectorBytes(
      vector: FieldVector,
      rowIndex: Int
  ): Array[Byte] = vector match {
    case fixed: FixedSizeBinaryVector => fixed.get(rowIndex)
    case variable: VarBinaryVector    => variable.get(rowIndex)
    case other =>
      throw new IllegalArgumentException(
        s"Cannot read ${other.getClass.getSimpleName} as a binary-backed vector"
      )
  }

  private def decodeListArray(
      vector: FieldVector,
      rowIndex: Int,
      elementType: DataType
  ): ArrayData = {
    val listVector = vector match {
      case list: ListVector => list
      case other =>
        throw new IllegalArgumentException(
          s"Cannot decode ${other.getClass.getSimpleName} as Array[$elementType]"
        )
    }
    val dataVector = listVector.getDataVector
    val startIndex = listVector.getElementStartIndex(rowIndex)
    val endIndex = listVector.getElementEndIndex(rowIndex)
    val length = endIndex - startIndex

    val arrayElements = (0 until length).map { i =>
      val elemIndex = startIndex + i
      if (dataVector.isNull(elemIndex)) {
        null
      } else {
        arrowValueToSparkValue(dataVector, elemIndex, elementType, None)
      }
    }.toArray

    ArrayData.toArrayData(arrayElements)
  }

  private def requireSparseSparkTypes(
      keyType: DataType,
      valueType: DataType
  ): Unit = {
    if (keyType != LongType || valueType != FloatType) {
      throw new IllegalArgumentException(
        s"SparseFloatVector requires MapType(LongType, FloatType), got MapType($keyType, $valueType)"
      )
    }
  }

  private def decodeSparseMap(bytes: Array[Byte]): ArrayBasedMapData = {
    val entries = SparseFloatVectorConverter.decodeSparseFloatVector(bytes)
    val keys = new Array[Any](entries.size)
    val values = new Array[Any](entries.size)
    entries.zipWithIndex.foreach { case ((index, value), entryIndex) =>
      keys(entryIndex) = index
      values(entryIndex) = value
    }
    ArrayBasedMapData(keys, values)
  }

  private def encodeSparseMap(
      mapData: MapData,
      keyType: DataType,
      valueType: DataType
  ): Array[Byte] = {
    requireSparseSparkTypes(keyType, valueType)
    val keys = mapData.keyArray()
    val values = mapData.valueArray()
    val entries = (0 until mapData.numElements()).map { index =>
      if (keys.isNullAt(index) || values.isNullAt(index)) {
        throw new IllegalArgumentException(
          "SparseFloatVector map entries cannot contain null keys or values"
        )
      }
      keys.getLong(index) -> values.getFloat(index)
    }
    SparseFloatVectorConverter.encodeSparseFloatVectorEntries(entries)
  }

  private def utf8StringFromVariableWidth(
      vector: FieldVector,
      rowIndex: Int
  ): UTF8String = {
    val bytes = vector match {
      case v: VarCharVector =>
        v.get(rowIndex)
      case v: VarBinaryVector =>
        val bytes = v.get(rowIndex)
        try {
          StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes))
        } catch {
          case e: CharacterCodingException =>
            throw new IllegalArgumentException(
              s"Arrow VarBinary value in column ${vector.getName} at row $rowIndex is not valid UTF-8",
              e
            )
        }
        bytes
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot decode ${vector.getClass.getSimpleName} as StringType"
        )
    }
    UTF8String.fromBytes(bytes)
  }

  private def decodeBinaryFloats(
      bytes: Array[Byte],
      milvusType: Option[MilvusDataType],
      allowLegacyFloatVector: Boolean
  ): Array[Float] = {
    milvusType match {
      case Some(MilvusDataType.Float16Vector) =>
        bytes
          .grouped(2)
          .map(b => FloatConverter.fromFloat16Bytes(b.toSeq))
          .toArray
      case Some(MilvusDataType.BFloat16Vector) =>
        bytes
          .grouped(2)
          .map(b => FloatConverter.fromBFloat16Bytes(b.toSeq))
          .toArray
      case Some(MilvusDataType.FloatVector) =>
        decodeFloatVectorBytes(bytes)
      case None if allowLegacyFloatVector =>
        decodeFloatVectorBytes(bytes)
      case None =>
        throw new IllegalArgumentException(
          s"Binary-backed float vectors require ${MilvusDataTypeMetadataKey} metadata"
        )
      case Some(other) =>
        throw new IllegalArgumentException(
          s"Cannot decode binary-backed vector as Array[Float] for Milvus type $other"
        )
    }
  }

  private def decodeFloatVectorBytes(bytes: Array[Byte]): Array[Float] = {
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    (0 until (bytes.length / 4)).map(_ => buffer.getFloat()).toArray
  }

  private def encodeFloatVectorBytes(values: Array[Float]): Array[Byte] = {
    val bytes = new Array[Byte](values.length * 4)
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    values.foreach(buffer.putFloat)
    bytes
  }

  private def setBinaryVectorValue(
      vector: FieldVector,
      rowIndex: Int,
      bytes: Array[Byte],
      sparkType: String,
      milvusType: Option[MilvusDataType],
      vectorDimension: Option[Int]
  ): Unit = {
    validateDenseVectorByteWidth(
      vector,
      bytes,
      sparkType,
      milvusType,
      vectorDimension
    )

    vector match {
      case fixed: FixedSizeBinaryVector => fixed.set(rowIndex, bytes)
      case variable: VarBinaryVector    =>
        // setSafe grows the variable-width data buffer when needed.
        variable.setSafe(rowIndex, bytes)
      case other =>
        throw new IllegalArgumentException(
          s"Cannot encode $sparkType into ${other.getClass.getSimpleName}"
        )
    }
  }

  private def validateDenseVectorByteWidth(
      vector: FieldVector,
      bytes: Array[Byte],
      sparkType: String,
      milvusType: Option[MilvusDataType],
      vectorDimension: Option[Int]
  ): Unit =
    milvusType.filter(DenseVectorTypes.contains).foreach { dataType =>
      val expectedBytes = vectorDimension match {
        case Some(dimension) => denseVectorByteWidth(dataType, dimension)
        case None =>
          vector match {
            case fixed: FixedSizeBinaryVector => fixed.getByteWidth
            case _: VarBinaryVector =>
              throw new IllegalArgumentException(
                s"Cannot encode $sparkType for nullable dense vector '${vector.getName}' ($dataType): missing $MilvusVectorDimensionMetadataKey or Arrow $ArrowVectorDimensionMetadataKey metadata"
              )
            case other =>
              throw new IllegalArgumentException(
                s"Cannot determine dense vector width from ${other.getClass.getSimpleName}"
              )
          }
      }

      if (bytes.length != expectedBytes) {
        throw new IllegalArgumentException(
          s"Cannot encode $sparkType for dense vector '${vector.getName}' ($dataType): expected $expectedBytes bytes, got ${bytes.length}"
        )
      }
    }

  private def denseVectorByteWidth(
      dataType: MilvusDataType,
      dimension: Int
  ): Int = {
    if (dimension <= 0) {
      throw new IllegalArgumentException(
        s"Dense vector dimension must be positive, got $dimension for $dataType"
      )
    }

    dataType match {
      case MilvusDataType.FloatVector => Math.multiplyExact(dimension, 4)
      case MilvusDataType.Float16Vector | MilvusDataType.BFloat16Vector =>
        Math.multiplyExact(dimension, 2)
      case MilvusDataType.Int8Vector => dimension
      case MilvusDataType.BinaryVector =>
        if (dimension % 8 != 0) {
          throw new IllegalArgumentException(
            s"BinaryVector dimension must be a multiple of 8, got $dimension"
          )
        }
        dimension / 8
      case other =>
        throw new IllegalArgumentException(
          s"Cannot compute dense vector byte width for $other"
        )
    }
  }

  val MilvusDataTypeMetadataKey = "milvus.data_type"
  val MilvusVectorDimensionMetadataKey = "milvus.vector_dim"

  /** Set a value in an Arrow vector from a Spark InternalRow
    *
    * @param vector
    *   Arrow FieldVector to write to
    * @param rowIndex
    *   Index of the row to write
    * @param record
    *   Spark InternalRow containing the data
    * @param colIndex
    *   Column index in the InternalRow
    * @param sparkType
    *   Spark data type of the column
    */
  def sparkValueToArrowValue(
      vector: FieldVector,
      rowIndex: Int,
      record: InternalRow,
      colIndex: Int,
      sparkType: DataType
  ): Unit =
    sparkValueToArrowValue(
      vector,
      rowIndex,
      record,
      colIndex,
      sparkType,
      None,
      None
    )

  private def sparkValueToArrowValue(
      vector: FieldVector,
      rowIndex: Int,
      record: InternalRow,
      colIndex: Int,
      sparkType: DataType,
      milvusType: Option[MilvusDataType],
      vectorDimension: Option[Int]
  ): Unit = {
    if (record.isNullAt(colIndex)) {
      vector.setNull(rowIndex)
      return
    }

    sparkType match {
      case LongType =>
        vector
          .asInstanceOf[BigIntVector]
          .set(rowIndex, record.getLong(colIndex))

      case IntegerType =>
        vector.asInstanceOf[IntVector].set(rowIndex, record.getInt(colIndex))

      case ShortType =>
        vector
          .asInstanceOf[SmallIntVector]
          .set(rowIndex, record.getShort(colIndex))

      case FloatType =>
        vector
          .asInstanceOf[Float4Vector]
          .set(rowIndex, record.getFloat(colIndex))

      case DoubleType =>
        vector
          .asInstanceOf[Float8Vector]
          .set(rowIndex, record.getDouble(colIndex))

      case BooleanType =>
        vector
          .asInstanceOf[BitVector]
          .set(rowIndex, if (record.getBoolean(colIndex)) 1 else 0)

      case StringType =>
        val str = record.getUTF8String(colIndex)
        if (str == null) {
          vector.setNull(rowIndex)
        } else {
          // setSafe grows the data buffer when actual bytes exceed the
          // density-based initial capacity. `set` throws IndexOutOfBounds
          // instead of reallocating, which blows up whenever strings average
          // more than the configured density (32 bytes).
          vector.asInstanceOf[VarCharVector].setSafe(rowIndex, str.getBytes)
        }

      case ArrayType(FloatType, _) if isBinaryBackedVector(vector) =>
        val arrayData = record.getArray(colIndex)
        val floats = (0 until arrayData.numElements())
          .map(i => arrayData.getFloat(i))
          .toArray
        val bytes = milvusType match {
          case Some(MilvusDataType.Float16Vector) =>
            floats.flatMap(value => FloatConverter.toFloat16Bytes(value))
          case Some(MilvusDataType.BFloat16Vector) =>
            floats.flatMap(value => FloatConverter.toBFloat16Bytes(value))
          case Some(MilvusDataType.FloatVector) | None =>
            encodeFloatVectorBytes(floats)
          case Some(other) =>
            throw new IllegalArgumentException(
              s"Cannot encode Array[Float] as binary-backed Milvus type $other"
            )
        }
        setBinaryVectorValue(
          vector,
          rowIndex,
          bytes,
          "Array[Float]",
          milvusType,
          vectorDimension
        )

      case ArrayType(ShortType, _) if isBinaryBackedVector(vector) =>
        val arrayData = record.getArray(colIndex)
        val values = (0 until arrayData.numElements())
          .map(i => arrayData.getShort(i))
          .toArray
        val bytes = milvusType match {
          case Some(MilvusDataType.Int8Vector) =>
            values.map { value =>
              if (value < Byte.MinValue || value > Byte.MaxValue) {
                throw new IllegalArgumentException(
                  s"Int8Vector value $value is outside [${Byte.MinValue}, ${Byte.MaxValue}]"
                )
              }
              value.toByte
            }
          case Some(other) =>
            throw new IllegalArgumentException(
              s"Cannot encode Array[Short] as binary-backed Milvus type $other"
            )
          case None =>
            throw new IllegalArgumentException(
              s"Binary-backed Array[Short] requires ${MilvusDataTypeMetadataKey} metadata for Int8Vector"
            )
        }
        setBinaryVectorValue(
          vector,
          rowIndex,
          bytes,
          "Array[Short]",
          milvusType,
          vectorDimension
        )

      case ArrayType(ByteType, _) if isBinaryBackedVector(vector) =>
        val arrayData = record.getArray(colIndex)
        val bytes = (0 until arrayData.numElements())
          .map(i => arrayData.getByte(i))
          .toArray
        milvusType match {
          case Some(MilvusDataType.BinaryVector) =>
            setBinaryVectorValue(
              vector,
              rowIndex,
              bytes,
              "Array[Byte]",
              milvusType,
              vectorDimension
            )
          case Some(other) =>
            throw new IllegalArgumentException(
              s"Cannot encode Array[Byte] as binary-backed Milvus type $other"
            )
          case None =>
            throw new IllegalArgumentException(
              s"Binary-backed Array[Byte] requires ${MilvusDataTypeMetadataKey} metadata for BinaryVector"
            )
        }

      case ArrayType(elementType, _) =>
        // Generic array handling
        val listVector = vector match {
          case list: ListVector => list
          case other =>
            throw new IllegalArgumentException(
              s"Cannot encode Array[$elementType] into ${other.getClass.getSimpleName}"
            )
        }
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
            sparkValueToArrowValue(
              dataVector,
              elemIndex,
              elemRow,
              0,
              elementType,
              None,
              None
            )
          }
        }

        listVector.endValue(rowIndex, arrayData.numElements())

      case BinaryType =>
        val bytes = record.getBinary(colIndex)
        if (bytes == null) {
          vector.setNull(rowIndex)
        } else {
          setBinaryVectorValue(
            vector,
            rowIndex,
            bytes,
            "BinaryType",
            milvusType,
            vectorDimension
          )
        }

      case MapType(keyType, valueType, _)
          if milvusType.contains(MilvusDataType.SparseFloatVector) =>
        if (!isBinaryBackedVector(vector)) {
          throw new IllegalArgumentException(
            s"SparseFloatVector MapType requires a binary-backed Arrow vector, got ${vector.getClass.getSimpleName}"
          )
        }
        val bytes = encodeSparseMap(record.getMap(colIndex), keyType, valueType)
        setBinaryVectorValue(
          vector,
          rowIndex,
          bytes,
          "SparseFloatVector MapType",
          milvusType,
          vectorDimension
        )

      case MapType(_, _, _) if isBinaryBackedVector(vector) =>
        throw new IllegalArgumentException(
          s"Binary-backed MapType requires ${MilvusDataTypeMetadataKey} metadata for SparseFloatVector"
        )

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

          sparkValueToArrowValue(
            structVector.getChild("key"),
            elemIndex,
            keyRow,
            0,
            keyType,
            None,
            None
          )
          sparkValueToArrowValue(
            structVector.getChild("value"),
            elemIndex,
            valueRow,
            0,
            valueType,
            None,
            None
          )
        }

        mapVector.endValue(rowIndex, mapData.numElements())

      case _ =>
        logWarning(s"Unsupported Spark type for writing: $sparkType")
    }
  }

  /** Add a Spark InternalRow to an Arrow VectorSchemaRoot
    *
    * @param root
    *   Arrow VectorSchemaRoot to write to
    * @param rowIndex
    *   Index of the row to write
    * @param record
    *   Spark InternalRow to convert
    * @param sparkSchema
    *   Spark schema of the InternalRow
    */
  def internalRowToArrow(
      root: VectorSchemaRoot,
      rowIndex: Int,
      record: InternalRow,
      sparkSchema: StructType
  ): Unit = {
    sparkSchema.fields.zipWithIndex.foreach { case (field, colIndex) =>
      val vector = root.getVector(colIndex)
      sparkValueToArrowValue(
        vector,
        rowIndex,
        record,
        colIndex,
        field.dataType,
        milvusDataType(field),
        milvusVectorDimension(field, vector)
      )
    }
  }

  private def milvusVectorDimension(
      field: StructField,
      vector: FieldVector
  ): Option[Int] = {
    val sparkDimension = Option(field.metadata)
      .filter(_.contains(MilvusVectorDimensionMetadataKey))
      .map(_.getLong(MilvusVectorDimensionMetadataKey))

    val arrowDimension = Option(vector.getField)
      .flatMap(arrowField => Option(arrowField.getMetadata))
      .flatMap(metadata =>
        Option(metadata.get(ArrowVectorDimensionMetadataKey))
      )
      .map { rawDimension =>
        try rawDimension.toLong
        catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Invalid Arrow $ArrowVectorDimensionMetadataKey metadata '$rawDimension' for vector '${vector.getName}'"
            )
        }
      }

    sparkDimension.orElse(arrowDimension).map { dimension =>
      if (dimension <= 0 || dimension > Int.MaxValue) {
        throw new IllegalArgumentException(
          s"Invalid vector dimension $dimension for field '${field.name}'"
        )
      }
      dimension.toInt
    }
  }
}
