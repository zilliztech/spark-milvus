package com.zilliz.spark.connector.write

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.write.codec.{
  ArrayEncoder,
  SparseFloatVectorEncoder
}
import com.zilliz.spark.connector.types.ArrowValueTypes.{
  isBinaryBackedVector,
  milvusDataType,
  requireSparseSparkTypes
}
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Encode Spark InternalRows into Arrow batches for Milvus writers. */
object ArrowWriter {
  private val DenseVectorTypes: Set[MilvusDataType] = Set(
    MilvusDataType.FloatVector,
    MilvusDataType.BinaryVector,
    MilvusDataType.Float16Vector,
    MilvusDataType.BFloat16Vector,
    MilvusDataType.Int8Vector
  )

  private val ArrowVectorDimensionMetadataKey = "dim"

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
    SparseFloatVectorEncoder.encodeSparseFloatVectorEntries(entries)
  }

  private def requireNonNullVectorArrayElement(
      arrayData: ArrayData,
      index: Int,
      vector: FieldVector,
      sparkType: String
  ): Unit = {
    if (arrayData.isNullAt(index)) {
      throw new IllegalArgumentException(
        s"Cannot encode $sparkType for vector '${vector.getName}': null element at index $index"
      )
    }
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
                s"Cannot encode $sparkType for nullable dense vector '${vector.getName}' ($dataType): missing $FieldMetadata.MilvusVectorDimensionMetadataKey or Arrow $ArrowVectorDimensionMetadataKey metadata"
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
      None,
      WriteLimits.Unchecked
    )

  /** The write schema's facts about a column that the value checks need, from
    * its field metadata (SparkTypes.metadata): the column name for the error,
    * and an Array's element type.
    */
  private final case class WriteLimits(
      column: String,
      elementType: Option[MilvusDataType]
  )

  private object WriteLimits {
    val Unchecked: WriteLimits = WriteLimits("", None)

    def of(field: StructField): WriteLimits =
      WriteLimits(
        column = field.name,
        elementType = Option(field.metadata)
          .filter(_.contains(FieldMetadata.MilvusElementTypeMetadataKey))
          .map(m =>
            MilvusDataType.fromValue(
              m.getLong(FieldMetadata.MilvusElementTypeMetadataKey).toInt
            )
          )
      )
  }

  private def sparkValueToArrowValue(
      vector: FieldVector,
      rowIndex: Int,
      record: InternalRow,
      colIndex: Int,
      sparkType: DataType,
      milvusType: Option[MilvusDataType],
      vectorDimension: Option[Int],
      limits: WriteLimits
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

      case ByteType =>
        vector
          .asInstanceOf[TinyIntVector]
          .set(rowIndex, record.getByte(colIndex))

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
          vector match {
            case chars: VarCharVector =>
              chars.setSafe(rowIndex, str.getBytes)
            // JSON: Milvus stores the text as Binary (serde.go byteEntry).
            case binary: VarBinaryVector =>
              binary.setSafe(rowIndex, str.getBytes)
            case other =>
              throw new IllegalArgumentException(
                s"Cannot write a string into ${other.getClass.getSimpleName}"
              )
          }
        }

      // A Milvus Array: one serialized ScalarField per row, the value Milvus
      // itself writes (payload_writer.go AddOneArrayToPayload). Matched before
      // the binary-backed vector cases, which share the VarBinary layout. The
      // collection's element type comes from the write schema: Int8, Int16
      // and Int32 elements all arrive as Spark integers of one width.
      case ArrayType(elementType, _)
          if milvusType.contains(MilvusDataType.Array) =>
        val milvusElementType = limits.elementType.getOrElse(
          throw new IllegalArgumentException(
            s"Column '${limits.column}' is a Milvus Array, but the write schema carries no element type"
          )
        )
        val binary = vector match {
          case b: VarBinaryVector => b
          case other =>
            throw new IllegalArgumentException(
              s"a Milvus Array is written as Binary, not into ${other.getClass.getSimpleName}"
            )
        }
        val arrayData = record.getArray(colIndex)
        val elements = (0 until arrayData.numElements()).map { i =>
          if (arrayData.isNullAt(i)) {
            throw new IllegalArgumentException(
              s"a Milvus Array cannot hold a null element (index $i)"
            )
          }
          elementType match {
            case BooleanType => arrayData.getBoolean(i)
            case ByteType    => arrayData.getByte(i).toInt
            case ShortType   => arrayData.getShort(i).toInt
            case IntegerType => arrayData.getInt(i)
            case LongType    => arrayData.getLong(i)
            case FloatType   => arrayData.getFloat(i)
            case DoubleType  => arrayData.getDouble(i)
            case StringType  => arrayData.getUTF8String(i).toString
            case other =>
              throw new IllegalArgumentException(
                s"a Milvus Array cannot have elements of Spark type $other"
              )
          }
        }
        val encoded =
          try
            ArrayEncoder.encode(milvusElementType, elements)
          catch {
            case e: IllegalArgumentException =>
              throw new IllegalArgumentException(
                s"Column '${limits.column}': ${e.getMessage}",
                e
              )
          }
        binary.setSafe(rowIndex, encoded)

      case ArrayType(FloatType, _) if isBinaryBackedVector(vector) =>
        val arrayData = record.getArray(colIndex)
        val floats = (0 until arrayData.numElements()).map { i =>
          requireNonNullVectorArrayElement(
            arrayData,
            i,
            vector,
            "Array[Float]"
          )
          arrayData.getFloat(i)
        }.toArray
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
        val values = (0 until arrayData.numElements()).map { i =>
          requireNonNullVectorArrayElement(
            arrayData,
            i,
            vector,
            "Array[Short]"
          )
          arrayData.getShort(i)
        }.toArray
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
              s"Binary-backed Array[Short] requires ${FieldMetadata.MilvusDataTypeMetadataKey} metadata for Int8Vector"
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
        val bytes = (0 until arrayData.numElements()).map { i =>
          requireNonNullVectorArrayElement(
            arrayData,
            i,
            vector,
            "Array[Byte]"
          )
          arrayData.getByte(i)
        }.toArray
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
              s"Binary-backed Array[Byte] requires ${FieldMetadata.MilvusDataTypeMetadataKey} metadata for BinaryVector"
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
              None,
              WriteLimits.Unchecked
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
          s"Binary-backed MapType requires ${FieldMetadata.MilvusDataTypeMetadataKey} metadata for SparseFloatVector"
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
            None,
            WriteLimits.Unchecked
          )
          sparkValueToArrowValue(
            structVector.getChild("value"),
            elemIndex,
            valueRow,
            0,
            valueType,
            None,
            None,
            WriteLimits.Unchecked
          )
        }

        mapVector.endValue(rowIndex, mapData.numElements())

      // A value that cannot be written must fail the task: logging and
      // moving on commits a null in its place.
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported Spark type for writing: $sparkType into ${vector.getClass.getSimpleName}"
        )
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
        milvusVectorDimension(field, vector),
        WriteLimits.of(field)
      )
    }
  }

  private def milvusVectorDimension(
      field: StructField,
      vector: FieldVector
  ): Option[Int] = {
    val sparkDimension = Option(field.metadata)
      .filter(_.contains(FieldMetadata.MilvusVectorDimensionMetadataKey))
      .map(_.getLong(FieldMetadata.MilvusVectorDimensionMetadataKey))

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
