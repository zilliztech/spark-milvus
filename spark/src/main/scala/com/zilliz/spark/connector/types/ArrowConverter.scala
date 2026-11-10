package com.zilliz.spark.connector.types

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.{
  CharacterCodingException,
  CodingErrorAction,
  StandardCharsets
}

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.milvus.storage.codec.{
  FloatConverter,
  SparseFloatVectorConverter
}
import com.zilliz.milvus.storage.codec.ArrayCodec
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.spark.connector.types.ArrowValueTypes.{
  isBinaryBackedVector,
  milvusDataType,
  requireSparseSparkTypes
}
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Decode Arrow values as independently owned Spark InternalRows. */
object ArrowConverter {

  /** Convert an Arrow VectorSchemaRoot row to an independently owned Spark
    * InternalRow whose values remain valid after the batch is reused or closed.
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
        throw new IllegalStateException(
          s"the Arrow batch has no column '$arrowColumnName' for Spark field " +
            s"'${field.name}'; it carries ${root.getSchema.getFields}"
        )
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

      // A Milvus Array is known from the field's type, not from its layout:
      // it is VarBinary like a nullable dense vector, so it has to be matched
      // before the vector cases.
      case ArrayType(elementType, _)
          if milvusType.contains(MilvusDataType.Array) =>
        vector match {
          case binary: VarBinaryVector =>
            decodeStoredArray(binary.get(rowIndex), elementType)
          case list: ListVector =>
            decodeListArray(list, rowIndex, elementType)
          case other =>
            throw new IllegalArgumentException(
              s"a Milvus Array is stored as Binary or List, not ${other.getClass.getSimpleName}"
            )
        }

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
              s"Binary-backed byte vectors require ${FieldMetadata.MilvusDataTypeMetadataKey} metadata for BinaryVector"
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
              s"Binary-backed short vectors require ${FieldMetadata.MilvusDataTypeMetadataKey} metadata"
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
        vector match {
          // A Milvus Array field: one serialized ScalarField per row.
          case binary: VarBinaryVector =>
            decodeStoredArray(binary.get(rowIndex), elementType)
          case _ => decodeListArray(vector, rowIndex, elementType)
        }

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
                  s"FixedSizeBinary vectors require ${FieldMetadata.MilvusDataTypeMetadataKey} metadata"
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
              s"Binary-backed maps require ${FieldMetadata.MilvusDataTypeMetadataKey} metadata for SparseFloatVector"
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
        throw new IllegalArgumentException(
          s"Unsupported Spark type for Arrow conversion: $sparkType"
        )
    }
  }

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

  /** The elements of a stored Array value, as the Spark element type. */
  private[types] def decodeStoredArray(
      bytes: Array[Byte],
      elementType: DataType
  ): ArrayData = {
    val elements = ArrayCodec.elements(bytes)
    val converted: Array[Any] = elementType match {
      case BooleanType => elements.map(_.asInstanceOf[Boolean]).toArray
      case ByteType    => elements.map(_.asInstanceOf[Int].toByte).toArray
      case ShortType   => elements.map(_.asInstanceOf[Int].toShort).toArray
      case IntegerType => elements.map(_.asInstanceOf[Int]).toArray
      case LongType    => elements.map(_.asInstanceOf[Long]).toArray
      case FloatType   => elements.map(_.asInstanceOf[Float]).toArray
      case DoubleType  => elements.map(_.asInstanceOf[Double]).toArray
      case StringType =>
        elements.map(e => UTF8String.fromString(e.asInstanceOf[String])).toArray
      case other =>
        throw new IllegalArgumentException(
          s"a Milvus Array cannot have elements of Spark type $other"
        )
    }
    ArrayData.toArrayData(converted)
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
          s"Binary-backed float vectors require ${FieldMetadata.MilvusDataTypeMetadataKey} metadata"
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
}
