package com.zilliz.spark.connector

import java.nio.{ByteBuffer, ByteOrder}

import com.google.protobuf.ByteString

import com.zilliz.spark.connector.DataParseException
import io.milvus.grpc.schema.{
  ArrayArray,
  BoolArray,
  BytesArray,
  DataType,
  DoubleArray,
  FieldData,
  FieldSchema,
  FloatArray,
  GeometryArray,
  IntArray,
  JSONArray,
  LongArray,
  ScalarField,
  SparseFloatArray,
  StringArray,
  VectorField
}

object MilvusFieldData {
  def packBoolFieldData(
      fieldName: String,
      fieldValues: Seq[Boolean]
  ): FieldData = {
    FieldData(
      `type` = DataType.Bool,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.BoolData(BoolArray(data = fieldValues))
        )
      )
    )
  }

  def packInt8FieldData(
      fieldName: String,
      fieldValues: Seq[Short]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int8,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.IntData(IntArray(data = fieldValues.map(_.toInt)))
        )
      )
    )
  }

  def packInt16FieldData(
      fieldName: String,
      fieldValues: Seq[Short]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int16,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.IntData(IntArray(data = fieldValues.map(_.toInt)))
        )
      )
    )
  }

  def packInt32FieldData(
      fieldName: String,
      fieldValues: Seq[Int]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int32,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(
          data = ScalarField.Data.IntData(
            IntArray(data = fieldValues)
          )
        )
      )
    )
  }

  def packInt64FieldData(
      fieldName: String,
      fieldValues: Seq[Long]
  ): FieldData = {
    FieldData(
      `type` = DataType.Int64,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.LongData(LongArray(data = fieldValues))
        )
      )
    )
  }

  def packFloatFieldData(
      fieldName: String,
      fieldValues: Seq[Float]
  ): FieldData = {
    FieldData(
      `type` = DataType.Float,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.FloatData(FloatArray(data = fieldValues))
        )
      )
    )
  }

  def packDoubleFieldData(
      fieldName: String,
      fieldValues: Seq[Double]
  ): FieldData = {
    FieldData(
      `type` = DataType.Double,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.DoubleData(DoubleArray(data = fieldValues))
        )
      )
    )
  }

  def packStringFieldData(
      fieldName: String,
      fieldValues: Seq[String]
  ): FieldData = {
    FieldData(
      `type` = DataType.VarChar,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.StringData(StringArray(data = fieldValues))
        )
      )
    )
  }

  def packArrayFieldData(
      fieldName: String,
      fieldValues: Seq[ScalarField],
      elementType: DataType
  ): FieldData = {
    FieldData(
      `type` = DataType.Array,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.ArrayData(
            ArrayArray(
              data = fieldValues,
              elementType = elementType
            )
          )
        )
      )
    )
  }

  def packJsonFieldData(
      fieldName: String,
      fieldValues: Seq[String]
  ): FieldData = {
    FieldData(
      `type` = DataType.JSON,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.JsonData(
            JSONArray(data = fieldValues.map(ByteString.copyFromUtf8))
          )
        )
      )
    )
  }

  def packGeometryFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Byte]]
  ): FieldData = {
    FieldData(
      `type` = DataType.Geometry,
      fieldName = fieldName,
      field = FieldData.Field.Scalars(
        ScalarField(data =
          ScalarField.Data.GeometryData(
            GeometryArray(
              data =
                fieldValues.map(bytes => ByteString.copyFrom(bytes.toArray))
            )
          )
        )
      )
    )
  }

  def packFloatVectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Float]],
      dim: Int
  ): FieldData = {
    val allValues = fieldValues.flatten.toArray
    FieldData(
      `type` = DataType.FloatVector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.FloatVector(
            value = FloatArray(data = allValues)
          )
        )
      )
    )
  }

  def packBinaryVectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Byte]],
      dim: Int
  ): FieldData = {
    val allValues = ByteString.copyFrom(fieldValues.flatten.toArray)
    FieldData(
      `type` = DataType.BinaryVector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.BinaryVector(allValues)
        )
      )
    )
  }

  def packInt8VectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Short]],
      dim: Int
  ): FieldData = {
    val allValues = fieldValues.flatten.toArray.map(_.toByte)
    FieldData(
      `type` = DataType.Int8Vector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.Int8Vector(
            value = ByteString.copyFrom(allValues)
          )
        )
      )
    )
  }

  def packFloat16VectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Float]],
      dim: Int
  ): FieldData = {
    val allValues =
      fieldValues.flatten.map(FloatConverter.toFloat16Bytes).flatten
    FieldData(
      `type` = DataType.Float16Vector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.Float16Vector(
            value = ByteString.copyFrom(allValues.toArray)
          )
        )
      )
    )
  }

  def packBFloat16VectorFieldData(
      fieldName: String,
      fieldValues: Seq[Seq[Float]],
      dim: Int
  ): FieldData = {
    val allValues =
      fieldValues.flatten.map(FloatConverter.toBFloat16Bytes).flatten
    FieldData(
      `type` = DataType.BFloat16Vector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.Bfloat16Vector(
            value = ByteString.copyFrom(allValues.toArray)
          )
        )
      )
    )
  }

  def packSparseFloatVectorFieldData(
      fieldName: String,
      fieldValues: Seq[Map[Long, Float]],
      dim: Int
  ): FieldData = {
    val sparseDim = fieldValues.map(_.size).max
    val allValues = fieldValues
      .map(SparseFloatVectorConverter.encodeSparseFloatVector)
      .toArray
    FieldData(
      `type` = DataType.SparseFloatVector,
      fieldName = fieldName,
      field = FieldData.Field.Vectors(
        VectorField(
          dim = dim,
          data = VectorField.Data.SparseFloatVector(
            SparseFloatArray(
              contents = allValues.map(ByteString.copyFrom),
              dim = sparseDim
            )
          )
        )
      )
    )
  }
}

object FloatConverter {
  private def isWithinFloat16Range(value: Float): Boolean = {
    val absValue = Math.abs(value)
    // float16 range: ±65504
    absValue <= 65504f && !value.isNaN && !value.isInfinite
  }

  private def isWithinBFloat16Range(value: Float): Boolean = {
    // bfloat16 has 8 exponent bits, same as float32
    // so it can represent the same range as float32
    // but with less precision
    !value.isNaN && !value.isInfinite
  }

  def toFloat16Bytes(value: Float): Seq[Byte] = {
    if (!isWithinFloat16Range(value)) {
      throw new DataParseException(
        s"Value $value is out of float16 range"
      )
    }

    // IEEE 754 float32 format: 1 sign bit, 8 exponent bits, 23 fraction bits
    val bits = java.lang.Float.floatToRawIntBits(value)
    val sign = (bits >>> 31) & 0x1
    val exp = (bits >>> 23) & 0xff
    val frac = bits & 0x7fffff

    // IEEE 754 float16 format: 1 sign bit, 5 exponent bits, 10 fraction bits
    val f16Sign = sign
    val f16Exp = if (exp == 0) {
      0 // Zero/Subnormal
    } else if (exp == 0xff) {
      0x1f // Infinity/NaN
    } else {
      val newExp = exp - 127 + 15
      if (newExp < 0) 0
      else if (newExp > 0x1f) 0x1f
      else newExp
    }
    val f16Frac = frac >>> 13

    // Combine components into a half-precision (float16) value
    val f16Bits = (f16Sign << 15) | (f16Exp << 10) | f16Frac

    // Create byte array in big-endian format (high byte first)
    // val bytes = new Array[Byte](2)
    // bytes(0) = ((f16Bits >>> 8) & 0xff).toByte
    // bytes(1) = (f16Bits & 0xff).toByte
    // bytes.toSeq

    // Create byte array in little-endian format (low byte first)
    val bytes = new Array[Byte](2)
    bytes(0) = (f16Bits & 0xff).toByte // Low byte
    bytes(1) = ((f16Bits >>> 8) & 0xff).toByte // High byte
    bytes.toSeq
  }

  def toBFloat16Bytes(value: Float): Seq[Byte] = {
    if (!isWithinBFloat16Range(value)) {
      throw new DataParseException(
        s"Value $value is out of bfloat16 range"
      )
    }

    // Get raw 32-bit representation
    val intValue = java.lang.Float.floatToRawIntBits(value)

    // bfloat16 preserves the sign bit, all 8 exponent bits, and the top 7 bits of the fraction
    // Create byte array in big-endian format (high byte first)
    // val bytes = new Array[Byte](2)
    // bytes(0) = ((intValue >>> 24) & 0xff).toByte
    // bytes(1) = ((intValue >>> 16) & 0xff).toByte
    // bytes.toSeq

    // Create byte array in little-endian format (low byte first)
    val bytes = new Array[Byte](2)
    bytes(0) =
      ((intValue >>> 16) & 0xff).toByte // Low byte (middle 8 bits of float32)
    bytes(1) =
      ((intValue >>> 24) & 0xff).toByte // High byte (top 8 bits of float32)
    bytes.toSeq
  }

  def fromBFloat16Bytes(bytes: Seq[Byte]): Float = {
    if (bytes.length != 2) {
      throw new DataParseException(
        s"BFloat16 requires 2 bytes, but got ${bytes.length}"
      )
    }

    // Reconstruct the 16-bit bfloat16 value (little-endian: low byte first)
    val bfloat16Bits = ((bytes(1) & 0xff) << 8) | (bytes(0) & 0xff)

    // To convert bfloat16 to float32, we essentially shift the 16 bits left by 16
    // and pad the lower 16 bits with zeros. This is because bfloat16 has the
    // same exponent range as float32, and its mantissa is the upper part of float32's.
    val float32Bits = bfloat16Bits << 16

    java.lang.Float.intBitsToFloat(float32Bits)
  }

  def fromFloat16Bytes(bytes: Seq[Byte]): Float = {
    if (bytes.length != 2) {
      throw new DataParseException(
        s"Float16 requires 2 bytes, but got ${bytes.length}"
      )
    }

    // Reconstruct the 16-bit float16 value (little-endian: low byte first)
    val f16Bits = ((bytes(1) & 0xff) << 8) | (bytes(0) & 0xff)

    // Extract float16 components: 1 sign bit, 5 exponent bits, 10 fraction bits
    val f16Sign = (f16Bits >>> 15) & 0x1
    val f16Exp = (f16Bits >>> 10) & 0x1f
    val f16Frac = f16Bits & 0x3ff

    // Convert float16 components to float32 components
    var float32Sign = f16Sign
    var float32Exp = 0
    var float32Frac = 0

    if (f16Exp == 0) { // Denormalized or Zero
      if (f16Frac == 0) { // Zero
        float32Exp = 0
        float32Frac = 0
      } else { // Denormalized
        // Find leading 1 and normalize
        var msbPos = 0
        var tempFrac = f16Frac
        while (((tempFrac >>> (10 - 1 - msbPos)) & 0x1) == 0 && msbPos < 10) {
          msbPos += 1
        }
        // Adjust exponent for denormalized numbers
        float32Exp =
          127 - 15 - msbPos // 127 (float32 bias) - 15 (float16 bias) - leading zero count
        float32Frac =
          (f16Frac << (msbPos + 1)) & 0x7fffff // Shift to align with float32 mantissa
      }
    } else if (f16Exp == 0x1f) { // Infinity or NaN
      float32Exp = 0xff // Float32 infinity/NaN exponent
      float32Frac =
        if (f16Frac == 0) 0 else (f16Frac << 13) // Propagate NaN payload
    } else { // Normalized
      float32Exp = f16Exp - 15 + 127 // Adjust bias
      float32Frac =
        f16Frac << 13 // Shift fraction to align with float32 mantissa
    }

    // Combine float32 components into a 32-bit integer
    val float32Bits = (float32Sign << 31) | (float32Exp << 23) | float32Frac

    java.lang.Float.intBitsToFloat(float32Bits)
  }

  def fromFloatBytes(bytes: Seq[Byte]): Float = {
    if (bytes.length != 4) {
      throw new DataParseException(
        s"Float requires 4 bytes, but got ${bytes.length}"
      )
    }
    val buffer = ByteBuffer.wrap(bytes.toArray).order(ByteOrder.LITTLE_ENDIAN)
    buffer.getFloat()
  }

  def toFloatBytes(value: Float): Seq[Byte] = {
    val buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putFloat(value)
    buffer.array()
  }
}

object IntConverter {
  def fromUInt32Bytes(bytes: Seq[Byte]): Long = {
    if (bytes.length != 4) {
      throw new DataParseException(
        s"UInt32 requires 4 bytes, but got ${bytes.length}"
      )
    }

    val buffer = ByteBuffer.wrap(bytes.toArray).order(ByteOrder.LITTLE_ENDIAN)
    val signedInt = buffer.getInt()
    signedInt & 0xffffffff
  }

  def toUInt32Bytes(value: Long): Seq[Byte] = {
    if (value < 0 || value > 0xffffffffL) {
      throw new IllegalArgumentException(
        s"Value $value is out of UInt32 range (0 to 4294967295)"
      )
    }
    val buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putInt(value.toInt)
    buffer.array()
  }
}

object SparseFloatVectorConverter {
  def encodeSparseFloatVector(sparse: Map[Long, Float]): Array[Byte] = {
    val sortedEntries = sparse.toSeq.sortBy(_._1)

    val buf = ByteBuffer.allocate((4 + 4) * sortedEntries.size)
    buf.order(ByteOrder.LITTLE_ENDIAN)

    for ((k, v) <- sortedEntries) {
      if (k < 0 || k >= Math.pow(2.0, 32) - 1) {
        throw new DataParseException(
          s"Sparse vector index ($k) must be positive and less than 2^32-1"
        )
      }

      val lBuf = ByteBuffer.allocate(8)
      lBuf.order(ByteOrder.LITTLE_ENDIAN)
      lBuf.putLong(k)
      buf.put(lBuf.array(), 0, 4)

      if (v.isNaN || v.isInfinite) {
        throw new DataParseException(
          s"Sparse vector value ($v) cannot be NaN or Infinite"
        )
      }

      buf.putFloat(v)
    }

    buf.array()
  }
}

object MilvusSchemaUtil {
  def getDim(fieldSchema: FieldSchema): Int = {
    for (param <- fieldSchema.typeParams) {
      if (param.key == "dim") {
        return param.value.toInt
      }
    }
    throw new DataParseException(
      s"Field ${fieldSchema.name} has no dim parameter"
    )
  }
}
