package com.zilliz.milvus.storage.codec

import java.nio.{ByteBuffer, ByteOrder}

import com.zilliz.milvus.storage.DataParseException

object FloatConverter {
  private def isFiniteFloat16Input(value: Float): Boolean =
    !value.isNaN && !value.isInfinite

  private def isWithinBFloat16Range(value: Float): Boolean = {
    // bfloat16 has 8 exponent bits, same as float32
    // so it can represent the same range as float32
    // but with less precision
    !value.isNaN && !value.isInfinite
  }

  def toFloat16Bytes(value: Float): Seq[Byte] = {
    if (!isFiniteFloat16Input(value)) {
      throw new DataParseException(
        s"Value $value is not a finite float16 input"
      )
    }

    // Match Milvus's float16.Fromfloat32 conversion: IEEE-754
    // round-to-nearest-even, including subnormal values. Truncating the
    // mantissa here makes normal ingestion produce different bytes from
    // Milvus import/backfill for values such as 0.3f and 0.7f.
    val bits = java.lang.Float.floatToRawIntBits(value)
    val sign = (bits >>> 16) & 0x8000
    val exponent = (bits >>> 23) & 0xff
    val mantissa = bits & 0x7fffff
    val halfExponent = exponent - 127 + 15

    if (halfExponent >= 31) {
      throw new DataParseException(
        s"Value $value is out of float16 range"
      )
    }

    val f16Bits = if (halfExponent <= 0) {
      if (halfExponent < -10) {
        sign
      } else {
        val normalizedMantissa = mantissa | 0x800000
        val shift = 14 - halfExponent
        var halfMantissa = normalizedMantissa >>> shift
        val remainderMask = (1 << shift) - 1
        val remainder = normalizedMantissa & remainderMask
        val halfway = 1 << (shift - 1)
        if (
          remainder > halfway ||
          (remainder == halfway && (halfMantissa & 1) != 0)
        ) {
          halfMantissa += 1
        }
        sign | halfMantissa
      }
    } else {
      var half = sign | (halfExponent << 10) | (mantissa >>> 13)
      val remainder = mantissa & 0x1fff
      if (remainder > 0x1000 || (remainder == 0x1000 && (half & 1) != 0)) {
        half += 1
      }
      if (((half >>> 10) & 0x1f) == 0x1f) {
        throw new DataParseException(
          s"Value $value rounds out of float16 range"
        )
      }
      half
    }

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
