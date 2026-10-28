package com.zilliz.milvus.storage.codec

import java.lang.{Float => JavaFloat}
import java.nio.{ByteBuffer, ByteOrder}

import com.zilliz.milvus.storage.DataParseException

object FloatConverter {
  def toFloat16Bytes(value: Float): Seq[Byte] =
    littleEndianBytes(floatToFloat16Bits(value))

  /** IEEE-754 float16 bits, without allocating a per-element byte container. */
  def floatToFloat16Bits(value: Float): Int = {
    if (!JavaFloat.isFinite(value)) {
      throw new DataParseException(
        s"Value $value is not a finite float16 input"
      )
    }

    // Match Milvus's float16.Fromfloat32 conversion: IEEE-754
    // round-to-nearest-even, including subnormal values. Truncating the
    // mantissa here makes normal ingestion produce different bytes from
    // Milvus import/backfill for values such as 0.3f and 0.7f.
    val bits = JavaFloat.floatToRawIntBits(value)
    val sign = (bits >>> 16) & 0x8000
    val exponent = (bits >>> 23) & 0xff
    val mantissa = bits & 0x7fffff
    val halfExponent = exponent - 127 + 15

    if (halfExponent >= 31) {
      throw new DataParseException(
        s"Value $value is out of float16 range"
      )
    }

    if (halfExponent <= 0) {
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
  }

  def toBFloat16Bytes(value: Float): Seq[Byte] =
    littleEndianBytes(floatToBfloat16Bits(value))

  /** Bfloat16 bits, retaining float32's upper 16 bits without rounding. */
  def floatToBfloat16Bits(value: Float): Int = {
    if (!JavaFloat.isFinite(value)) {
      throw new DataParseException(
        s"Value $value is out of bfloat16 range"
      )
    }

    JavaFloat.floatToRawIntBits(value) >>> 16
  }

  private def littleEndianBytes(bits: Int): Seq[Byte] =
    Array(bits.toByte, (bits >>> 8).toByte).toSeq

  def fromBFloat16Bytes(bytes: Seq[Byte]): Float = {
    if (bytes.length != 2) {
      throw new DataParseException(
        s"BFloat16 requires 2 bytes, but got ${bytes.length}"
      )
    }

    // Reconstruct the 16-bit bfloat16 value (little-endian: low byte first)
    bfloat16BitsToFloat(((bytes(1) & 0xff) << 8) | (bytes(0) & 0xff))
  }

  /** Same conversion from the 16 bits themselves.
    *
    * The columnar reader decodes straight out of an Arrow buffer and calls this
    * once per element, so it must not allocate a `Seq[Byte]` to do it.
    */
  def bfloat16BitsToFloat(bits: Int): Float = {
    // bfloat16 has float32's exponent range and its mantissa is float32's upper
    // part, so widening is a shift with zero padding.
    JavaFloat.intBitsToFloat((bits & 0xffff) << 16)
  }

  def fromFloat16Bytes(bytes: Seq[Byte]): Float = {
    if (bytes.length != 2) {
      throw new DataParseException(
        s"Float16 requires 2 bytes, but got ${bytes.length}"
      )
    }

    // Reconstruct the 16-bit float16 value (little-endian: low byte first)
    float16BitsToFloat(((bytes(1) & 0xff) << 8) | (bytes(0) & 0xff))
  }

  /** Same conversion from the 16 bits themselves. See [[bfloat16BitsToFloat]]
    * for why this exists separately.
    */
  def float16BitsToFloat(f16Bits: Int): Float = {
    // Extract float16 components: 1 sign bit, 5 exponent bits, 10 fraction bits
    val f16Sign = (f16Bits >>> 15) & 0x1
    val f16Exp = (f16Bits >>> 10) & 0x1f
    val f16Frac = f16Bits & 0x3ff

    // Convert float16 components to float32 components
    val float32Sign = f16Sign
    var float32Exp = 0
    var float32Frac = 0

    if (f16Exp == 0) { // Denormalized or Zero
      if (f16Frac == 0) { // Zero
        float32Exp = 0
        float32Frac = 0
      } else { // Denormalized
        // Find leading 1 and normalize
        var msbPos = 0
        val tempFrac = f16Frac
        while (((tempFrac >>> (10 - 1 - msbPos)) & 0x1) == 0 && msbPos < 10) {
          msbPos += 1
        }
        // Adjust exponent for denormalized numbers
        float32Exp =
          127 - 15 - msbPos // 127 (float32 bias) - 15 (float16 bias) - leading zero count
        float32Frac =
          ((f16Frac << (msbPos + 1)) & 0x3ff) << 13 // Drop the implicit leading bit, then align the fraction
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

    JavaFloat.intBitsToFloat(float32Bits)
  }

  def toFloatBytes(value: Float): Seq[Byte] = {
    val buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    buffer.putFloat(value)
    buffer.array()
  }
}
