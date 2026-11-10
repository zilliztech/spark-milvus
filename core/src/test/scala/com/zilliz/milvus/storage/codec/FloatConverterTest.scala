package com.zilliz.milvus.storage.codec

import java.lang.{Float => JavaFloat, Math => JavaMath}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FloatConverterTest extends AnyFunSuite with Matchers {
  test("every finite float16 pattern decodes to its exact mathematical value") {
    (0 to 0xffff).filter(bits => (bits & 0x7c00) != 0x7c00).foreach { bits =>
      val exponent = (bits >>> 10) & 0x1f
      val fraction = bits & 0x3ff
      // Binary16 is fraction * 2^-24 for subnormals, or
      // (1024 + fraction) * 2^(exponent - 25) for normal values.
      // Double represents both formulas exactly, independently of the decoder.
      val magnitude =
        if (exponent == 0) JavaMath.scalb(fraction.toDouble, -24)
        else JavaMath.scalb((1024 + fraction).toDouble, exponent - 25)
      val expected =
        (if ((bits & 0x8000) == 0) magnitude else -magnitude).toFloat
      withClue(f"float16 0x$bits%04x: ") {
        JavaFloat
          .floatToRawIntBits(FloatConverter.float16BitsToFloat(bits)) shouldBe
          JavaFloat.floatToRawIntBits(expected)
      }
    }
  }

  test("finite float16 byte values round trip without losing subnormal bits") {
    (0 to 0xffff).filter(bits => (bits & 0x7c00) != 0x7c00).foreach { bits =>
      val bytes = Seq(bits.toByte, (bits >>> 8).toByte)
      withClue(f"float16 0x$bits%04x: ") {
        FloatConverter.toFloat16Bytes(
          FloatConverter.fromFloat16Bytes(bytes)
        ) shouldBe bytes
      }
    }
  }

  test("float16 infinities and NaNs retain their classification and sign") {
    Seq(0, 0x8000).foreach { sign =>
      val infinity = FloatConverter.float16BitsToFloat(sign | 0x7c00)
      infinity shouldBe
        (if (sign == 0) Float.PositiveInfinity else Float.NegativeInfinity)
      (1 to 0x3ff).foreach { fraction =>
        val decoded =
          FloatConverter.float16BitsToFloat(sign | 0x7c00 | fraction)
        withClue(f"float16 0x${sign | 0x7c00 | fraction}%04x: ") {
          JavaFloat.isNaN(decoded) shouldBe true
          (JavaFloat.floatToRawIntBits(decoded) >>> 31) shouldBe (sign >>> 15)
        }
      }
    }
  }
}
