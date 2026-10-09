package com.zilliz.milvus.storage.codec

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.DataType

/** Int8, Int16 and Int32 elements share `IntData`, so the element type is what
  * bounds them, as the Milvus proxy checks on insert
  * (internal/proxy/fieldvalidator/validate_util.go verifyOverflowByRange).
  */
class ArrayCodecTest extends AnyFunSuite with Matchers {

  test("Int8 and Int16 elements outside their range are refused") {
    Seq(128, -129).foreach { bad =>
      intercept[IllegalArgumentException](
        ArrayCodec.encode(DataType.Int8, Seq(bad))
      ).getMessage should include(bad.toString)
    }
    Seq(32768, -32769).foreach { bad =>
      intercept[IllegalArgumentException](
        ArrayCodec.encode(DataType.Int16, Seq(bad))
      )
    }
    ArrayCodec.elements(
      ArrayCodec.encode(DataType.Int8, Seq(-128, 127))
    ) shouldBe Seq(-128, 127)
    ArrayCodec.elements(
      ArrayCodec.encode(DataType.Int16, Seq(128, -32768, 32767))
    ) shouldBe Seq(128, -32768, 32767)
    ArrayCodec.elements(
      ArrayCodec.encode(DataType.Int32, Seq(Int.MaxValue))
    ) shouldBe Seq(Int.MaxValue)
  }
}
