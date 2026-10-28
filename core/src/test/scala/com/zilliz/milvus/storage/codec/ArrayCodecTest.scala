package com.zilliz.milvus.storage.codec

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.{IntArray, ScalarField}

class ArrayCodecTest extends AnyFunSuite with Matchers {
  test(
    "integer array payloads preserve Int8, Int16 and Int32 boundary values"
  ) {
    Seq(Seq(-128, 127), Seq(128, -32768, 32767), Seq(Int.MaxValue)).foreach {
      values =>
        val field =
          ScalarField(data = ScalarField.Data.IntData(IntArray(data = values)))
        ArrayCodec.elements(field.toByteArray) shouldBe values
    }
  }
}
