package com.zilliz.spark.connector.types

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class Utf8FromBinaryColumnTest extends AnyFunSuite with Matchers {
  test("a VarBinary string column refuses invalid UTF-8") {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new VarBinaryVector("s", allocator)
    try {
      vector.allocateNew()
      vector.setSafe(0, Array[Byte](0xc3.toByte, 0x28))
      vector.setValueCount(1)

      val error = intercept[IllegalArgumentException] {
        new Utf8FromBinaryColumn(vector).getUTF8String(0)
      }
      error.getMessage should include("not valid UTF-8")
    } finally {
      vector.close()
      allocator.close()
    }
  }
}
