package com.zilliz.spark.connector.write

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.MilvusOption

class MilvusV2BinlogWriterTest extends AnyFunSuite with Matchers {

  test("parseVariableWidthBytesPerValue accepts finite positive values") {
    MilvusV2BinlogWriter.parseVariableWidthBytesPerValue(
      Map(MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "64.5")
    ) shouldBe 64.5
  }

  test(
    "parseVariableWidthBytesPerValue returns default when option is absent"
  ) {
    MilvusV2BinlogWriter.parseVariableWidthBytesPerValue(
      Map.empty
    ) shouldBe 32.0
  }

  test("parseVariableWidthBytesPerValue rejects invalid values") {
    Seq("0", "-1", "NaN", "Infinity", "-Infinity", "abc").foreach { value =>
      an[IllegalArgumentException] should be thrownBy {
        MilvusV2BinlogWriter.parseVariableWidthBytesPerValue(
          Map(
            MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> value
          )
        )
      }
    }
  }
}
