package com.zilliz.spark.connector.loon

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.write.MilvusLoonPartitionWriter
import com.zilliz.spark.connector.MilvusOption

class MilvusLoonWriterTest extends AnyFunSuite with Matchers {

  test("parsePositiveDoubleOption rejects invalid values") {
    Seq("0", "-1", "NaN", "Infinity", "-Infinity", "abc").foreach { value =>
      an[IllegalArgumentException] should be thrownBy {
        MilvusLoonPartitionWriter.parsePositiveDoubleOption(
          Map(
            MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> value
          ),
          MilvusOption.WriterVariableWidthBytesPerValue,
          defaultValue = 32.0
        )
      }
    }
  }

  test("parsePositiveDoubleOption accepts finite positive values") {
    MilvusLoonPartitionWriter.parsePositiveDoubleOption(
      Map(MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "64.5"),
      MilvusOption.WriterVariableWidthBytesPerValue,
      defaultValue = 32.0
    ) shouldBe 64.5
  }

  test(
    "constructor validates variable-width density before bucket validation"
  ) {
    val options = Map(
      Properties.FsConfig.FsBucketName -> "   ",
      MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "NaN"
    )

    val err = intercept[IllegalArgumentException] {
      new MilvusLoonPartitionWriter(
        partitionId = 1,
        taskId = 1L,
        sparkSchema = org.apache.spark.sql.types.StructType(Nil),
        milvusOption = MilvusOption(options)
      )
    }

    err.getMessage should include(MilvusOption.WriterVariableWidthBytesPerValue)
    err.getMessage should not include (Properties.FsConfig.FsBucketName)
  }
}
