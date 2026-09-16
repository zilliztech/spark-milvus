package com.zilliz.spark.connector.write

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.options.MilvusOption

class MilvusV3WriterTest extends AnyFunSuite with Matchers {

  test("parsePositiveDoubleOption rejects invalid values") {
    Seq("0", "-1", "NaN", "Infinity", "-Infinity", "abc").foreach { value =>
      an[IllegalArgumentException] should be thrownBy {
        MilvusV3PartitionWriter.parsePositiveDoubleOption(
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
    MilvusV3PartitionWriter.parsePositiveDoubleOption(
      Map(MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "64.5"),
      MilvusOption.WriterVariableWidthBytesPerValue,
      defaultValue = 32.0
    ) shouldBe 64.5
  }

  test(
    "constructor validates variable-width density before bucket validation"
  ) {
    val options = Map(
      StorageProperties.BucketName -> "   ",
      MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "NaN"
    )

    val err = intercept[IllegalArgumentException] {
      new MilvusV3PartitionWriter(
        partitionId = 1,
        taskId = 1L,
        sparkSchema = org.apache.spark.sql.types.StructType(Nil),
        milvusOption = MilvusOption(options),
        storage = Map(StorageProperties.StorageType -> "local")
      )
    }

    err.getMessage should include(MilvusOption.WriterVariableWidthBytesPerValue)
    err.getMessage should not include (StorageProperties.BucketName)
  }
}
