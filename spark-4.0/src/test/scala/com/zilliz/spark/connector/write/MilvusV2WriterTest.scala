package com.zilliz.spark.connector.write

import java.nio.file.Files

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.options.{MilvusOption, OptionParsing}

class MilvusV2WriterTest extends AnyFunSuite with Matchers {

  private val singleFieldSchema = StructType(
    Seq(StructField("payload", StringType, nullable = true))
  )

  test("positiveDouble accepts finite positive values") {
    OptionParsing.positiveDouble(
      Map(MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "64.5"),
      MilvusOption.WriterVariableWidthBytesPerValue,
      defaultValue = 32.0
    ) shouldBe 64.5
  }

  test(
    "positiveDouble returns default when option is absent"
  ) {
    OptionParsing.positiveDouble(
      Map.empty,
      MilvusOption.WriterVariableWidthBytesPerValue,
      defaultValue = 32.0
    ) shouldBe 32.0
  }

  test("positiveDouble rejects invalid values") {
    Seq("0", "-1", "NaN", "Infinity", "-Infinity", "abc").foreach { value =>
      an[IllegalArgumentException] should be thrownBy {
        OptionParsing.positiveDouble(
          Map(
            MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> value
          ),
          MilvusOption.WriterVariableWidthBytesPerValue,
          defaultValue = 32.0
        )
      }
    }
  }

  test(
    "constructor validates variable-width density before IAM bucket validation"
  ) {
    val tempDir = Files.createTempDirectory("v2-binlog-writer-test-")
    val options = Map(
      StorageProperties.UseIam -> "true",
      StorageProperties.RootPath -> tempDir.toString,
      MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "NaN"
    )

    val err = intercept[IllegalArgumentException] {
      new MilvusV2Writer(
        collectionId = 1L,
        partitionId = 2L,
        segmentId = 3L,
        newFieldNames = Seq("payload"),
        newFieldIds = Seq(101L),
        targetSchema = singleFieldSchema,
        milvusOption = MilvusOption(options),
        allocateLogId = () => 1L
      )
    }

    err.getMessage should include(MilvusOption.WriterVariableWidthBytesPerValue)
    err.getMessage should not include (StorageProperties.BucketName)
  }
}
