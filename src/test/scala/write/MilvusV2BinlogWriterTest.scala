package com.zilliz.spark.connector.write

import java.nio.file.Files

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.MilvusOption

class MilvusV2BinlogWriterTest extends AnyFunSuite with Matchers {

  private val singleFieldSchema = StructType(
    Seq(StructField("payload", StringType, nullable = true))
  )

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

  test(
    "constructor validates variable-width density before IAM bucket validation"
  ) {
    val tempDir = Files.createTempDirectory("v2-binlog-writer-test-")
    val options = Map(
      Properties.FsConfig.FsUseIam -> "true",
      Properties.FsConfig.FsRootPath -> tempDir.toString,
      MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "NaN"
    )

    val err = intercept[IllegalArgumentException] {
      new MilvusV2BinlogWriter(
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
    err.getMessage should not include (Properties.FsConfig.FsBucketName)
  }
}
