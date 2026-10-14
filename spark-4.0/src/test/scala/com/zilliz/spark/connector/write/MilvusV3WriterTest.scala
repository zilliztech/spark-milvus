package com.zilliz.spark.connector.write

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.options.{MilvusOption, OptionParsing}

class MilvusV3WriterTest extends AnyFunSuite with Matchers {

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
    "the staging prefix is fs.root_path on object storage and empty locally"
  ) {
    // The local backend is rooted at fs.root_path and appends every key to
    // it; repeating the root would put the segments under {root}/{root}/.
    StagingLayout.rootFor(
      Map(StorageProperties.RootPath -> "files")
    ) shouldBe "files"
    StagingLayout.rootFor(Map.empty) shouldBe "files"
    StagingLayout.rootFor(
      Map(
        StorageProperties.StorageType -> "local",
        StorageProperties.RootPath -> "/tmp/run"
      )
    ) shouldBe ""
    StagingLayout.rootFor(
      Map(StorageProperties.StorageType -> "LOCAL")
    ) shouldBe ""
    StagingLayout.rootFor(
      Map(
        StorageProperties.StorageType -> "remote",
        StorageProperties.RootPath -> "milvus"
      )
    ) shouldBe "milvus"
  }

  test("positiveDouble accepts finite positive values") {
    OptionParsing.positiveDouble(
      Map(MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "64.5"),
      MilvusOption.WriterVariableWidthBytesPerValue,
      defaultValue = 32.0
    ) shouldBe 64.5
  }

  test("positiveDouble retains the writer default for blank density") {
    OptionParsing.positiveDouble(
      Map(MilvusOption.WriterVariableWidthBytesPerValue.toLowerCase -> "   "),
      MilvusOption.WriterVariableWidthBytesPerValue,
      defaultValue = 32.0
    ) shouldBe 32.0
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
