package com.zilliz.spark.connector.loon

import scala.util.{Failure, Success}

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.write.{
  MilvusLoonPartitionWriter,
  MilvusLoonWriter
}
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

  test("Write DataFrame using Loon Writer to Minio") {
    val spark = SparkSession
      .builder()
      .appName("LoonWriterMinioTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1L, 25, "ABC"),
        (2L, 30, "BCD"),
        (3L, 35, "DDDD")
      ).toDF("int64_field", "int32_field", "string_field")

      // S3 configuration options
      val options = Map(
        Properties.FsConfig.FsStorageType -> "remote",
        Properties.FsConfig.FsAddress -> "localhost:9000",
        Properties.FsConfig.FsBucketName -> "a-bucket",
        Properties.FsConfig.FsRootPath -> "files",
        Properties.FsConfig.FsAccessKeyId -> "minioadmin",
        Properties.FsConfig.FsAccessKeyValue -> "minioadmin",
        Properties.FsConfig.FsUseSSL -> "false",
        Properties.FsConfig.FsRegion -> "us-east-1",
        MilvusOption.MilvusCollectionName -> "test_collection"
      )

      // Write using MilvusLoonWriter API
      val result = MilvusLoonWriter.writeDataFrame(testData, options)

      result match {
        case Success(manifestPaths) =>
          info(s"Successfully wrote ${testData.count()} rows to Minio")
          info(s"Manifest paths: ${manifestPaths.mkString(", ")}")
          manifestPaths should not be empty
        case Failure(exception) =>
          fail(s"Failed to write data: ${exception.getMessage}", exception)
      }

    } finally {
      spark.stop()
    }
  }

  test("Write DataFrame with vector field to Minio") {
    val spark = SparkSession
      .builder()
      .appName("StorageV2VectorWriterTest")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Create test data with vector field
      val testData = Seq(
        (1L, Array(0.1f, 0.2f, 0.3f, 0.4f)),
        (2L, Array(0.5f, 0.6f, 0.7f, 0.8f)),
        (3L, Array(0.9f, 1.0f, 1.1f, 1.2f))
      ).toDF("id", "vector")

      // S3 configuration options with vector dimension
      val options = Map(
        Properties.FsConfig.FsStorageType -> "remote",
        Properties.FsConfig.FsAddress -> "localhost:9000",
        Properties.FsConfig.FsBucketName -> "a-bucket",
        Properties.FsConfig.FsRootPath -> "files",
        Properties.FsConfig.FsAccessKeyId -> "minioadmin",
        Properties.FsConfig.FsAccessKeyValue -> "minioadmin",
        Properties.FsConfig.FsUseSSL -> "false",
        Properties.FsConfig.FsRegion -> "us-east-1",
        MilvusOption.MilvusCollectionName -> "vector_test_collection",
        "vector.vector.dim" -> "4" // Specify vector dimension
      )

      // Write using MilvusLoonWriter API
      val result = MilvusLoonWriter.writeDataFrame(testData, options)

      result match {
        case Success(manifestPaths) =>
          info(
            s"Successfully wrote ${testData.count()} rows with vectors to Minio"
          )
          info(s"Manifest paths: ${manifestPaths.mkString(", ")}")
          manifestPaths should not be empty
        case Failure(exception) =>
          fail(s"Failed to write data: ${exception.getMessage}", exception)
      }

    } finally {
      spark.stop()
    }
  }
}
