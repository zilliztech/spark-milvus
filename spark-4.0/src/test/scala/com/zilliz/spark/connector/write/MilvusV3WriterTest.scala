package com.zilliz.spark.connector.write

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  DataWriter,
  DataWriterFactory,
  PhysicalWriteInfo,
  WriterCommitMessage
}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.options.{MilvusOption, OptionParsing}

class MilvusV3WriterTest extends AnyFunSuite with Matchers {

  private class Batch extends BatchWrite {
    var commits = 0
    var aborts = 0
    override def createBatchWriterFactory(
        info: PhysicalWriteInfo
    ): DataWriterFactory = new MilvusV3WriterTest.Factory
    override def commit(messages: Array[WriterCommitMessage]): Unit =
      commits += 1
    override def abort(messages: Array[WriterCommitMessage]): Unit = {
      messages shouldBe empty
      aborts += 1
    }
  }

  test("a failed writer factory aborts the batch and preserves the failure") {
    val original = new IllegalStateException("factory failed")
    val abortFailure = new IllegalStateException("abort failed")
    val batch = new Batch {
      override def createBatchWriterFactory(
          info: PhysicalWriteInfo
      ): DataWriterFactory = throw original
      override def abort(messages: Array[WriterCommitMessage]): Unit = {
        super.abort(messages)
        throw abortFailure
      }
    }
    val failure = the[IllegalStateException] thrownBy
      MilvusV3Writer.writeV3(null, batch)
    failure should be theSameInstanceAs original
    failure.getSuppressed.toSeq shouldBe Seq(abortFailure)
    batch.aborts shouldBe 1
    batch.commits shouldBe 0
  }

  test("direct writes abort failed task collection but never abort a commit") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("direct-v3-write-protocol")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      val input = spark.range(1L).coalesce(1).toDF()
      val failed = new Batch
      val failure = the[RuntimeException] thrownBy
        MilvusV3Writer.writeV3(
          input.selectExpr("raise_error('task collection failed')"),
          failed
        )
      failure.getMessage should include("task collection failed")
      failed.aborts shouldBe 1
      failed.commits shouldBe 0

      val successful = new Batch
      MilvusV3Writer.writeV3(input, successful) shouldBe Seq("segment-0")
      successful.commits shouldBe 1
      successful.aborts shouldBe 0

      val commitFailure = new IllegalStateException("commit failed")
      val committed = new Batch {
        override def commit(messages: Array[WriterCommitMessage]): Unit = {
          super.commit(messages)
          throw commitFailure
        }
      }
      val failureAfterTasks = the[IllegalStateException] thrownBy
        MilvusV3Writer.writeV3(input, committed)
      failureAfterTasks should be theSameInstanceAs commitFailure
      committed.commits shouldBe 1
      committed.aborts shouldBe 0
    } finally spark.stop()
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

object MilvusV3WriterTest {
  final class Factory extends DataWriterFactory {
    override def createWriter(
        partitionId: Int,
        taskId: Long
    ): DataWriter[InternalRow] = new DataWriter[InternalRow] {
      override def write(row: InternalRow): Unit = ()
      override def commit(): WriterCommitMessage =
        MilvusV3CommitMessage(partitionId, 1L, s"segment-$partitionId", 1L)
      override def abort(): Unit = ()
      override def close(): Unit = ()
    }
  }
}
