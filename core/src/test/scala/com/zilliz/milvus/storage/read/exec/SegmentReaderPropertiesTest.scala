package com.zilliz.milvus.storage.read.exec

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.plan.{ReadLimits, SegmentReadTask}
import com.zilliz.milvus.storage.snapshot.SegmentLayout

class SegmentReaderPropertiesTest extends AnyFunSuite with Matchers {

  test("typed batch limits override raw native reader keys") {
    val task = SegmentReadTask(
      segmentId = 3L,
      partitionId = 2L,
      layout = SegmentLayout.Manifest("files/insert_log/1/2/3", 7L),
      schemaBytes = Array.emptyByteArray,
      properties = Map(
        "fs.storage_type" -> "local",
        SegmentReaderRegistry.RecordBatchMaxRows -> "1",
        SegmentReaderRegistry.RecordBatchMaxSize -> "2"
      ),
      limits = ReadLimits(2048, 16777216L, 67108864L)
    )

    SegmentReaderRegistry.nativeProperties(task) should contain allOf (
      "fs.storage_type" -> "local",
      SegmentReaderRegistry.RecordBatchMaxRows -> "2048",
      SegmentReaderRegistry.RecordBatchMaxSize -> "16777216"
    )
  }

  test("a batch is requested as eight ranges at once, none under 4 MiB") {
    def task(batchMaxBytes: Long, raw: Map[String, String] = Map.empty) =
      SegmentReadTask(
        segmentId = 3L,
        partitionId = 2L,
        layout = SegmentLayout.Manifest("files/insert_log/1/2/3", 7L),
        schemaBytes = Array.emptyByteArray,
        properties = Map("fs.storage_type" -> "local") ++ raw,
        limits = ReadLimits(2048, batchMaxBytes, 67108864L)
      )

    SegmentReaderRegistry.nativeProperties(
      task(
        32L << 20,
        Map(
          SegmentReaderRegistry.PrebufferLazy -> "true",
          SegmentReaderRegistry.PrebufferRangeSizeLimit -> "1"
        )
      )
    ) should contain allOf (
      SegmentReaderRegistry.PrebufferLazy -> "false",
      SegmentReaderRegistry.PrebufferRangeSizeLimit -> (4L << 20).toString
    )
    SegmentReaderRegistry.rangeBytes(256L << 20) shouldBe (32L << 20)
    SegmentReaderRegistry.rangeBytes(8L << 20) shouldBe (4L << 20)
    SegmentReaderRegistry.rangeBytes((32L << 20) + 1) shouldBe (4L << 20) + 1
  }

  test("the IO pool holds every range of one batch per processor") {
    SegmentReaderRegistry.ioThreads(4) shouldBe 32
    SegmentReaderRegistry.ioThreads(
      1
    ) shouldBe SegmentReaderRegistry.RangesPerBatch
  }
}
