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
}
