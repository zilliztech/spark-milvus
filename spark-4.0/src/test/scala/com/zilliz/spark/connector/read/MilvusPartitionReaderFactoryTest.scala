package com.zilliz.spark.connector.read

import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.MilvusOption

class MilvusPartitionReaderFactoryTest extends AnyFunSuite {
  test("requestedExtraColumns normalizes legacy aliases") {
    val requested = MilvusPartitionReaderFactory.requestedExtraColumns(
      Map(MilvusOption.MilvusExtraColumns -> "partition,segment_id,row_offset")
    )

    assert(
      requested == Set(
        MilvusOption.MilvusExtraColumnPartition,
        MilvusOption.MilvusExtraColumnSegmentID,
        MilvusOption.MilvusExtraColumnRowOffset
      )
    )
  }

  test("metadata classification is limited to requested extra columns") {
    val requested = Set(MilvusOption.MilvusExtraColumnSegmentID)

    assert(
      !MilvusPartitionReaderFactory.isMetadataExtraField(
        MilvusOption.MilvusExtraColumnPartition,
        requested
      )
    )
    assert(
      MilvusPartitionReaderFactory.isMetadataExtraField(
        MilvusOption.MilvusExtraColumnSegmentID,
        requested
      )
    )
  }

  test("partition metadata values use Spark UTF8String representation") {
    assert(
      MilvusPartitionReaderFactory.stringValue("20") == UTF8String.fromString(
        "20"
      )
    )
  }
}
