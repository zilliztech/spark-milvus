package com.zilliz.spark.connector.sources

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.read.{StorageV2ManifestItem, V2SegmentInfo}
import com.zilliz.spark.connector.MilvusOption

class MilvusScanClientSnapshotTest extends AnyFunSuite {
  test(
    "resolveClientSnapshotLocation prefixes bucket-relative snapshot locations"
  ) {
    assert(
      MilvusScan.resolveClientSnapshotLocation(
        "files/snapshots/1/metadata/2.json",
        "a-bucket"
      ) == "s3a://a-bucket/files/snapshots/1/metadata/2.json"
    )
  }

  test("resolveClientSnapshotLocation normalizes s3 scheme to s3a") {
    assert(
      MilvusScan.resolveClientSnapshotLocation(
        "s3://a-bucket/files/snapshots/1/metadata/2.json",
        "ignored"
      ) == "s3a://a-bucket/files/snapshots/1/metadata/2.json"
    )
  }

  test(
    "buildClientSnapshotOptions preserves read options and adds snapshot options"
  ) {
    val base = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusCollectionName -> "c",
      MilvusOption.MilvusExtraColumns -> "$segment_id,$row_offset",
      MilvusOption.ReaderDebug -> "true",
      MilvusOption.SnapshotCollectionId -> "old"
    )

    val out = MilvusScan.buildClientSnapshotOptions(
      base,
      collectionName = "c",
      collectionId = 10L,
      partitionIds = Seq(20L),
      snapshotJson = "{\"snapshot_info\":{},\"collection\":{}}",
      schemaBytesBase64 = "abc",
      manifestList = Seq(
        StorageV2ManifestItem(
          30L,
          "{\"ver\":1,\"base_path\":\"files/insert_log/10/20/30\"}"
        )
      ),
      v2Segments = Seq.empty[V2SegmentInfo]
    )

    assert(out(MilvusOption.SnapshotMode) == "true")
    assert(out(MilvusOption.MilvusCollectionName) == "c")
    assert(out(MilvusOption.SnapshotCollectionId) == "10")
    assert(out(MilvusOption.SnapshotPartitionIds) == "20")
    assert(out(MilvusOption.SnapshotSchemaJson).nonEmpty)
    assert(out(MilvusOption.SnapshotSchemaBytes) == "abc")
    assert(out.contains(MilvusOption.SnapshotManifests))
    assert(out(MilvusOption.MilvusExtraColumns) == "$segment_id,$row_offset")
    assert(out(MilvusOption.ReaderDebug) == "true")
  }
}
