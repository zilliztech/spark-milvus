package com.zilliz.spark.connector.sources

import java.util.HashMap

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.read.{StorageV2ManifestItem, V2SegmentInfo}
import com.zilliz.spark.connector.MilvusOption

class MilvusScanClientSnapshotTest extends AnyFunSuite {
  test("snapshot table schema appends requested extra metadata columns") {
    val rawOptions = new HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, "[]")
    rawOptions.put(MilvusOption.MilvusCollectionName, "c")
    rawOptions.put(
      MilvusOption.MilvusExtraColumns,
      "$segment_id,$row_offset"
    )
    val options = new CaseInsensitiveStringMap(rawOptions)
    val table = MilvusTable(
      MilvusOption(options),
      Some(StructType(Seq(StructField("id", LongType, nullable = false))))
    )

    assert(
      table.schema().fieldNames.toSeq == Seq("id", "$segment_id", "$row_offset")
    )
  }

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

  test("snapshotBucketsToConfigure includes cross-bucket snapshot locations") {
    assert(
      MilvusScan.snapshotBucketsToConfigure(
        "s3a://snapshot-bucket/files/snapshots/1/metadata/2.json",
        "connector-bucket"
      ) == Seq("connector-bucket", "snapshot-bucket")
    )
    assert(
      MilvusScan.snapshotBucketsToConfigure(
        "s3a://connector-bucket/files/snapshots/1/metadata/2.json",
        "connector-bucket"
      ) == Seq("connector-bucket")
    )
  }

  test(
    "client snapshot fast path is disabled when partition or segment selectors are set"
  ) {
    val base = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusCollectionName -> "c"
    )

    assert(MilvusScan.canUseClientSnapshotFastPath(MilvusOption(base)))
    assert(
      !MilvusScan.canUseClientSnapshotFastPath(
        MilvusOption(base + (MilvusOption.MilvusPartitionID -> "10"))
      )
    )
    assert(
      !MilvusScan.canUseClientSnapshotFastPath(
        MilvusOption(base + (MilvusOption.MilvusSegmentID -> "20"))
      )
    )
    assert(
      !MilvusScan.canUseClientSnapshotFastPath(
        MilvusOption(base + (MilvusOption.MilvusPartitionName -> "p1"))
      )
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
