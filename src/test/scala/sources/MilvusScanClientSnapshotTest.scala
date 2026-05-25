package com.zilliz.spark.connector.sources

import java.{util => ju}

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.read.{
  MilvusPackedV2InputPartition,
  MilvusSnapshotReader,
  MilvusStorageV3InputPartition,
  StorageV2ManifestItem,
  V2ColumnGroup,
  V2SegmentInfo
}
import com.zilliz.spark.connector.MilvusOption

class MilvusScanClientSnapshotTest extends AnyFunSuite {
  private val emptySchemaBytes = java.util.Base64.getEncoder.encodeToString(
    io.milvus.grpc.schema.CollectionSchema(name = "c").toByteArray
  )

  private def scanWithOptions(
      rawOptions: ju.HashMap[String, String]
  ): MilvusScan = {
    new MilvusScan(
      StructType(Seq(StructField("RowID", LongType, nullable = false))),
      new CaseInsensitiveStringMap(rawOptions)
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

  test("resolveClientSnapshotLocation rejects unsupported schemes") {
    Seq("gs://a-bucket/files/snapshot.json", "file:///tmp/snapshot.json")
      .foreach { location =>
        val err = intercept[IllegalArgumentException] {
          MilvusScan.resolveClientSnapshotLocation(location, "ignored")
        }
        assert(
          err.getMessage.contains("Unsupported snapshot s3_location scheme")
        )
      }
  }

  test("snapshotBucket extracts authority when URI host is null") {
    assert(
      MilvusScan.snapshotBucket(
        "s3a://snapshot_bucket/files/snapshots/1/metadata/2.json"
      ) == Some("snapshot_bucket")
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
        MilvusOption(base + (MilvusOption.MilvusPartitionName -> "p"))
      )
    )
    assert(
      !MilvusScan.canUseClientSnapshotFastPath(
        MilvusOption(base + (MilvusOption.MilvusPartitionID -> "20"))
      )
    )
    assert(
      !MilvusScan.canUseClientSnapshotFastPath(
        MilvusOption(base + (MilvusOption.MilvusSegmentID -> "30"))
      )
    )
  }

  test(
    "buildClientSnapshotOptions preserves read options and adds snapshot options"
  ) {
    val base = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusCollectionName -> "c",
      MilvusOption.MilvusExtraColumns -> "partition",
      MilvusOption.SnapshotMode -> "false",
      MilvusOption.SnapshotCollectionId -> "old",
      MilvusOption.SnapshotSchemaJson -> "stale-schema-json"
    )
    val out = MilvusScan.buildClientSnapshotOptions(
      baseOptions = base,
      collectionName = "snapshot_collection",
      collectionId = 10L,
      partitionIds = Seq(20L, 21L),
      schemaBytesBase64 = "abc",
      manifestList = Seq.empty,
      v2Segments = Seq.empty
    )
    assert(out(MilvusOption.SnapshotMode) == "true")
    assert(out(MilvusOption.MilvusCollectionName) == "snapshot_collection")
    assert(out(MilvusOption.SnapshotCollectionId) == "10")
    assert(out(MilvusOption.SnapshotPartitionIds) == "20,21")
    assert(!out.contains(MilvusOption.SnapshotSchemaJson))
    assert(out(MilvusOption.SnapshotSchemaBytes) == "abc")
    assert(out.contains(MilvusOption.SnapshotManifests))
    assert(out(MilvusOption.MilvusExtraColumns) == "partition")
  }

  test("parsePositiveLongOption rejects non-numeric and non-positive values") {
    Seq("not-a-number", "0", "-1").foreach { value =>
      val rawOptions = new ju.HashMap[String, String]()
      rawOptions.put(
        MilvusOption.ClientSnapshotCompactionProtectionSeconds,
        value
      )
      val err = intercept[IllegalArgumentException] {
        MilvusScan.parsePositiveLongOption(
          new CaseInsensitiveStringMap(rawOptions),
          MilvusOption.ClientSnapshotCompactionProtectionSeconds,
          86400L
        )
      }
      assert(
        err.getMessage.contains(
          MilvusOption.ClientSnapshotCompactionProtectionSeconds
        )
      )
    }
  }

  test("snapshot planner returns no partitions for empty snapshots") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, "[]")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)
    val scan = scanWithOptions(rawOptions)
    val firstPartitions = scan.planInputPartitions()
    val secondPartitions = scan.planInputPartitions()
    assert(firstPartitions.isEmpty)
    assert(firstPartitions eq secondPartitions)
  }

  test("snapshot planner fails loudly on malformed manifest JSON") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, "not-json")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)
    val err = intercept[Exception] {
      scanWithOptions(rawOptions).planInputPartitions()
    }
    assert(err.getMessage.contains("Failed to parse snapshot manifests"))
  }

  test("snapshot planner tags V3 partitions with partition ID string") {
    val manifestJson = MilvusSnapshotReader.serializeManifestList(
      Seq(
        StorageV2ManifestItem(
          30L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
        )
      )
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, manifestJson)
    rawOptions.put(MilvusOption.SnapshotPartitionIds, "20,21")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)
    val partitions = scanWithOptions(rawOptions).planInputPartitions()
    assert(partitions.length == 1)
    val partition = partitions.head.asInstanceOf[MilvusStorageV3InputPartition]
    assert(partition.partitionName == "20")
    assert(partition.segmentID == 30L)
    assert(partition.readVersion == 7L)
  }

  test("snapshot planner accepts V2-only snapshot segments") {
    val v2Json = MilvusSnapshotReader.serializeV2Segments(
      Seq(
        V2SegmentInfo(
          segmentId = 30L,
          partitionId = 20L,
          numOfRows = 1L,
          storageVersion = 2L,
          columnGroups = Seq(
            V2ColumnGroup(
              fieldIds = Seq(100L),
              filePaths = Seq("files/insert_log/10/20/30/100/1.parquet"),
              fileRowCounts = Seq(1L)
            )
          )
        )
      )
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotV2Segments, v2Json)
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)
    val partitions = scanWithOptions(rawOptions).planInputPartitions()
    assert(partitions.length == 1)
    val partition = partitions.head.asInstanceOf[MilvusPackedV2InputPartition]
    assert(partition.segmentID == 30L)
    assert(partition.partitionID == 20L)
  }
}
