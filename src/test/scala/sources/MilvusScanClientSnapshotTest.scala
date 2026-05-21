package com.zilliz.spark.connector.sources

import java.util.{Base64, HashMap}

import org.apache.spark.sql.sources.{EqualTo, Filter}
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
  private val emptySchemaBytes =
    Base64.getEncoder.encodeToString(Array.emptyByteArray)

  private def scanWithOptions(
      rawOptions: HashMap[String, String]
  ): MilvusScan = {
    new MilvusScan(
      StructType(Seq(StructField("id", LongType, nullable = false))),
      new CaseInsensitiveStringMap(rawOptions)
    )
  }

  test("snapshot table schema appends requested extra metadata columns") {
    val rawOptions = new HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, "[]")
    rawOptions.put(MilvusOption.MilvusCollectionName, "c")
    rawOptions.put(
      MilvusOption.MilvusExtraColumns,
      "partition,$segment_id,$row_offset"
    )
    val options = new CaseInsensitiveStringMap(rawOptions)
    val table = MilvusTable(
      MilvusOption(options),
      Some(StructType(Seq(StructField("id", LongType, nullable = false))))
    )

    assert(
      table.schema().fieldNames.toSeq == Seq(
        "id",
        "partition",
        "$segment_id",
        "$row_offset"
      )
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

  test("pruneColumns uses first available field for empty required schema") {
    val rawOptions = new HashMap[String, String]()
    val schema = StructType(
      Seq(
        StructField("RowID", LongType, nullable = false),
        StructField("Timestamp", LongType, nullable = false)
      )
    )
    val builder = new MilvusScanBuilder(
      schema,
      new CaseInsensitiveStringMap(rawOptions)
    )

    builder.pruneColumns(StructType(Seq.empty))

    assert(builder.build().readSchema().fieldNames.toSeq == Seq("RowID"))
  }

  test("pushFilters returns all filters for client snapshot fast path") {
    val rawOptions = new HashMap[String, String]()
    rawOptions.put(MilvusOption.MilvusUri, "http://localhost:19530")
    rawOptions.put(MilvusOption.MilvusCollectionName, "c")
    val builder = new MilvusScanBuilder(
      StructType(Seq(StructField("id", LongType, nullable = false))),
      new CaseInsensitiveStringMap(rawOptions)
    )
    val filters: Array[Filter] = Array(EqualTo("id", 10L))

    assert(builder.pushFilters(filters).sameElements(filters))
    assert(builder.pushedFilters().isEmpty)
  }

  test(
    "buildClientSnapshotOptions preserves read options and adds snapshot options"
  ) {
    val base = Map(
      MilvusOption.MilvusUri -> "http://localhost:19530",
      MilvusOption.MilvusCollectionName -> "c",
      MilvusOption.MilvusExtraColumns -> "$segment_id,$row_offset",
      MilvusOption.ReaderDebug -> "true",
      MilvusOption.SnapshotMode -> "false",
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

  test(
    "parseInsertLogPathIds extracts partition and segment from manifest base path"
  ) {
    assert(
      MilvusScan
        .parseInsertLogPathIds(
          "a-bucket/files/insert_log/10/20/30"
        ) == ("20" -> 30L)
    )
    assert(
      MilvusScan
        .parseInsertLogPathIds(
          "s3a://a-bucket/files/insert_log/10/21/31"
        ) == ("21" -> 31L)
    )
    val err = intercept[IllegalArgumentException] {
      MilvusScan.parseInsertLogPathIds("a-bucket/files/30")
    }
    assert(err.getMessage.contains("does not contain insert_log"))
  }

  test(
    "snapshot planner fails loudly when all snapshot segment lists are empty"
  ) {
    val rawOptions = new HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, "[]")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)

    val err = intercept[IllegalArgumentException] {
      scanWithOptions(rawOptions).planInputPartitions()
    }

    assert(
      err.getMessage.contains("no StorageV3 manifests or StorageV2 segments")
    )
  }

  test("snapshot planner tags V3 partitions from manifest base path") {
    val manifestJson = MilvusSnapshotReader.serializeManifestList(
      Seq(
        StorageV2ManifestItem(
          0L,
          "{\"ver\":7,\"base_path\":\"a-bucket/files/insert_log/10/20/30\"}"
        )
      )
    )
    val rawOptions = new HashMap[String, String]()
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

  test("snapshot planner rejects mismatched V3 segment id metadata") {
    val manifestJson = MilvusSnapshotReader.serializeManifestList(
      Seq(
        StorageV2ManifestItem(
          31L,
          "{\"ver\":7,\"base_path\":\"a-bucket/files/insert_log/10/20/30\"}"
        )
      )
    )
    val rawOptions = new HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, manifestJson)
    rawOptions.put(MilvusOption.SnapshotPartitionIds, "20")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)

    val err = intercept[IllegalArgumentException] {
      scanWithOptions(rawOptions).planInputPartitions()
    }

    assert(err.getMessage.contains("does not match base_path segment"))
  }

  test("snapshot planner accepts V2-only snapshot segments") {
    val v2Json = MilvusSnapshotReader.serializeV2Segments(
      Seq(
        V2SegmentInfo(
          segmentId = 30L,
          partitionId = 20L,
          numOfRows = 2L,
          storageVersion = 2L,
          columnGroups = Seq(
            V2ColumnGroup(
              fieldIds = Seq(0L, 1L),
              filePaths = Seq("a-bucket/files/insert_log/10/20/30/0/1"),
              fileRowCounts = Seq(2L)
            )
          )
        )
      )
    )
    val rawOptions = new HashMap[String, String]()
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
