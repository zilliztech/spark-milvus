package com.zilliz.spark.connector.sources

import java.{util => ju}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import scala.util.{Failure, Success}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{
  FSDataInputStream,
  FSDataOutputStream,
  FSInputStream,
  FileStatus,
  FileSystem,
  Path
}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  ByteType,
  FloatType,
  LongType,
  MetadataBuilder,
  ShortType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach

import com.zilliz.spark.connector.{MilvusCollectionInfo, MilvusOption}
import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.read.{
  Collection,
  CollectionSchema,
  MilvusDeletePlan,
  MilvusPackedV2InputPartition,
  MilvusSnapshotReader,
  MilvusStorageV3InputPartition,
  SnapshotInfo,
  SnapshotMetadata,
  StorageV2ManifestItem,
  V2ColumnGroup,
  V2SegmentInfo
}
import com.zilliz.spark.connector.serde.ArrowConverter

class MilvusScanClientSnapshotTest extends AnyFunSuite with BeforeAndAfterEach {
  private val emptySchemaBytes = java.util.Base64.getEncoder.encodeToString(
    io.milvus.grpc.schema.CollectionSchema(name = "c").toByteArray
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    CloseTrackingFileSystem.reset()
  }

  private def scanWithOptions(
      rawOptions: ju.HashMap[String, String]
  ): MilvusScan = {
    new MilvusScan(
      StructType(Seq(StructField("RowID", LongType, nullable = false))),
      new CaseInsensitiveStringMap(rawOptions)
    )
  }

  private val vectorSnapshotSchemaJson =
    """
      {
        "snapshot-info": {
          "name": "test",
          "id": 1,
          "collection_id": 10,
          "partition_ids": [1],
          "create_ts": 1
        },
        "collection": {
          "schema": {
            "name": "c",
            "fields": [
              {
                "fieldID": 100,
                "name": "binary_vec",
                "data_type": "BinaryVector",
                "type_params": [{"key": "dim", "value": "128"}]
              },
              {
                "fieldID": 101,
                "name": "float_vec",
                "data_type": "FloatVector",
                "type_params": [{"key": "dim", "value": "4"}]
              },
              {
                "fieldID": 102,
                "name": "int8_vec",
                "data_type": "Int8Vector",
                "type_params": [{"key": "dim", "value": "4"}]
              },
              {
                "fieldID": 103,
                "name": "json_payload",
                "data_type": "JSON"
              }
            ]
          }
        },
        "indexes": [],
        "manifest-list": []
      }
    """

  private val vectorSnapshotSchemaBytes =
    java.util.Base64.getEncoder.encodeToString(
      MilvusSnapshotReader
        .toProtobufSchemaBytes(
          MilvusSnapshotReader
            .parseSnapshotMetadata(vectorSnapshotSchemaJson)
            .toOption
            .get
            .collection
            .schema
        )
    )

  private def metadata(
      entries: (String, Long)*
  ): org.apache.spark.sql.types.Metadata = {
    val builder = new MetadataBuilder()
    entries.foreach { case (key, value) => builder.putLong(key, value) }
    builder.build()
  }

  private def snapshotTableSchema(
      baseSchema: StructType,
      extraColumns: String,
      snapshotSchemaJson: Option[String] = Some(vectorSnapshotSchemaJson),
      snapshotSchemaBytes: Option[String] = None
  ): StructType = {
    val options = scala.collection.mutable.Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotManifests -> "[]",
      MilvusOption.SnapshotCollectionId -> "10",
      MilvusOption.MilvusCollectionName -> "c",
      MilvusOption.MilvusExtraColumns -> extraColumns
    )
    snapshotSchemaJson.foreach(json =>
      options += MilvusOption.SnapshotSchemaJson -> json
    )
    snapshotSchemaBytes.foreach(bytes =>
      options += MilvusOption.SnapshotSchemaBytes -> bytes
    )
    MilvusTable(MilvusOption(options.toMap), Some(baseSchema)).schema()
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

  test("snapshotBucket returns None for bucket-relative snapshot locations") {
    assert(
      MilvusScan.snapshotBucket("files/snapshots/1/metadata/2.json") == None
    )
  }

  test("snapshotBucket rejects unsupported schemes") {
    val err = intercept[IllegalArgumentException] {
      MilvusScan.snapshotBucket("gs://a-bucket/files/snapshot.json")
    }
    assert(err.getMessage.contains("Unsupported snapshot s3_location scheme"))
  }

  test(
    "validateSnapshotBucketForRelativeDataPaths rejects cross-bucket relative data paths"
  ) {
    val err = intercept[IllegalArgumentException] {
      MilvusScan.validateSnapshotBucketForRelativeDataPaths(
        "s3a://snapshot-bucket/files/snapshots/1/metadata/snapshot.json",
        Some("connector-bucket"),
        Seq(
          StorageV2ManifestItem(
            30L,
            "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
          )
        ),
        Seq.empty
      )
    }
    assert(err.getMessage.contains("snapshot-bucket"))
    assert(err.getMessage.contains("connector-bucket"))
    assert(err.getMessage.contains("bucket-relative"))
  }

  test(
    "validateSnapshotBucketForRelativeDataPaths rejects unset connector bucket with relative V3 paths"
  ) {
    val err = intercept[IllegalArgumentException] {
      MilvusScan.validateSnapshotBucketForRelativeDataPaths(
        "s3a://snapshot-bucket/files/snapshots/1/metadata/snapshot.json",
        None,
        Seq(
          StorageV2ManifestItem(
            30L,
            "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
          )
        ),
        Seq.empty
      )
    }
    assert(err.getMessage.contains("snapshot-bucket"))
    assert(err.getMessage.contains("<unset>"))
    assert(err.getMessage.contains("files/insert_log/10/20/30"))
  }

  test(
    "validateSnapshotBucketForRelativeDataPaths rejects unset connector bucket with relative V2 paths"
  ) {
    val err = intercept[IllegalArgumentException] {
      MilvusScan.validateSnapshotBucketForRelativeDataPaths(
        "s3a://snapshot-bucket/files/snapshots/1/metadata/snapshot.json",
        None,
        Seq.empty,
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
    }
    assert(err.getMessage.contains("snapshot-bucket"))
    assert(err.getMessage.contains("<unset>"))
    assert(err.getMessage.contains("files/insert_log/10/20/30/100/1.parquet"))
  }

  test(
    "validateSnapshotBucketForRelativeDataPaths accepts cross-bucket fully-qualified data paths"
  ) {
    MilvusScan.validateSnapshotBucketForRelativeDataPaths(
      "s3a://snapshot-bucket/files/snapshots/1/metadata/snapshot.json",
      Some("connector-bucket"),
      Seq(
        StorageV2ManifestItem(
          30L,
          "{\"ver\":7,\"base_path\":\"s3a://data-bucket/files/insert_log/10/20/30\"}"
        )
      ),
      Seq(
        V2SegmentInfo(
          segmentId = 30L,
          partitionId = 20L,
          numOfRows = 1L,
          storageVersion = 2L,
          columnGroups = Seq(
            V2ColumnGroup(
              fieldIds = Seq(100L),
              filePaths = Seq(
                "s3a://data-bucket/files/insert_log/10/20/30/100/1.parquet"
              ),
              fileRowCounts = Seq(1L)
            )
          )
        )
      )
    )
  }

  test("snapshotS3BucketForRelativePaths prefers snapshot bucket") {
    assert(
      MilvusScan.snapshotS3BucketForRelativePaths(
        "s3a://snapshot-bucket/files/snapshots/1/metadata/2.json",
        Map(Properties.FsConfig.FsBucketName -> "connector-bucket")
      ) == Some("snapshot-bucket")
    )
    assert(
      MilvusScan.snapshotS3BucketForRelativePaths(
        "files/snapshots/1/metadata/2.json",
        Map(Properties.FsConfig.FsBucketName -> "connector-bucket")
      ) == Some("connector-bucket")
    )
  }

  test("snapshotS3BucketForRelativePaths accepts connector bucket aliases") {
    assert(
      MilvusScan.snapshotS3BucketForRelativePaths(
        "files/snapshots/1/metadata/2.json",
        Map(MilvusOption.FsBucketName -> "connector-bucket")
      ) == Some("connector-bucket")
    )
    assert(
      MilvusScan.snapshotS3BucketForRelativePaths(
        "files/snapshots/1/metadata/2.json",
        Map(MilvusOption.S3BucketName -> "connector-bucket")
      ) == Some("connector-bucket")
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

  test("resolveConnectorS3Bucket trims configured bucket") {
    assert(
      MilvusScan.resolveConnectorS3Bucket(
        Map(Properties.FsConfig.FsBucketName -> " connector-bucket ")
      ) == "connector-bucket"
    )
  }

  test("resolveConnectorS3Bucket rejects missing or blank bucket") {
    Seq(Map.empty[String, String], Map(Properties.FsConfig.FsBucketName -> " "))
      .foreach { options =>
        val err = intercept[IllegalArgumentException] {
          MilvusScan.resolveConnectorS3Bucket(options)
        }
        assert(err.getMessage.contains(Properties.FsConfig.FsBucketName))
      }
  }

  test("buildSnapshotHadoopConf disables S3A FileSystem cache") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(Properties.FsConfig.FsBucketName, "connector-bucket")
    val conf = scanWithOptions(rawOptions).buildSnapshotHadoopConf(
      "s3a://connector-bucket/files/snapshots/1/metadata/2.json"
    )
    assert(conf.get("fs.s3a.impl.disable.cache") == "true")
  }

  test("buildSnapshotHadoopConf maps connector S3 options to S3A") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(Properties.FsConfig.FsBucketName, "connector-bucket")
    rawOptions.put(Properties.FsConfig.FsAddress, "minio:9000")
    rawOptions.put(Properties.FsConfig.FsAccessKeyId, "ak")
    rawOptions.put(Properties.FsConfig.FsAccessKeyValue, "sk")
    rawOptions.put(Properties.FsConfig.FsUseSSL, "false")
    rawOptions.put(Properties.FsConfig.FsRegion, "us-west-2")
    rawOptions.put(Properties.FsConfig.FsUseVirtualHost, "false")

    val conf = scanWithOptions(rawOptions).buildSnapshotHadoopConf(
      "s3a://snapshot-bucket/files/snapshots/1/metadata/2.json"
    )

    assert(conf.get("fs.s3a.endpoint") == "minio:9000")
    assert(conf.get("fs.s3a.connection.ssl.enabled") == "false")
    assert(conf.get("fs.s3a.path.style.access") == "true")
    assert(conf.get("fs.s3a.endpoint.region") == "us-west-2")
    assert(conf.get("fs.s3a.access.key") == "ak")
    assert(conf.get("fs.s3a.secret.key") == "sk")
    assert(
      conf.get("fs.s3a.aws.credentials.provider") ==
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    assert(conf.get("fs.s3a.bucket.connector-bucket.endpoint") == "minio:9000")
    assert(conf.get("fs.s3a.bucket.snapshot-bucket.endpoint") == "minio:9000")
    assert(
      conf.get("fs.s3a.bucket.snapshot-bucket.path.style.access") == "true"
    )
    assert(
      conf.get("fs.s3a.bucket.connector-bucket.aws.credentials.provider") ==
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    assert(
      conf.get("fs.s3a.bucket.snapshot-bucket.aws.credentials.provider") ==
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
  }

  test("buildSnapshotHadoopConf maps IAM mode without static credentials") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(Properties.FsConfig.FsBucketName, "connector-bucket")
    rawOptions.put(Properties.FsConfig.FsUseIam, "true")
    rawOptions.put(Properties.FsConfig.FsAccessKeyId, "ak")
    rawOptions.put(Properties.FsConfig.FsAccessKeyValue, "sk")

    val conf = scanWithOptions(rawOptions).buildSnapshotHadoopConf(
      "s3a://connector-bucket/files/snapshots/1/metadata/2.json"
    )

    assert(conf.get("fs.s3a.access.key") == null)
    assert(conf.get("fs.s3a.secret.key") == null)
    assert(
      conf.get("fs.s3a.aws.credentials.provider") ==
        "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    )
    assert(conf.get("fs.s3a.bucket.connector-bucket.access.key") == null)
    assert(conf.get("fs.s3a.bucket.connector-bucket.secret.key") == null)
    assert(
      conf.get("fs.s3a.bucket.connector-bucket.aws.credentials.provider") ==
        "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    )
  }

  test(
    "buildSnapshotHadoopConf accepts snapshot bucket without connector bucket"
  ) {
    val conf = scanWithOptions(new ju.HashMap[String, String]())
      .buildSnapshotHadoopConf(
        "s3a://snapshot-bucket/files/snapshots/1/metadata/2.json"
      )
    assert(conf.get("fs.s3a.impl.disable.cache") == "true")
  }

  test("readAllBytes closes the FileSystem instance when cache is disabled") {
    val rawOptions = new ju.HashMap[String, String]()
    val conf = new Configuration()
    conf.set(
      "fs.close-tracking.impl",
      classOf[CloseTrackingFileSystem].getName
    )
    conf.set("fs.close-tracking.impl.disable.cache", "true")
    val content = scanWithOptions(rawOptions).readAllBytes(
      conf,
      "close-tracking://bucket/snapshot.json"
    )
    assert(content == "{}")
    assert(CloseTrackingFileSystem.closeCount.get() == 1)
  }

  test("readAllBytes ignores cache close config for scheme-less paths") {
    val rawOptions = new ju.HashMap[String, String]()
    val conf = new Configuration()
    conf.set("fs.defaultFS", "close-tracking://bucket")
    conf.set(
      "fs.close-tracking.impl",
      classOf[CloseTrackingFileSystem].getName
    )
    conf.set("fs.close-tracking.impl.disable.cache", "true")
    val content = scanWithOptions(rawOptions).readAllBytes(
      conf,
      "/snapshot.json"
    )
    assert(content == "{}")
    assert(CloseTrackingFileSystem.closeCount.get() == 0)
    assert(conf.get("fs.null.impl.disable.cache") == null)
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
    "table schema emits canonical metadata extra column names from legacy aliases"
  ) {
    val schema = snapshotTableSchema(
      StructType(Seq(StructField("pk", LongType, nullable = false))),
      "partition,segment_id,row_offset"
    )

    assert(
      schema.fieldNames.toSeq == Seq(
        "pk",
        "partition",
        "$segment_id",
        "$row_offset"
      )
    )
  }

  test(
    "table schema rejects user field conflicting with canonical metadata column"
  ) {
    val err = intercept[IllegalArgumentException] {
      snapshotTableSchema(
        StructType(Seq(StructField("$segment_id", LongType, nullable = false))),
        "$segment_id"
      )
    }

    assert(err.getMessage.contains("$segment_id"))
    assert(err.getMessage.contains("metadata extra column"))
  }

  test(
    "table schema rejects legacy metadata aliases in provided schema"
  ) {
    val err = intercept[IllegalArgumentException] {
      snapshotTableSchema(
        StructType(Seq(StructField("segment_id", LongType, nullable = false))),
        "segment_id"
      )
    }

    assert(err.getMessage.contains("segment_id"))
    assert(err.getMessage.contains("legacy alias"))
    assert(err.getMessage.contains("$segment_id"))
  }

  test(
    "snapshot mode injects milvus.data_type metadata into provided external schema"
  ) {
    val schema = snapshotTableSchema(
      StructType(
        Seq(
          StructField("binary_vec", BinaryType, nullable = true),
          StructField("float_vec", ArrayType(FloatType), nullable = true),
          StructField("int8_vec", ArrayType(ShortType), nullable = true),
          StructField("json_payload", StringType, nullable = true)
        )
      ),
      "partition"
    )

    assert(
      schema("binary_vec").metadata.getLong(
        ArrowConverter.MilvusDataTypeMetadataKey
      ) == 100L
    )
    assert(
      schema("float_vec").metadata.getLong(
        ArrowConverter.MilvusDataTypeMetadataKey
      ) == 101L
    )
    assert(
      schema("int8_vec").metadata.getLong(
        ArrowConverter.MilvusDataTypeMetadataKey
      ) == 105L
    )
    assert(
      schema("binary_vec").metadata.getLong(
        ArrowConverter.MilvusVectorDimensionMetadataKey
      ) == 128L
    )
    assert(
      schema("float_vec").metadata.getLong(
        ArrowConverter.MilvusVectorDimensionMetadataKey
      ) == 4L
    )
    assert(
      schema("int8_vec").metadata.getLong(
        ArrowConverter.MilvusVectorDimensionMetadataKey
      ) == 4L
    )
    assert(
      schema("json_payload").metadata.getLong(
        ArrowConverter.MilvusDataTypeMetadataKey
      ) == 23L
    )
    assert(schema.fieldNames.toSeq.last == "partition")
  }

  test(
    "snapshot mode preserves caller metadata when injecting milvus.data_type"
  ) {
    val schema = snapshotTableSchema(
      StructType(
        Seq(
          StructField(
            "binary_vec",
            BinaryType,
            nullable = true,
            metadata = metadata("custom.flag" -> 7L)
          )
        )
      ),
      ""
    )

    assert(schema("binary_vec").metadata.getLong("custom.flag") == 7L)
    assert(
      schema("binary_vec").metadata.getLong(
        ArrowConverter.MilvusDataTypeMetadataKey
      ) == 100L
    )
    assert(
      schema("binary_vec").metadata.getLong(
        ArrowConverter.MilvusVectorDimensionMetadataKey
      ) == 128L
    )
  }

  test(
    "snapshot mode injects milvus.data_type metadata from snapshot schema bytes"
  ) {
    val schema = snapshotTableSchema(
      StructType(
        Seq(
          StructField("binary_vec", BinaryType, nullable = true),
          StructField("float_vec", ArrayType(FloatType), nullable = true),
          StructField("int8_vec", ArrayType(ShortType), nullable = true)
        )
      ),
      extraColumns = "",
      snapshotSchemaJson = None,
      snapshotSchemaBytes = Some(vectorSnapshotSchemaBytes)
    )

    assert(
      schema("binary_vec").metadata.getLong(
        ArrowConverter.MilvusDataTypeMetadataKey
      ) == 100L
    )
    assert(
      schema("float_vec").metadata.getLong(
        ArrowConverter.MilvusDataTypeMetadataKey
      ) == 101L
    )
    assert(
      schema("int8_vec").metadata.getLong(
        ArrowConverter.MilvusDataTypeMetadataKey
      ) == 105L
    )
    assert(
      schema("binary_vec").metadata.getLong(
        ArrowConverter.MilvusVectorDimensionMetadataKey
      ) == 128L
    )
    assert(
      schema("float_vec").metadata.getLong(
        ArrowConverter.MilvusVectorDimensionMetadataKey
      ) == 4L
    )
    assert(
      schema("int8_vec").metadata.getLong(
        ArrowConverter.MilvusVectorDimensionMetadataKey
      ) == 4L
    )
  }

  test("snapshot mode does not overwrite existing milvus.data_type") {
    val schema = snapshotTableSchema(
      StructType(
        Seq(
          StructField(
            "binary_vec",
            BinaryType,
            nullable = true,
            metadata = metadata(
              ArrowConverter.MilvusDataTypeMetadataKey -> 999L,
              "custom.flag" -> 7L
            )
          ),
          StructField("legacy_bytes", ArrayType(ByteType), nullable = true)
        )
      ),
      ""
    )

    assert(
      schema("binary_vec").metadata.getLong(
        ArrowConverter.MilvusDataTypeMetadataKey
      ) == 999L
    )
    assert(schema("binary_vec").metadata.getLong("custom.flag") == 7L)
    assert(
      !schema("binary_vec").metadata.contains(
        ArrowConverter.MilvusVectorDimensionMetadataKey
      )
    )
    assert(
      !schema("legacy_bytes").metadata.contains(
        ArrowConverter.MilvusDataTypeMetadataKey
      )
    )
  }

  test("snapshot mode fails loudly on malformed snapshot schema json") {
    val err = intercept[IllegalArgumentException] {
      snapshotTableSchema(
        StructType(Seq(StructField("binary_vec", BinaryType, nullable = true))),
        extraColumns = "",
        snapshotSchemaJson = Some("not-json"),
        snapshotSchemaBytes = None
      )
    }

    assert(err.getMessage.contains(MilvusOption.SnapshotSchemaJson))
    assert(err.getMessage.contains("Failed to parse"))
  }

  test("snapshot mode fails loudly on malformed snapshot schema bytes") {
    val err = intercept[IllegalArgumentException] {
      snapshotTableSchema(
        StructType(Seq(StructField("binary_vec", BinaryType, nullable = true))),
        extraColumns = "",
        snapshotSchemaJson = None,
        snapshotSchemaBytes = Some("not-base64%%")
      )
    }

    assert(err.getMessage.contains(MilvusOption.SnapshotSchemaBytes))
    assert(err.getMessage.contains("Failed to parse"))
  }

  test(
    "client-derived schema allows user fields named legacy metadata aliases"
  ) {
    val options = Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotManifests -> "[]",
      MilvusOption.SnapshotCollectionId -> "10",
      MilvusOption.MilvusCollectionName -> "c",
      MilvusOption.MilvusExtraColumns -> "segment_id,row_offset"
    )
    val collectionSchema = io.milvus.grpc.schema.CollectionSchema(
      name = "c",
      fields = Seq(
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 100,
          name = "segment_id",
          dataType = io.milvus.grpc.schema.DataType.Int64,
          nullable = false
        ),
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 101,
          name = "row_offset",
          dataType = io.milvus.grpc.schema.DataType.Int64,
          nullable = false
        )
      )
    )

    val schema = new MilvusTable(MilvusOption(options), None) {
      override def initInfo(): Unit = {
        milvusCollection = MilvusCollectionInfo(
          dbName = "",
          collectionName = "c",
          collectionID = 10L,
          schema = collectionSchema
        )
      }
    }.schema()

    assert(
      schema.fieldNames.toSeq == Seq(
        "RowID",
        "Timestamp",
        "segment_id",
        "row_offset",
        "$segment_id",
        "$row_offset"
      )
    )
  }

  test(
    "scan pruning preserves canonical metadata fields requested by legacy aliases"
  ) {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.MilvusExtraColumns, "segment_id,row_offset")
    val schema = StructType(
      Seq(
        StructField("pk", LongType, nullable = false),
        StructField("$segment_id", LongType, nullable = false),
        StructField("$row_offset", LongType, nullable = false)
      )
    )
    val builder = new MilvusScanBuilder(
      schema,
      new CaseInsensitiveStringMap(rawOptions)
    )

    builder.pruneColumns(
      StructType(Seq(StructField("pk", LongType, nullable = false)))
    )

    assert(
      builder.build().readSchema().fieldNames.toSeq == Seq(
        "pk",
        "$segment_id",
        "$row_offset"
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

  test("buildClientSnapshotOptions overrides relative-path bucket") {
    val out = MilvusScan.buildClientSnapshotOptions(
      baseOptions = Map(
        Properties.FsConfig.FsBucketName.toUpperCase -> "connector-bucket"
      ),
      collectionName = "snapshot_collection",
      collectionId = 10L,
      partitionIds = Seq(20L),
      schemaBytesBase64 = "abc",
      manifestList = Seq.empty,
      v2Segments = Seq.empty,
      snapshotBucketForRelativePaths = Some("snapshot-bucket")
    )
    assert(out(Properties.FsConfig.FsBucketName) == "snapshot-bucket")
  }

  test("snapshot option keys use dotted lowercase suffixes") {
    assert(
      MilvusOption.SnapshotMaxJsonBytes == "milvus.snapshot.max.json.bytes"
    )
    assert(
      MilvusOption.ClientSnapshotCompactionProtectionSeconds ==
        "milvus.client.snapshot.compaction.protection.seconds"
    )
    assert(
      MilvusOption.ClientSnapshotAutoCleanup ==
        "milvus.client.snapshot.auto.cleanup"
    )
  }

  test("clientSnapshotAutoCleanup defaults to true and parses false") {
    assert(
      MilvusOption.clientSnapshotAutoCleanup(Map.empty[String, String])
    )
    assert(
      !MilvusOption.clientSnapshotAutoCleanup(
        Map(MilvusOption.ClientSnapshotAutoCleanup -> "false")
      )
    )
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

  test("readAllBytes reuses positive long parser for max json bytes") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMaxJsonBytes, "not-a-number")
    val err = intercept[IllegalArgumentException] {
      scanWithOptions(rawOptions).readAllBytes(
        new Configuration(),
        "close-tracking://bucket/snapshot.json"
      )
    }
    assert(err.getMessage.contains(MilvusOption.SnapshotMaxJsonBytes))
  }

  test(
    "parseClientSnapshotCompactionProtectionSeconds rejects excessive values"
  ) {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(
      MilvusOption.ClientSnapshotCompactionProtectionSeconds,
      (8L * 24L * 60L * 60L).toString
    )
    val err = intercept[IllegalArgumentException] {
      MilvusScan.parseClientSnapshotCompactionProtectionSeconds(
        new CaseInsensitiveStringMap(rawOptions)
      )
    }
    assert(
      err.getMessage.contains(
        MilvusOption.ClientSnapshotCompactionProtectionSeconds
      )
    )
  }

  test("preserveResultWhenCloseFails keeps the original cleanup result") {
    val ok = MilvusScan.preserveResultWhenCloseFails(
      Success(()),
      throw new RuntimeException("close failed"),
      "test client"
    )
    assert(ok == Success(()))

    val original = new RuntimeException("drop failed")
    val failed = MilvusScan.preserveResultWhenCloseFails(
      Failure(original),
      (),
      "test client"
    )
    assert(failed == Failure(original))
  }

  test(
    "ensureClientSnapshotHasPackedSegments rejects filtered-empty snapshots"
  ) {
    val err = intercept[IllegalArgumentException] {
      MilvusScan.ensureClientSnapshotHasPackedSegments(
        Seq.empty,
        Seq.empty,
        "c"
      )
    }
    assert(err.getMessage.contains("No packed-parquet segments"))
    assert(err.getMessage.contains("c"))
  }

  test("generatedClientSnapshotName caps long collection names") {
    val name = MilvusScan.generatedClientSnapshotName(
      collectionName = "c" * 300,
      currentTimeMillis = 1L,
      uuid = "u" * 32
    )
    assert(name.length <= 255)
    assert(name.startsWith("spark_read_"))
    assert(name.endsWith("_1_" + "u" * 32))
  }

  test("generatedClientSnapshotName sanitizes collection names") {
    val name = MilvusScan.generatedClientSnapshotName(
      collectionName = "col-name.with unicode值",
      currentTimeMillis = 1L,
      uuid = "u" * 32
    )
    assert(name == s"spark_read_col_name_with_unicode__1_${"u" * 32}")
  }

  test("buildClientSnapshotOptions enables snapshot mode") {
    val out = MilvusScan.buildClientSnapshotOptions(
      baseOptions = Map(MilvusOption.SnapshotMode.toUpperCase -> "false"),
      collectionName = "snapshot_collection",
      collectionId = 10L,
      partitionIds = Seq(20L),
      schemaBytesBase64 = "abc",
      manifestList = Seq.empty,
      v2Segments = Seq.empty
    )
    assert(MilvusOption.isSnapshotMode(out))
    assert(!out.contains(MilvusOption.SnapshotMode.toUpperCase))
  }

  test("validateClientSnapshotMetadata rejects missing required fields") {
    val snapshotPath = "s3a://bucket/snapshot.json"
    val missingSnapshotInfo = SnapshotMetadata(
      snapshotInfo = null,
      collection = Collection(CollectionSchema("c", fields = Seq.empty))
    )
    val snapshotInfoErr = intercept[IllegalArgumentException] {
      MilvusScan.validateClientSnapshotMetadata(
        missingSnapshotInfo,
        snapshotPath
      )
    }
    assert(snapshotInfoErr.getMessage.contains("snapshot_info"))

    val missingCollection = SnapshotMetadata(
      snapshotInfo = SnapshotInfo("s"),
      collection = null
    )
    val collectionErr = intercept[IllegalArgumentException] {
      MilvusScan.validateClientSnapshotMetadata(missingCollection, snapshotPath)
    }
    assert(collectionErr.getMessage.contains("collection"))

    val missingSchema = SnapshotMetadata(
      snapshotInfo = SnapshotInfo("s"),
      collection = Collection(null)
    )
    val schemaErr = intercept[IllegalArgumentException] {
      MilvusScan.validateClientSnapshotMetadata(missingSchema, snapshotPath)
    }
    assert(schemaErr.getMessage.contains("collection.schema"))

    val emptySnapshot = SnapshotMetadata(
      snapshotInfo = SnapshotInfo("s"),
      collection = Collection(CollectionSchema("c", fields = Seq.empty)),
      manifestList = Seq.empty,
      storageV2ManifestList = Some(Seq.empty)
    )
    val emptyErr = intercept[IllegalArgumentException] {
      MilvusScan.validateClientSnapshotMetadata(emptySnapshot, snapshotPath)
    }
    assert(emptyErr.getMessage.contains("client snapshot is empty"))
    assert(emptyErr.getMessage.contains("no manifests and no V2 segments"))
  }

  test(
    "snapshot planner rejects explicit snapshot mode without segment hints"
  ) {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)
    val err = intercept[IllegalArgumentException] {
      scanWithOptions(rawOptions).planInputPartitions()
    }
    assert(err.getMessage.contains(MilvusOption.SnapshotManifests))
    assert(err.getMessage.contains(MilvusOption.SnapshotV2Segments))
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

  test("client snapshot fast path caches planned input partitions") {
    val clientOptions = new ju.HashMap[String, String]()
    clientOptions.put(MilvusOption.MilvusUri, "http://localhost:19530")
    clientOptions.put(MilvusOption.MilvusCollectionName, "c")
    assert(scanWithOptions(clientOptions).shouldCacheInputPartitions)

    val partitionScopedOptions = new ju.HashMap[String, String]()
    partitionScopedOptions.put(MilvusOption.MilvusUri, "http://localhost:19530")
    partitionScopedOptions.put(MilvusOption.MilvusCollectionName, "c")
    partitionScopedOptions.put(MilvusOption.MilvusPartitionName, "p1")
    assert(!scanWithOptions(partitionScopedOptions).shouldCacheInputPartitions)

    val snapshotOptions = new ju.HashMap[String, String]()
    snapshotOptions.put(MilvusOption.SnapshotMode, "true")
    snapshotOptions.put(MilvusOption.SnapshotManifests, "[]")
    assert(scanWithOptions(snapshotOptions).shouldCacheInputPartitions)
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
        ),
        StorageV2ManifestItem(
          31L,
          "{\"ver\":8,\"base_path\":\"files/insert_log/10/21/31\"}"
        )
      )
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, manifestJson)
    rawOptions.put(MilvusOption.SnapshotPartitionIds, "20,21")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)
    val partitions = scanWithOptions(rawOptions).planInputPartitions()
    assert(partitions.length == 2)
    val first = partitions(0).asInstanceOf[MilvusStorageV3InputPartition]
    val second = partitions(1).asInstanceOf[MilvusStorageV3InputPartition]
    assert(first.partitionName == "20")
    assert(first.segmentID == 30L)
    assert(first.readVersion == 7L)
    assert(second.partitionName == "21")
    assert(second.segmentID == 31L)
    assert(second.readVersion == 8L)
  }

  test(
    "snapshot planner falls back to default partition ID for unexpected V3 paths"
  ) {
    val manifestJson = MilvusSnapshotReader.serializeManifestList(
      Seq(
        StorageV2ManifestItem(
          30L,
          "{\"ver\":7,\"base_path\":\"files/unexpected/10/20/30\"}"
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

  test("snapshot planner attaches StorageV3 manifest delete plans") {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val deletePlan = MilvusDeletePlan.fromLongPks(Map(7L -> 100L))
    val partitions = scan.buildSnapshotPartitions(
      manifestList = Seq(
        StorageV2ManifestItem(
          30L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
        )
      ),
      defaultPartitionId = "20",
      schemaBytes = java.util.Base64.getDecoder.decode(emptySchemaBytes),
      v3DeletePlans = Map(30L -> deletePlan),
      v2Segments = Seq.empty,
      v2DeletePlans = Map.empty
    )

    val partition = partitions.head.asInstanceOf[MilvusStorageV3InputPartition]
    assert(partition.segmentID == 30L)
    assert(partition.deletePlan == deletePlan)
    assert(partition.applyDeletes)
  }

  test(
    "snapshot planner pins StorageV3 raw manifest path to resolved version"
  ) {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val partitions = scan.buildSnapshotPartitions(
      manifestList = Seq(
        StorageV2ManifestItem(
          30L,
          "files/insert_log/10/20/30"
        )
      ),
      defaultPartitionId = "20",
      schemaBytes = java.util.Base64.getDecoder.decode(emptySchemaBytes),
      v3ReadVersions = Map(30L -> 11L),
      v2Segments = Seq.empty,
      v2DeletePlans = Map.empty
    )

    val partition = partitions.head.asInstanceOf[MilvusStorageV3InputPartition]
    assert(partition.readVersion == 11L)
  }

  test("snapshot planner applies inherited L0 delete plans to StorageV3") {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val v3Plan = MilvusDeletePlan.fromLongPks(Map(7L -> 100L))
    val inheritedPlans = Map(
      -1L -> MilvusDeletePlan.fromLongPks(Map(8L -> 120L)),
      20L -> MilvusDeletePlan.fromLongPks(Map(9L -> 140L))
    )

    val partitions = scan.buildSnapshotPartitions(
      manifestList = Seq(
        StorageV2ManifestItem(
          30L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
        ),
        StorageV2ManifestItem(
          31L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/21/31\"}"
        )
      ),
      defaultPartitionId = "20",
      schemaBytes = java.util.Base64.getDecoder.decode(emptySchemaBytes),
      v3DeletePlans = Map(30L -> v3Plan),
      v2Segments = Seq.empty,
      v2DeletePlans = Map.empty,
      inheritedDeletePlansByPartition = inheritedPlans
    )

    val first = partitions(0).asInstanceOf[MilvusStorageV3InputPartition]
    val second = partitions(1).asInstanceOf[MilvusStorageV3InputPartition]
    assert(first.deletePlan.containsLongPk(7L, 50L))
    assert(first.deletePlan.containsLongPk(8L, 100L))
    assert(first.deletePlan.containsLongPk(9L, 130L))
    assert(second.deletePlan.containsLongPk(8L, 100L))
    assert(!second.deletePlan.containsLongPk(9L, 130L))
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

  test(
    "snapshot planner keeps inherited delete plan reference out of per-segment plan"
  ) {
    val inherited = MilvusDeletePlan.fromLongPks(Map(7L -> 100L))
    val segmentPlan = MilvusDeletePlan.fromLongPks(Map(9L -> 200L))
    val partition = MilvusPackedV2InputPartition(
      segmentID = 30L,
      partitionID = 20L,
      columnGroups = Seq(
        V2ColumnGroup(
          fieldIds = Seq(100L, 1L),
          filePaths = Seq("files/insert_log/10/20/30/100/1.parquet"),
          fileRowCounts = Seq(1L)
        )
      ),
      milvusSchemaBytes = java.util.Base64.getDecoder.decode(emptySchemaBytes),
      milvusOption = MilvusOption(
        new CaseInsensitiveStringMap(new ju.HashMap[String, String]())
      ),
      deletePlan = segmentPlan,
      inheritedDeletePlanPartitionId = Some(20L)
    )

    assert(partition.deletePlan == segmentPlan)
    assert(partition.inheritedDeletePlanPartitionId.contains(20L))
    assert(inherited.containsLongPk(7L, 50L))
  }

  test(
    "snapshot partition planning marks collection-wide L0 deletes for every partition"
  ) {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val inheritedPlans = Map(
      -1L -> MilvusDeletePlan.fromLongPks(Map(7L -> 100L)),
      20L -> MilvusDeletePlan.fromLongPks(Map(8L -> 120L))
    )
    val ownPlan = MilvusDeletePlan.fromLongPks(Map(9L -> 140L))

    val partitions = scan.buildSnapshotPartitions(
      manifestList = Seq.empty,
      defaultPartitionId = "20",
      schemaBytes = java.util.Base64.getDecoder.decode(emptySchemaBytes),
      v2Segments = Seq(
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
        ),
        V2SegmentInfo(
          segmentId = 31L,
          partitionId = 21L,
          numOfRows = 1L,
          storageVersion = 2L,
          columnGroups = Seq(
            V2ColumnGroup(
              fieldIds = Seq(100L),
              filePaths = Seq("files/insert_log/10/21/31/100/1.parquet"),
              fileRowCounts = Seq(1L)
            )
          )
        )
      ),
      v2DeletePlans = Map(30L -> ownPlan),
      inheritedDeletePlansByPartition = inheritedPlans
    )

    val first = partitions(0).asInstanceOf[MilvusPackedV2InputPartition]
    val second = partitions(1).asInstanceOf[MilvusPackedV2InputPartition]
    assert(first.deletePlan == ownPlan)
    assert(first.inheritedDeletePlanPartitionId.contains(20L))
    assert(second.deletePlan == MilvusDeletePlan.empty)
    assert(second.inheritedDeletePlanPartitionId.contains(21L))
  }

  test(
    "client snapshot partition planning inlines inherited L0 deletes into V2 partition plans"
  ) {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val inheritedPlans = Map(
      -1L -> MilvusDeletePlan.fromLongPks(Map(7L -> 100L)),
      20L -> MilvusDeletePlan.fromLongPks(Map(8L -> 120L))
    )
    val ownPlan = MilvusDeletePlan.fromLongPks(Map(9L -> 140L))

    val partitions = scan.buildSnapshotPartitions(
      manifestList = Seq.empty,
      defaultPartitionId = "20",
      schemaBytes = java.util.Base64.getDecoder.decode(emptySchemaBytes),
      v2Segments = Seq(
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
        ),
        V2SegmentInfo(
          segmentId = 31L,
          partitionId = 21L,
          numOfRows = 1L,
          storageVersion = 2L,
          columnGroups = Seq(
            V2ColumnGroup(
              fieldIds = Seq(100L),
              filePaths = Seq("files/insert_log/10/21/31/100/1.parquet"),
              fileRowCounts = Seq(1L)
            )
          )
        )
      ),
      v2DeletePlans = Map(30L -> ownPlan),
      inheritedDeletePlansByPartition = inheritedPlans,
      inlineInheritedDeletePlans = true
    )

    val first = partitions(0).asInstanceOf[MilvusPackedV2InputPartition]
    val second = partitions(1).asInstanceOf[MilvusPackedV2InputPartition]
    assert(first.inheritedDeletePlanPartitionId.isEmpty)
    assert(first.deletePlan.containsLongPk(7L, 50L))
    assert(first.deletePlan.containsLongPk(8L, 100L))
    assert(first.deletePlan.containsLongPk(9L, 130L))
    assert(second.inheritedDeletePlanPartitionId.isEmpty)
    assert(second.deletePlan.containsLongPk(7L, 50L))
    assert(!second.deletePlan.containsLongPk(8L, 100L))
  }
}

class CloseTrackingFileSystem extends FileSystem {
  private var uri: URI = _

  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    uri = name
  }

  override def getUri: URI = uri

  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    val bytes = "{}".getBytes(StandardCharsets.UTF_8)
    val in = new FSInputStream {
      private var pos = 0

      override def read(): Int = {
        if (pos >= bytes.length) -1
        else {
          val value = bytes(pos) & 0xff
          pos += 1
          value
        }
      }

      override def seek(newPos: Long): Unit = {
        pos = newPos.toInt
      }

      override def getPos: Long = pos

      override def seekToNewSource(targetPos: Long): Boolean = false
    }
    new FSDataInputStream(in)
  }

  override def close(): Unit = {
    CloseTrackingFileSystem.closeCount.incrementAndGet()
    super.close()
  }

  override def create(
      path: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable
  ): FSDataOutputStream = throw new UnsupportedOperationException

  override def append(
      path: Path,
      bufferSize: Int,
      progress: Progressable
  ): FSDataOutputStream = throw new UnsupportedOperationException

  override def rename(src: Path, dst: Path): Boolean =
    throw new UnsupportedOperationException

  override def delete(path: Path, recursive: Boolean): Boolean =
    throw new UnsupportedOperationException

  override def listStatus(path: Path): Array[FileStatus] =
    throw new UnsupportedOperationException

  override def setWorkingDirectory(path: Path): Unit = ()

  override def getWorkingDirectory: Path = new Path("/")

  override def mkdirs(path: Path, permission: FsPermission): Boolean = true

  override def getFileStatus(path: Path): FileStatus =
    new FileStatus(2L, false, 1, 2L, 0L, path)
}

object CloseTrackingFileSystem {
  val closeCount = new AtomicInteger(0)

  def reset(): Unit = closeCount.set(0)
}
