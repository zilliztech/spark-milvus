package com.zilliz.spark.connector.read

import java.{util => ju}
import java.io.IOException
import java.nio.file.{Files, Path}
import java.util.Comparator

import org.apache.hadoop.conf.Configuration
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

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.expr.{
  Comparison,
  ComparisonOperator,
  FieldRef,
  Literal
}
import com.zilliz.milvus.storage.io.{
  FailingObjectStore,
  FileInfo,
  LocalObjectStore,
  ObjectStore,
  ReadFixtures
}
import com.zilliz.milvus.storage.read.plan.{
  DeleteFileListing,
  DeleteSource,
  ReadLimits,
  SegmentReadTask
}
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.{
  DeleteFiles,
  DeltaLogFile,
  Segment,
  SegmentStatistics,
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin
}
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson,
  SnapshotJson
}
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.milvus.storage.stats.PrimaryKeyStats
import com.zilliz.spark.connector.options.{MilvusOption, StorageOptions}
import com.zilliz.spark.connector.table.MilvusTable
import com.zilliz.spark.connector.table.OptionStringsSnapshotSource
import io.milvus.grpc.schema.{
  CollectionSchema => ProtoSchema,
  DataType => MilvusDataType,
  FieldSchema
}

class MilvusScanSnapshotTest extends AnyFunSuite {
  private val emptySchemaBytes = java.util.Base64.getEncoder.encodeToString(
    io.milvus.grpc.schema.CollectionSchema(name = "c").toByteArray
  )

  /** Storage configuration is parsed and validated while partitions are planned
    * now, not per task, so a scan that never had a bucket fails before it
    * returns any partition. These suites are about planning, so the minimum a
    * remote store needs is filled in unless the case set it.
    */
  private def withStorageDefaults(
      rawOptions: ju.HashMap[String, String]
  ): ju.HashMap[String, String] = {
    val filled = new ju.HashMap[String, String](rawOptions)
    filled.putIfAbsent("fs.bucket_name", "test-bucket")
    filled.putIfAbsent("fs.address", "localhost:9000")
    // Only when the case did not bring its own credentials: setting use_iam on
    // top of static keys changes which provider the S3A mapping picks.
    if (!filled.containsKey("fs.access_key_id")) {
      filled.putIfAbsent("fs.use_iam", "true")
    }
    filled
  }

  private val rowIdSchema =
    StructType(Seq(StructField("RowID", LongType, nullable = false)))

  /** A scan over a given snapshot; the options only feed the context. */
  private def scanWithOptions(
      rawOptions: ju.HashMap[String, String],
      snapshot: Snapshot = null
  ): MilvusScan = {
    new MilvusScan(
      rowIdSchema,
      new CaseInsensitiveStringMap(withStorageDefaults(rawOptions)),
      if (snapshot == null) snapshotOf() else snapshot
    )
  }

  /** A scan whose snapshot comes from explicit V3 option strings, as a read
    * without `milvus.snapshot.path` resolves it.
    */
  private def scanFromOptions(
      rawOptions: ju.HashMap[String, String]
  ): MilvusScan = {
    val options = new CaseInsensitiveStringMap(withStorageDefaults(rawOptions))
    val snapshot = new OptionStringsSnapshotSource(MilvusOption(options))
      .snapshot()
      .fold(e => throw e, identity)
    new MilvusScan(rowIdSchema, options, snapshot)
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
      SnapshotJson
        .parse(vectorSnapshotSchemaJson)
        .toOption
        .get
        .collection
        .schema
        .toProtobufBytes
    )

  /** A snapshot from its parts, the way every planner sees one. */
  private def snapshotOf(
      v3: Seq[ManifestItemJson] = Seq.empty,
      segments: Seq[Segment] = Seq.empty,
      partitionIds: Seq[Long] = Seq(20L),
      bucket: String = "",
      schemaBytes: Array[Byte] =
        java.util.Base64.getDecoder.decode(emptySchemaBytes)
  ): Snapshot =
    SnapshotCatalog
      .fromLists(
        name = "t",
        collectionId = 10L,
        createdAt = None,
        partitionIds = partitionIds,
        schemaBytes = schemaBytes,
        v3Items = v3,
        bucket = bucket,
        origin = SnapshotOrigin.Options
      )
      .map(snapshot => snapshot.copy(segments = snapshot.segments ++ segments))
      .fold(e => throw e, identity)

  private def plannedSegment(
      id: Long,
      partitionId: Long,
      rows: Long,
      statistics: SegmentStatistics = SegmentStatistics.Unknown
  ): Segment = Segment(
    id = id,
    partitionId = partitionId,
    storageVersion = 3,
    rows = Some(rows),
    layout =
      SegmentLayout.Manifest(s"files/insert_log/10/$partitionId/$id", 1L),
    deletes = DeleteFiles.Empty,
    statistics = statistics
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
      snapshotSchemaBytes: Option[String] = None,
      selectedFieldIds: Option[String] = None
  ): StructType = {
    val options = scala.collection.mutable.Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotManifests -> "[]",
      MilvusOption.SnapshotCollectionId -> "10",
      MilvusOption.MilvusExtraColumns -> extraColumns
    )
    snapshotSchemaJson.foreach(json =>
      options += MilvusOption.SnapshotSchemaJson -> json
    )
    snapshotSchemaBytes.foreach(bytes =>
      options += MilvusOption.SnapshotSchemaBytes -> bytes
    )
    selectedFieldIds.foreach(ids =>
      options += MilvusOption.ReaderFieldIDs -> ids
    )
    val milvusOption = MilvusOption(options.toMap)
    val snapshot = new OptionStringsSnapshotSource(milvusOption)
      .snapshot()
      .fold(e => throw e, identity)
    MilvusTable(snapshot, milvusOption, Some(baseSchema)).schema()
  }

  test("snapshotBucket extracts authority when URI host is null") {
    assert(
      StorageOptions.snapshotBucket(
        "s3a://snapshot_bucket/files/snapshots/1/metadata/2.json"
      ) == Some("snapshot_bucket")
    )
  }

  test("snapshotBucket returns None for bucket-relative snapshot locations") {
    assert(
      StorageOptions.snapshotBucket("files/snapshots/1/metadata/2.json") == None
    )
  }

  // A gs:// location used to count as "no bucket", so the read opened the
  // local backend and read the key from the local root instead.
  test("snapshotBucket reads every object-store scheme and refuses others") {
    assert(
      StorageOptions.snapshotBucket("gs://a-bucket/files/snapshot.json") ==
        Some("a-bucket")
    )
    assert(
      StorageOptions.snapshotBucket("oss://o-bucket/files/snapshot.json") ==
        Some("o-bucket")
    )
    assert(StorageOptions.snapshotBucket("file:///data/backup/b1") == None)
    assert(
      StorageOptions.snapshotBucket(
        "https://s3.us-west-2.amazonaws.com/b/files/snapshot.json"
      ) == None
    )
    val unknown = intercept[IllegalArgumentException](
      StorageOptions.snapshotBucket("hdfs://namenode/files/snapshot.json")
    )
    assert(unknown.getMessage.contains("hdfs"))
  }

  test("snapshotMaxJsonBytes honors milvus.snapshot.max.json.bytes") {
    val withLimit = new ju.HashMap[String, String]()
    withLimit.put(MilvusOption.SnapshotMaxJsonBytes, "1048576")
    assert(
      StorageOptions.snapshotMaxJsonBytes(
        new CaseInsensitiveStringMap(withLimit)
      ) ==
        1048576L
    )
    assert(
      StorageOptions.snapshotMaxJsonBytes(
        new CaseInsensitiveStringMap(new ju.HashMap[String, String]())
      ) ==
        SnapshotJson.MaxBytes
    )
  }

  test("planning carries translated storage aliases into executor tasks") {
    val options = new ju.HashMap[String, String]()
    options.put(MilvusOption.S3Endpoint, "minio:9000")
    options.put(MilvusOption.S3PathStyleAccess, "true")
    options.put(StorageProperties.AccessKeyId, "ak")
    options.put(StorageProperties.AccessKeyValue, "sk")
    val scan = new MilvusScan(
      rowIdSchema,
      new CaseInsensitiveStringMap(options),
      snapshotOf(partitionIds = Seq(0L), bucket = "snapshot-bucket")
    )
    val segment = plannedSegment(
      id = 1L,
      partitionId = 0L,
      rows = 10L
    )

    val task = scan
      .inputPartitions(
        snapshotOf(
          segments = Seq(segment),
          partitionIds = Seq(0L),
          bucket = "snapshot-bucket"
        )
      )
      .head
      .asInstanceOf[MilvusInputPartition]
      .task

    assert(task.properties(StorageProperties.BucketName) == "snapshot-bucket")
    assert(task.properties(StorageProperties.Address) == "minio:9000")
    assert(task.properties(StorageProperties.UseVirtualHost) == "false")
    assert(task.properties(StorageProperties.AccessKeyId) == "ak")
  }

  test("planning carries the local storage mode into executor tasks") {
    val segment = plannedSegment(
      id = 1L,
      partitionId = 0L,
      rows = 1L
    )
    val scan = new MilvusScan(
      rowIdSchema,
      new CaseInsensitiveStringMap(new ju.HashMap[String, String]()),
      snapshotOf(
        segments = Seq(segment),
        partitionIds = Seq(0L),
        bucket = ""
      )
    )

    val task = scan
      .inputPartitions(scan.snapshot)
      .head
      .asInstanceOf[MilvusInputPartition]
      .task

    assert(
      task.properties(StorageProperties.StorageType) ==
        StorageProperties.StorageTypeLocal
    )
    assert(!task.properties.contains(StorageProperties.BucketName))
    assert(!task.properties.contains(StorageProperties.Address))
  }

  test("planning carries validated read limits into executor tasks") {
    val segment = plannedSegment(
      id = 1L,
      partitionId = 0L,
      rows = 1L
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.ReadBatchMaxRows, "2048")
    rawOptions.put(MilvusOption.ReadBatchMaxBytes, "16777216")
    rawOptions.put(MilvusOption.ReadArrowMaxBytes, "67108864")
    val snapshot = snapshotOf(
      segments = Seq(segment),
      partitionIds = Seq(0L),
      bucket = ""
    )

    val task = new MilvusScan(
      rowIdSchema,
      new CaseInsensitiveStringMap(rawOptions),
      snapshot
    ).inputPartitions(snapshot).head.asInstanceOf[MilvusInputPartition].task

    assert(task.limits == ReadLimits(2048, 16777216L, 67108864L))
  }

  test("snapshotS3BucketForRelativePaths prefers snapshot bucket") {
    assert(
      StorageOptions.snapshotS3BucketForRelativePaths(
        "s3a://snapshot-bucket/files/snapshots/1/metadata/2.json",
        Map(StorageProperties.BucketName -> "connector-bucket")
      ) == Some("snapshot-bucket")
    )
    assert(
      StorageOptions.snapshotS3BucketForRelativePaths(
        "files/snapshots/1/metadata/2.json",
        Map(StorageProperties.BucketName -> "connector-bucket")
      ) == Some("connector-bucket")
    )
  }

  test("snapshotS3BucketForRelativePaths accepts connector bucket aliases") {
    assert(
      StorageOptions.snapshotS3BucketForRelativePaths(
        "files/snapshots/1/metadata/2.json",
        Map(MilvusOption.FsBucketName -> "connector-bucket")
      ) == Some("connector-bucket")
    )
    assert(
      StorageOptions.snapshotS3BucketForRelativePaths(
        "files/snapshots/1/metadata/2.json",
        Map(MilvusOption.S3BucketName -> "connector-bucket")
      ) == Some("connector-bucket")
    )
  }

  test("minio snapshot locations use the configured bucket, not the endpoint") {
    val location =
      "minio://localhost:29000/spark-demo-live/files/snapshots/10/metadata/1.json"
    assert(StorageOptions.snapshotBucket(location).isEmpty)
    assert(
      StorageOptions.snapshotS3BucketForRelativePaths(
        location,
        Map(StorageProperties.BucketName -> "spark-demo-live")
      ) == Some("spark-demo-live")
    )
    assert(
      StorageOptions.snapshotBucketsToConfigure(location, "spark-demo-live") ==
        Seq("spark-demo-live")
    )
    assert(
      StorageOptions
        .snapshotS3BucketForRelativePaths(location, Map.empty)
        .isEmpty
    )
    val unknown = intercept[IllegalArgumentException](
      StorageOptions.snapshotBucket(
        "minioo://localhost:29000/spark-demo-live/files/snapshot.json"
      )
    )
    assert(
      unknown.getMessage.contains(
        "Unsupported storage location scheme 'minioo'"
      )
    )
  }

  test("snapshotBucketsToConfigure includes cross-bucket snapshot locations") {
    assert(
      StorageOptions.snapshotBucketsToConfigure(
        "s3a://snapshot-bucket/files/snapshots/1/metadata/2.json",
        "connector-bucket"
      ) == Seq("connector-bucket", "snapshot-bucket")
    )
    assert(
      StorageOptions.snapshotBucketsToConfigure(
        "s3a://connector-bucket/files/snapshots/1/metadata/2.json",
        "connector-bucket"
      ) == Seq("connector-bucket")
    )
  }

  test("buildSnapshotHadoopConf disables S3A FileSystem cache") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(StorageProperties.BucketName, "connector-bucket")
    val conf = scanWithOptions(rawOptions).hadoopConfFor(
      "s3a://connector-bucket/files/snapshots/1/metadata/2.json"
    )
    assert(conf.get("fs.s3a.impl.disable.cache") == "true")
  }

  test("buildSnapshotHadoopConf maps connector S3 options to S3A") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(StorageProperties.BucketName, "connector-bucket")
    rawOptions.put(StorageProperties.Address, "minio:9000")
    rawOptions.put(StorageProperties.AccessKeyId, "ak")
    rawOptions.put(StorageProperties.AccessKeyValue, "sk")
    rawOptions.put(StorageProperties.UseSSL, "false")
    rawOptions.put(StorageProperties.Region, "us-west-2")
    rawOptions.put(StorageProperties.UseVirtualHost, "false")

    val conf = scanWithOptions(rawOptions).hadoopConfFor(
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
    rawOptions.put(StorageProperties.BucketName, "connector-bucket")
    rawOptions.put(StorageProperties.UseIam, "true")
    rawOptions.put(StorageProperties.AccessKeyId, "ak")
    rawOptions.put(StorageProperties.AccessKeyValue, "sk")

    val conf = scanWithOptions(rawOptions).hadoopConfFor(
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

  test("legacy endpoint and path-style aliases configure Hadoop and native") {
    val options = Map(
      StorageProperties.BucketName -> "connector-bucket",
      MilvusOption.S3Endpoint -> "minio:9000",
      MilvusOption.S3PathStyleAccess -> "true"
    )
    val conf = StorageOptions.buildHadoopConfForOptions(
      options,
      "s3a://connector-bucket/files/snapshot.json"
    )

    assert(conf.get("fs.s3a.endpoint") == "minio:9000")
    assert(conf.get("fs.s3a.path.style.access") == "true")
    assert(
      conf.get("fs.s3a.bucket.connector-bucket.endpoint") == "minio:9000"
    )
    assert(
      conf.get("fs.s3a.bucket.connector-bucket.path.style.access") == "true"
    )

    val native = StorageOptions.storagePropertiesFor(
      new org.apache.hadoop.conf.Configuration(false),
      "connector-bucket",
      options
    )
    assert(native(StorageProperties.Address) == "minio:9000")
    assert(native(StorageProperties.UseVirtualHost) == "false")
  }

  test("canonical endpoint and path-style options win over legacy aliases") {
    val options = Map(
      StorageProperties.Address -> "canonical:9000",
      MilvusOption.S3Endpoint -> "legacy:9000",
      "fs.s3a.path.style.access" -> "false",
      MilvusOption.S3PathStyleAccess -> "true"
    )

    assert(StorageOptions.effectiveEndpoint(options).contains("canonical:9000"))
    assert(StorageOptions.effectivePathStyleAccess(options).contains(false))
  }

  test("Hadoop endpoint option is shared with native storage") {
    val options = Map("fs.s3a.endpoint" -> "hadoop-option:9000")

    assert(
      StorageOptions.effectiveEndpoint(options).contains("hadoop-option:9000")
    )
    val native = StorageOptions.storagePropertiesFor(
      new org.apache.hadoop.conf.Configuration(false),
      "connector-bucket",
      options
    )
    assert(native(StorageProperties.Address) == "hadoop-option:9000")
  }

  test("storage boolean options reject invalid values") {
    Seq(StorageProperties.UseSSL, StorageProperties.UseIam).foreach { key =>
      Seq("sometimes", "", "   ").foreach { value =>
        val error = intercept[IllegalArgumentException] {
          StorageOptions.buildHadoopConfForOptions(Map(key -> value), "")
        }
        assert(error.getMessage.contains(key))
      }
    }
    Seq(
      "fs.s3a.path.style.access",
      MilvusOption.S3PathStyleAccess,
      StorageProperties.UseVirtualHost
    ).foreach { key =>
      Seq("sometimes", "", "   ").foreach { value =>
        val pathStyleError = intercept[IllegalArgumentException] {
          StorageOptions.effectivePathStyleAccess(Map(key -> value))
        }
        assert(pathStyleError.getMessage.contains(key))
      }
    }
    val noFallback = intercept[IllegalArgumentException] {
      StorageOptions.effectivePathStyleAccess(
        Map(
          "fs.s3a.path.style.access" -> "   ",
          MilvusOption.S3PathStyleAccess -> "true"
        )
      )
    }
    assert(noFallback.getMessage.contains("fs.s3a.path.style.access"))
  }

  test(
    "buildSnapshotHadoopConf accepts snapshot bucket without connector bucket"
  ) {
    val conf = scanWithOptions(new ju.HashMap[String, String]()).hadoopConfFor(
      "s3a://snapshot-bucket/files/snapshots/1/metadata/2.json"
    )
    assert(conf.get("fs.s3a.impl.disable.cache") == "true")
  }

  test(
    "table schema emits canonical metadata extra column names from legacy aliases"
  ) {
    val schema = snapshotTableSchema(
      StructType(Seq(StructField("binary_vec", BinaryType, nullable = false))),
      "segment_id,row_offset,_timestamp"
    )

    assert(
      schema.fieldNames.toSeq == Seq(
        "binary_vec",
        "_segment_id",
        "_row_offset",
        "_timestamp"
      )
    )
    assert(
      schema("_timestamp").metadata.getLong(
        FieldMetadata.MilvusFieldIdMetadataKey
      ) == 1L
    )
  }

  test(
    "table schema rejects user field conflicting with canonical metadata column"
  ) {
    val err = intercept[IllegalArgumentException] {
      snapshotTableSchema(
        StructType(Seq(StructField("_segment_id", LongType, nullable = false))),
        "_segment_id"
      )
    }

    assert(err.getMessage.contains("_segment_id"))
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
    assert(err.getMessage.contains("_segment_id"))
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
      "_timestamp"
    )

    assert(
      schema("binary_vec").metadata.getLong(
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 100L
    )
    assert(
      schema("float_vec").metadata.getLong(
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 101L
    )
    assert(
      schema("int8_vec").metadata.getLong(
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 105L
    )
    assert(
      schema("binary_vec").metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      ) == 128L
    )
    assert(
      schema("float_vec").metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      ) == 4L
    )
    assert(
      schema("int8_vec").metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      ) == 4L
    )
    assert(
      schema("json_payload").metadata.getLong(
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 23L
    )
    assert(schema.fieldNames.toSeq.last == "_timestamp")
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
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 100L
    )
    assert(
      schema("binary_vec").metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      ) == 128L
    )
  }

  test("fieldIDs and an external schema must describe the same projection") {
    val selected = snapshotTableSchema(
      StructType(
        Seq(
          StructField("float_vec", ArrayType(FloatType), nullable = true),
          StructField("binary_vec", BinaryType, nullable = true)
        )
      ),
      extraColumns = "",
      selectedFieldIds = Some("101,100")
    )
    assert(selected.fieldNames.toSeq == Seq("float_vec", "binary_vec"))

    val error = intercept[IllegalArgumentException] {
      snapshotTableSchema(
        StructType(
          Seq(StructField("binary_vec", BinaryType, nullable = true))
        ),
        extraColumns = "",
        selectedFieldIds = Some("101")
      )
    }
    assert(error.getMessage.contains(MilvusOption.ReaderFieldIDs))
    assert(error.getMessage.contains("schema ids=100"))
  }

  test("schema inference applies fieldIDs in their requested order") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, vectorSnapshotSchemaBytes)
    rawOptions.put(MilvusOption.ReaderFieldIDs, "103,100")

    val schema = new com.zilliz.spark.connector.sources.MilvusDataSource()
      .inferSchema(new CaseInsensitiveStringMap(rawOptions))

    assert(schema.fieldNames.toSeq == Seq("json_payload", "binary_vec"))
    assert(
      schema.fields
        .map(
          _.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
        )
        .toSeq == Seq(103L, 100L)
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
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 100L
    )
    assert(
      schema("float_vec").metadata.getLong(
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 101L
    )
    assert(
      schema("int8_vec").metadata.getLong(
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 105L
    )
    assert(
      schema("binary_vec").metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      ) == 128L
    )
    assert(
      schema("float_vec").metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      ) == 4L
    )
    assert(
      schema("int8_vec").metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      ) == 4L
    )
  }

  test("snapshot mode rejects conflicting Milvus metadata") {
    val error = intercept[IllegalArgumentException] {
      snapshotTableSchema(
        StructType(
          Seq(
            StructField(
              "binary_vec",
              BinaryType,
              nullable = true,
              metadata = metadata(
                FieldMetadata.MilvusDataTypeMetadataKey -> 999L,
                "custom.flag" -> 7L
              )
            )
          )
        ),
        ""
      )
    }

    assert(error.getMessage.contains("binary_vec"))
    assert(error.getMessage.contains(FieldMetadata.MilvusDataTypeMetadataKey))
    assert(error.getMessage.contains("999"))
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
    "snapshot schema allows user fields named legacy metadata aliases"
  ) {
    val options = Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotManifests -> "[]",
      MilvusOption.SnapshotCollectionId -> "10",
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

    val schema = MilvusTable(
      snapshotOf(schemaBytes = collectionSchema.toByteArray),
      MilvusOption(options),
      None
    ).schema()

    assert(
      schema.fieldNames.toSeq == Seq(
        "segment_id",
        "row_offset",
        "_segment_id",
        "_row_offset"
      )
    )
  }

  test(
    "scan pruning preserves canonical metadata fields requested by legacy aliases"
  ) {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(
      MilvusOption.MilvusExtraColumns,
      "segment_id,row_offset,_timestamp"
    )
    val fieldId100 = metadata(FieldMetadata.MilvusFieldIdMetadataKey -> 100L)
    val fieldId1 = metadata(FieldMetadata.MilvusFieldIdMetadataKey -> 1L)
    val schema = StructType(
      Seq(
        StructField("pk", LongType, nullable = false, fieldId100),
        StructField("_segment_id", LongType, nullable = false),
        StructField("_row_offset", LongType, nullable = false),
        StructField("_timestamp", LongType, nullable = false, fieldId1)
      )
    )
    val builder = new MilvusScanBuilder(
      schema,
      new CaseInsensitiveStringMap(rawOptions),
      snapshotOf()
    )

    builder.pruneColumns(
      schema
    )

    assert(
      builder.build().readSchema().fieldNames.toSeq == Seq(
        "pk",
        "_segment_id",
        "_row_offset",
        "_timestamp"
      )
    )
  }

  test("reordered non-contiguous field ids reach every V3 task") {
    val collection = io.milvus.grpc.schema.CollectionSchema.parseFrom(
      java.util.Base64.getDecoder.decode(vectorSnapshotSchemaBytes)
    )
    val externalSchema = StructType(
      Seq(
        StructField("json_payload", StringType, nullable = true),
        StructField("binary_vec", BinaryType, nullable = true)
      )
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(
      StorageProperties.StorageType,
      StorageProperties.StorageTypeLocal
    )
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    val snapshot = snapshotOf(
      v3 = Seq(
        ManifestItemJson(
          31L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/31\"}"
        )
      ),
      segments = Seq(
        plannedSegment(
          id = 30L,
          partitionId = 20L,
          rows = 1L
        )
      ),
      schemaBytes = collection.toByteArray
    )
    val options = new CaseInsensitiveStringMap(rawOptions)
    val fullSchema = MilvusTable(
      snapshot,
      MilvusOption(options),
      Some(externalSchema)
    ).schema()
    val builder = new MilvusScanBuilder(
      fullSchema,
      options,
      snapshot
    )
    builder.pruneColumns(
      StructType(Seq(fullSchema("binary_vec"), fullSchema("json_payload")))
    )

    val scan = builder.build().asInstanceOf[MilvusScan]
    assert(
      scan.readSchema().fieldNames.toSeq == Seq("binary_vec", "json_payload")
    )
    val tasks = scan
      .inputPartitions(snapshot)
      .map(_.asInstanceOf[MilvusInputPartition].task)
    assert(tasks.length == 2)
    assert(tasks.forall(_.neededFieldIds == Seq(100L, 103L)))
  }

  test("V3 uses the bucket fixed by the snapshot") {
    val options = new ju.HashMap[String, String]()
    options.put(StorageProperties.BucketName, "configured-bucket")
    options.put(StorageProperties.Address, "localhost:9000")
    options.put(StorageProperties.UseIam, "true")
    val snapshot = snapshotOf(
      v3 = Seq(
        ManifestItemJson(
          30L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
        )
      ),
      bucket = "snapshot-bucket"
    )
    val task = new MilvusScan(
      rowIdSchema,
      new CaseInsensitiveStringMap(options),
      snapshot
    ).inputPartitions(snapshot).head.asInstanceOf[MilvusInputPartition].task

    assert(task.properties(StorageProperties.BucketName) == "snapshot-bucket")
  }

  test("scan builder accepts exact V2 predicates and returns residuals") {
    import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
    import org.apache.spark.sql.connector.expressions.filter.Predicate
    val schema = StructType(
      Seq(
        StructField(
          "score",
          LongType,
          nullable = true,
          metadata(
            FieldMetadata.MilvusFieldIdMetadataKey -> 100L,
            FieldMetadata.MilvusDataTypeMetadataKey ->
              io.milvus.grpc.schema.DataType.Int64.value.toLong
          )
        )
      )
    )
    def predicate(name: String, children: Expression*): Predicate =
      new Predicate(name, children.toArray)
    val accepted = predicate(
      ">",
      Expressions.column("score"),
      Expressions.literal(5L)
    )
    val contains = predicate(
      "CONTAINS",
      Expressions.column("score"),
      Expressions.literal(5L)
    )
    val missing = predicate(
      "=",
      Expressions.column("missing"),
      Expressions.literal(1L)
    )
    val predicates = Array(accepted, contains, missing)
    val segment = plannedSegment(
      id = 30L,
      partitionId = 20L,
      rows = 1L
    )
    val snapshots = Seq(
      "V3 synthetic" -> snapshotOf(segments = Seq(segment)),
      "V3" -> snapshotOf(v3 =
        Seq(
          ManifestItemJson(
            31L,
            "{\"ver\":1,\"base_path\":\"files/insert_log/10/20/31\"}"
          )
        )
      )
    )

    snapshots.foreach { case (layout, snapshot) =>
      val builder = new MilvusScanBuilder(
        schema,
        new CaseInsensitiveStringMap(new ju.HashMap[String, String]()),
        snapshot
      )
      assert(
        builder
          .pushPredicates(predicates)
          .sameElements(Array(contains, missing)),
        s"$layout residual predicates were not returned to Spark"
      )
      assert(
        builder.pushedPredicates().sameElements(Array(accepted)),
        s"$layout accepted predicate was not reported as pushed"
      )
    }
  }

  test(
    "runtime primary-key filters accumulate by intersection and are idempotent"
  ) {
    import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
    import org.apache.spark.sql.connector.expressions.filter.Predicate
    import com.zilliz.milvus.storage.stats.PrimaryKeyValue.LongValue

    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = io.milvus.grpc.schema.DataType.Int64,
          isPrimaryKey = true
        )
      )
    )
    val snapshot = snapshotOf(schemaBytes = collection.toByteArray)
    val options = new CaseInsensitiveStringMap(new ju.HashMap[String, String]())
    val fullSchema = MilvusTable(snapshot, MilvusOption(options), None).schema()
    def in(values: Long*): Predicate = new Predicate(
      "IN",
      (Seq[Expression](Expressions.column("id")) ++
        values.map(Expressions.literal)).toArray
    )
    val builder = new MilvusScanBuilder(fullSchema, options, snapshot)
    assert(builder.pushPredicates(Array(in(2L, 3L))).isEmpty)
    val scan = builder.build().asInstanceOf[MilvusScan]

    assert(
      scan.filterAttributes().map(_.fieldNames().mkString(".")).toSeq == Seq(
        "id"
      )
    )
    scan.filter(Array(in(1L, 2L)))
    assert(scan.currentPrimaryKeyFilter.get.values == Set(LongValue(2L)))
    scan.filter(Array(in(2L, 4L)))
    scan.filter(Array(in(2L, 4L)))
    assert(scan.currentPrimaryKeyFilter.get.values == Set(LongValue(2L)))

    val range = new Predicate(
      ">",
      Array[Expression](Expressions.column("id"), Expressions.literal(0L))
    )
    scan.filter(Array(range))
    assert(scan.currentPrimaryKeyFilter.get.values == Set(LongValue(2L)))
  }

  // Spark 4.2 resolves filterAttributes() against the scan output whenever it
  // plans the scan (SPARK-56467), so a primary key pruned out of readSchema()
  // failed every such query with "Unable to resolve id".
  test("the primary key is a runtime filter attribute only when it is read") {
    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = io.milvus.grpc.schema.DataType.Int64,
          isPrimaryKey = true
        ),
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 101L,
          name = "name",
          dataType = io.milvus.grpc.schema.DataType.VarChar
        )
      )
    )
    val snapshot = snapshotOf(schemaBytes = collection.toByteArray)
    val options = new CaseInsensitiveStringMap(new ju.HashMap[String, String]())
    val fullSchema = MilvusTable(snapshot, MilvusOption(options), None).schema()
    def attributes(columns: String*): Seq[String] = {
      val builder = new MilvusScanBuilder(fullSchema, options, snapshot)
      builder.pruneColumns(StructType(columns.map(fullSchema(_))))
      val scan = builder.build().asInstanceOf[MilvusScan]
      scan.filterAttributes().map(_.fieldNames().mkString(".")).toSeq
    }

    assert(attributes("name").isEmpty)
    assert(attributes().isEmpty)
    assert(attributes("id", "name") == Seq("id"))
  }

  test("runtime filters replan from cached Bloom statistics") {
    import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
    import org.apache.spark.sql.connector.expressions.filter.Predicate

    val primaryKey = io.milvus.grpc.schema.FieldSchema(
      fieldID = 100L,
      name = "id",
      dataType = io.milvus.grpc.schema.DataType.Int64,
      isPrimaryKey = true
    )
    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(primaryKey)
    )
    val values = Seq(101L, 202L, 303L)
    val directory = Files.createTempDirectory("scan-bloom-fixtures-")
    val stored =
      try {
        ReadFixtures.copyTo("stats/primary-key", directory)
        ReadFixtures.copyTo("segments/take-10", directory.resolve("v3"))
        val manifest = Files.readAllBytes(
          directory.resolve("v3/segment/_metadata/manifest-1.avro")
        )
        val files = scala.collection.mutable.Map(values.map { value =>
          s"stats/$value" -> Files.readAllBytes(
            directory.resolve(s"long-$value.json")
          )
        }: _*)
        values.indices.foreach { index =>
          files(
            s"files/insert_log/10/20/${30L + index}/_metadata/manifest-1.avro"
          ) = manifest
        }
        files
      } finally {
        val paths = Files.walk(directory)
        try
          paths
            .sorted(Comparator.reverseOrder[Path]())
            .forEach(path => Files.delete(path))
        finally paths.close()
      }
    values.foreach { value =>
      val stats = PrimaryKeyStats.fromBytes(stored(s"stats/$value")).head
      values.filterNot(_ == value).foreach { other =>
        assert(!stats.mightContainLong(other))
      }
    }
    var storeOpens = 0
    var statisticsReads = 0
    var manifestReads = 0

    val segments = values.zipWithIndex.map { case (value, index) =>
      plannedSegment(
        id = 30L + index,
        partitionId = 20L,
        rows = 10L,
        statistics = SegmentStatistics.Listed(
          Map(primaryKey.fieldID -> Seq(s"stats/$value"))
        )
      )
    }
    val snapshot = snapshotOf(
      segments = segments,
      schemaBytes = collection.toByteArray
    )
    val options = new CaseInsensitiveStringMap(
      new ju.HashMap[String, String]()
    )
    val fullSchema = MilvusTable(snapshot, MilvusOption(options), None).schema()
    val scan = new MilvusScan(fullSchema, options, snapshot) {
      override private[read] def openPlanningStore(): ObjectStore = {
        storeOpens += 1
        new ObjectStore {
          override def readAll(key: String): Array[Byte] = {
            if (key.startsWith("stats/")) statisticsReads += 1
            else manifestReads += 1
            stored(key)
          }
          override def size(key: String): Long = stored(key).length.toLong
          override def list(
              key: String,
              recursive: Boolean
          ): Seq[FileInfo] = Seq.empty
          override def exists(key: String): Boolean = stored.contains(key)
          override def readAt(
              key: String,
              offset: Long,
              length: Long,
              fileSize: Long
          ): Array[Byte] =
            stored(key).slice(offset.toInt, (offset + length).toInt)

          override def close(): Unit = ()
        }
      }
    }
    def in(selected: Long*): Predicate = new Predicate(
      "IN",
      (Seq[Expression](Expressions.column("id")) ++
        selected.map(Expressions.literal)).toArray
    )
    def plannedSegmentIds(): Seq[Long] =
      scan
        .planInputPartitions()
        .map(_.asInstanceOf[MilvusInputPartition].task.segmentId)
        .toSeq

    scan.filter(Array(in(101L, 202L)))
    assert(plannedSegmentIds() == Seq(30L, 31L))
    assert(storeOpens == 1)
    assert(statisticsReads == 3)
    assert(manifestReads == 3)

    stored.clear()
    scan.filter(Array(in(202L, 303L)))
    assert(plannedSegmentIds() == Seq(31L))
    assert(storeOpens == 1)
    assert(statisticsReads == 3)
    assert(manifestReads == 3)
    val cached = scan.planInputPartitions()
    scan.filter(Array(in(202L, 303L)))
    assert(scan.planInputPartitions() eq cached)
  }

  test(
    "Bloom paths use the store endpoint from global, bucket and explicit configuration"
  ) {
    val primaryKey = FieldSchema(
      fieldID = 100L,
      name = "id",
      dataType = MilvusDataType.Int64,
      isPrimaryKey = true
    )
    val collection = ProtoSchema(name = "t", fields = Seq(primaryKey))
    val directory = Files.createTempDirectory("scan-bloom-endpoint-")
    try {
      ReadFixtures.copyTo("stats/primary-key", directory.resolve("stats"))
      ReadFixtures.copyTo("segments/take-10", directory.resolve("v3"))
      val predicate = Comparison(
        FieldRef(100L, MilvusDataType.Int64),
        ComparisonOperator.EqualTo,
        Literal.IntegerValue(202L)
      )
      val configurations = Seq(
        (
          "global.example",
          Map("fs.s3a.endpoint" -> "global.example"),
          Map.empty[String, String]
        ),
        (
          "bucket.example",
          Map(
            "fs.s3a.endpoint" -> "other.example",
            "fs.s3a.bucket.test-bucket.endpoint" -> "bucket.example"
          ),
          Map.empty[String, String]
        ),
        (
          "explicit.example",
          Map(
            "fs.s3a.endpoint" -> "other.example",
            "fs.s3a.bucket.test-bucket.endpoint" -> "bucket.example"
          ),
          Map(StorageProperties.Address -> "explicit.example")
        )
      )
      configurations.foreach { case (endpoint, hadoop, declared) =>
        Seq(false, true).foreach { applyDeletes =>
          val segments = Seq(101L, 202L).zipWithIndex.map {
            case (value, index) =>
              plannedSegment(
                30L + index,
                20L,
                10L,
                SegmentStatistics.Listed(
                  Map(
                    100L -> Seq(
                      s"https://$endpoint/test-bucket/stats/long-$value.json"
                    )
                  )
                )
              ).copy(layout = SegmentLayout.Manifest("v3/segment", 1L))
          }
          val snapshot = snapshotOf(
            segments = segments,
            bucket = "test-bucket",
            schemaBytes = collection.toByteArray
          )
          val raw = new ju.HashMap[String, String]()
          raw.put(StorageProperties.UseIam, "true")
          raw.put(MilvusOption.ReadApplyDeletes, applyDeletes.toString)
          declared.foreach { case (key, value) => raw.put(key, value) }
          val options = new CaseInsensitiveStringMap(raw)
          val schema =
            MilvusTable(snapshot, MilvusOption(options), None).schema()
          val scan =
            new MilvusScan(schema, options, snapshot, Some(predicate)) {
              override private[read] def hadoopConfFor(
                  path: String
              ): Configuration = {
                val conf = new Configuration(false)
                hadoop.foreach { case (key, value) => conf.set(key, value) }
                conf
              }
              override private[read] def openPlanningStore(): ObjectStore =
                new LocalObjectStore(directory.toString)
            }
          assert(
            scan
              .planInputPartitions()
              .map(
                _.asInstanceOf[MilvusInputPartition].task.segmentId
              )
              .toSeq == Seq(31L),
            s"endpoint=$endpoint, applyDeletes=$applyDeletes"
          )
        }
      }
    } finally {
      val paths = Files.walk(directory)
      try
        paths
          .sorted(Comparator.reverseOrder[Path]())
          .forEach(path => Files.delete(path))
      finally paths.close()
    }
  }

  test(
    "planning-store initialization failures are optional only when deletes need no store"
  ) {
    val primaryKey = FieldSchema(
      fieldID = 100L,
      name = "id",
      dataType = MilvusDataType.Int64,
      isPrimaryKey = true
    )
    val collection = ProtoSchema(name = "t", fields = Seq(primaryKey))
    val snapshot = snapshotOf(
      segments = Seq(plannedSegment(30L, 20L, 1L)),
      schemaBytes = collection.toByteArray
    )
    Seq(false, true).foreach { applyDeletes =>
      Seq(
        new IOException("planning store unavailable"),
        new IllegalArgumentException("planning storage configuration invalid")
      ).foreach { failure =>
        val raw = new ju.HashMap[String, String]()
        raw.put(MilvusOption.ReadApplyDeletes, applyDeletes.toString)
        val options = new CaseInsensitiveStringMap(raw)
        val schema = MilvusTable(snapshot, MilvusOption(options), None).schema()
        val scan = new MilvusScan(
          schema,
          options,
          snapshot,
          Some(
            Comparison(
              FieldRef(100L, MilvusDataType.Int64),
              ComparisonOperator.EqualTo,
              Literal.IntegerValue(1L)
            )
          )
        ) {
          override private[read] def openPlanningStore(): ObjectStore =
            throw failure
        }
        if (applyDeletes) {
          assert(intercept[Exception](scan.planInputPartitions()) eq failure)
        } else {
          assert(
            scan
              .planInputPartitions()
              .map(
                _.asInstanceOf[MilvusInputPartition].task.segmentId
              )
              .toSeq == Seq(30L)
          )
        }
      }
    }
  }

  test("optional Bloom loading cannot hide a V3 delete-manifest failure") {
    import com.zilliz.milvus.storage.expr.{
      Comparison,
      ComparisonOperator,
      FieldRef,
      Literal
    }

    val primaryKey = io.milvus.grpc.schema.FieldSchema(
      fieldID = 100L,
      name = "id",
      dataType = io.milvus.grpc.schema.DataType.Int64,
      isPrimaryKey = true
    )
    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(primaryKey)
    )
    val segment = plannedSegment(
      id = 30L,
      partitionId = 20L,
      rows = 1L
    )
      .copy(
        deletes = DeleteFiles.Unknown,
        statistics = SegmentStatistics.Unknown
      )
    val snapshot = snapshotOf(
      segments = Seq(segment),
      schemaBytes = collection.toByteArray
    )
    val options = new CaseInsensitiveStringMap(
      new ju.HashMap[String, String]()
    )
    val schema = MilvusTable(snapshot, MilvusOption(options), None).schema()
    val scan = new MilvusScan(
      schema,
      options,
      snapshot,
      pushedExpression = Some(
        Comparison(
          FieldRef(100L, io.milvus.grpc.schema.DataType.Int64),
          ComparisonOperator.EqualTo,
          Literal.IntegerValue(1L)
        )
      ),
      planningSchema = schema
    ) {
      override private[read] def openPlanningStore(): ObjectStore =
        new FailingObjectStore(
          new IOException("required manifest is unreadable")
        )
    }

    val error = intercept[IllegalStateException](scan.planInputPartitions())
    assert(
      error.getMessage.contains("cannot list the delete files of segment 30")
    )
    assert(error.getMessage.contains("required manifest is unreadable"))
  }

  test("filter-only fields are read for either push and prune callback order") {
    import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
    import org.apache.spark.sql.connector.expressions.filter.Predicate
    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = io.milvus.grpc.schema.DataType.Int64,
          isPrimaryKey = true
        ),
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 101L,
          name = "score",
          dataType = io.milvus.grpc.schema.DataType.Int64
        )
      )
    )
    val snapshot = snapshotOf(
      v3 = Seq(
        ManifestItemJson(
          31L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/31\"}"
        )
      ),
      segments = Seq(
        plannedSegment(
          id = 30L,
          partitionId = 20L,
          rows = 1L
        )
      ),
      schemaBytes = collection.toByteArray
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(
      StorageProperties.StorageType,
      StorageProperties.StorageTypeLocal
    )
    val options = new CaseInsensitiveStringMap(rawOptions)
    val fullSchema = MilvusTable(
      snapshot,
      MilvusOption(options),
      None
    ).schema()
    val predicate = new Predicate(
      ">",
      Array[Expression](
        Expressions.column("score"),
        Expressions.literal(5L)
      )
    )

    Seq(true, false).foreach { pushFirst =>
      val builder = new MilvusScanBuilder(fullSchema, options, snapshot)
      if (pushFirst) {
        assert(builder.pushPredicates(Array(predicate)).isEmpty)
        builder.pruneColumns(StructType(Seq(fullSchema("id"))))
      } else {
        builder.pruneColumns(StructType(Seq(fullSchema("id"))))
        assert(builder.pushPredicates(Array(predicate)).isEmpty)
      }

      val scan = builder.build().asInstanceOf[MilvusScan]
      assert(scan.readSchema().fieldNames.toSeq == Seq("id"))
      val tasks = scan
        .inputPartitions(snapshot)
        .map(_.asInstanceOf[MilvusInputPartition].task)
      assert(tasks.length == 2)
      assert(tasks.forall(_.neededFieldIds == Seq(100L, 101L)))
    }
  }

  test("milvus filter fields are read without entering the output schema") {
    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = io.milvus.grpc.schema.DataType.Int64,
          isPrimaryKey = true
        ),
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 101L,
          name = "score",
          dataType = io.milvus.grpc.schema.DataType.Int64
        )
      )
    )
    val snapshot = snapshotOf(
      v3 = Seq(
        ManifestItemJson(
          31L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/31\"}"
        )
      ),
      segments = Seq(
        plannedSegment(
          id = 30L,
          partitionId = 20L,
          rows = 1L
        )
      ),
      schemaBytes = collection.toByteArray
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(
      StorageProperties.StorageType,
      StorageProperties.StorageTypeLocal
    )
    rawOptions.put(MilvusOption.MilvusFilter, "score > 5")
    val options = new CaseInsensitiveStringMap(rawOptions)
    val fullSchema = MilvusTable(snapshot, MilvusOption(options), None).schema()
    val builder = new MilvusScanBuilder(fullSchema, options, snapshot)

    builder.pruneColumns(StructType(Seq(fullSchema("id"))))

    val scan = builder.build().asInstanceOf[MilvusScan]
    assert(scan.readSchema().fieldNames.toSeq == Seq("id"))
    val partitions = scan
      .inputPartitions(snapshot)
      .map(_.asInstanceOf[MilvusInputPartition])
    assert(partitions.length == 2)
    assert(partitions.forall(_.task.neededFieldIds == Seq(100L, 101L)))
    val parsedFilter = MilvusOption(options).milvusFilter
    assert(parsedFilter.nonEmpty)
    assert(partitions.forall(_.milvusFilter == parsedFilter))
  }

  test("milvus filter preserves an unpruned all-fields native read") {
    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = io.milvus.grpc.schema.DataType.Int64
        ),
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 101L,
          name = "score",
          dataType = io.milvus.grpc.schema.DataType.Int64
        )
      )
    )
    val snapshot = snapshotOf(
      segments = Seq(
        plannedSegment(
          id = 30L,
          partitionId = 20L,
          rows = 1L
        )
      ),
      schemaBytes = collection.toByteArray
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(
      StorageProperties.StorageType,
      StorageProperties.StorageTypeLocal
    )
    rawOptions.put(MilvusOption.MilvusFilter, "score > 5")
    val options = new CaseInsensitiveStringMap(rawOptions)
    val fullSchema = MilvusTable(snapshot, MilvusOption(options), None).schema()
    val task = new MilvusScanBuilder(fullSchema, options, snapshot)
      .build()
      .asInstanceOf[MilvusScan]
      .inputPartitions(snapshot)
      .head
      .asInstanceOf[MilvusInputPartition]
      .task

    assert(task.neededFieldIds.isEmpty)
  }

  test("milvus filter widens an unpruned external schema") {
    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = io.milvus.grpc.schema.DataType.Int64
        ),
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 101L,
          name = "score",
          dataType = io.milvus.grpc.schema.DataType.Int64
        )
      )
    )
    val snapshot = snapshotOf(
      v3 = Seq(
        ManifestItemJson(
          31L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/31\"}"
        )
      ),
      segments = Seq(
        plannedSegment(
          id = 30L,
          partitionId = 20L,
          rows = 1L
        )
      ),
      schemaBytes = collection.toByteArray
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(
      StorageProperties.StorageType,
      StorageProperties.StorageTypeLocal
    )
    rawOptions.put(
      MilvusOption.SnapshotSchemaBytes,
      java.util.Base64.getEncoder.encodeToString(collection.toByteArray)
    )
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.MilvusFilter, "score > 5")
    val options = new CaseInsensitiveStringMap(rawOptions)
    val externalSchema = MilvusTable(
      snapshot,
      MilvusOption(options),
      Some(StructType(Seq(StructField("id", LongType, nullable = true))))
    ).schema()
    val scan = new MilvusScanBuilder(externalSchema, options, snapshot)
      .build()
      .asInstanceOf[MilvusScan]
    val tasks = scan
      .inputPartitions(snapshot)
      .map(_.asInstanceOf[MilvusInputPartition].task)

    assert(scan.readSchema().fieldNames.toSeq == Seq("id"))
    assert(tasks.length == 2)
    assert(tasks.forall(_.neededFieldIds == Seq(100L, 101L)))
  }

  test("milvus filter extends explicitly configured reader fields") {
    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = io.milvus.grpc.schema.DataType.Int64
        ),
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 101L,
          name = "score",
          dataType = io.milvus.grpc.schema.DataType.Int64
        )
      )
    )
    val snapshot = snapshotOf(
      segments = Seq(
        plannedSegment(
          id = 30L,
          partitionId = 20L,
          rows = 1L
        )
      ),
      schemaBytes = collection.toByteArray
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(
      StorageProperties.StorageType,
      StorageProperties.StorageTypeLocal
    )
    rawOptions.put(MilvusOption.ReaderFieldIDs, "100")
    rawOptions.put(MilvusOption.MilvusFilter, "score > 5")
    val options = new CaseInsensitiveStringMap(rawOptions)
    val fullSchema = MilvusTable(snapshot, MilvusOption(options), None).schema()
    val task = new MilvusScanBuilder(fullSchema, options, snapshot)
      .build()
      .asInstanceOf[MilvusScan]
      .inputPartitions(snapshot)
      .head
      .asInstanceOf[MilvusInputPartition]
      .task

    assert(task.neededFieldIds == Seq(100L, 101L))
  }

  test("milvus filter is validated against the fixed snapshot schema") {
    val collection = io.milvus.grpc.schema.CollectionSchema(
      name = "t",
      fields = Seq(
        io.milvus.grpc.schema.FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = io.milvus.grpc.schema.DataType.Int64
        )
      )
    )
    val snapshot = snapshotOf(schemaBytes = collection.toByteArray)
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.MilvusFilter, "missing == 1")

    val error = intercept[IllegalArgumentException] {
      new MilvusScanBuilder(
        MilvusTable(
          snapshot,
          MilvusOption(new CaseInsensitiveStringMap(rawOptions)),
          None
        ).schema(),
        new CaseInsensitiveStringMap(rawOptions),
        snapshot
      )
    }

    assert(error.getMessage.contains(MilvusOption.MilvusFilter))
    assert(error.getMessage.contains("Unknown filter field missing"))
  }

  test("snapshot option keys use dotted lowercase suffixes") {
    assert(
      MilvusOption.SnapshotMaxJsonBytes == "milvus.snapshot.max.json.bytes"
    )
    assert(MilvusOption.SnapshotPath == "milvus.snapshot.path")
  }

  test("parsePositiveLongOption rejects non-numeric and non-positive values") {
    Seq("not-a-number", "", "   ", "0", "-1").foreach { value =>
      val rawOptions = new ju.HashMap[String, String]()
      rawOptions.put(MilvusOption.SnapshotMaxJsonBytes, value)
      val err = intercept[IllegalArgumentException] {
        StorageOptions.parsePositiveLongOption(
          new CaseInsensitiveStringMap(rawOptions),
          MilvusOption.SnapshotMaxJsonBytes,
          86400L
        )
      }
      assert(err.getMessage.contains(MilvusOption.SnapshotMaxJsonBytes))
    }
  }

  test(
    "snapshot planner rejects explicit snapshot mode with neither segments nor a schema"
  ) {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    val err = intercept[IllegalArgumentException] {
      scanFromOptions(rawOptions).planInputPartitions()
    }
    assert(err.getMessage.contains(MilvusOption.SnapshotManifests))
    assert(err.getMessage.contains(MilvusOption.SnapshotSchemaBytes))
  }

  test("a schema alone is a snapshot with no segments: nothing to read") {
    // What a pure-connector write resolves its table from.
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)
    assert(scanFromOptions(rawOptions).planInputPartitions().isEmpty)
  }

  test("snapshot planner returns no partitions for empty snapshots") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, "[]")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)
    val scan = scanFromOptions(rawOptions)
    val firstPartitions = scan.planInputPartitions()
    val secondPartitions = scan.planInputPartitions()
    assert(firstPartitions.isEmpty)
    assert(firstPartitions eq secondPartitions)
  }

  test("a scan plans its input partitions once") {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    assert(scan.planInputPartitions() eq scan.planInputPartitions())
  }

  test("snapshot planner fails loudly on malformed manifest JSON") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.SnapshotMode, "true")
    rawOptions.put(MilvusOption.SnapshotManifests, "not-json")
    rawOptions.put(MilvusOption.SnapshotSchemaBytes, emptySchemaBytes)
    val err = intercept[Exception] {
      scanFromOptions(rawOptions).planInputPartitions()
    }
    assert(err.getMessage.contains("Failed to parse snapshot manifests"))
  }

  test(
    "nonempty explicit V3 options require a snapshot path before storage access"
  ) {
    val options = Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotSchemaBytes -> emptySchemaBytes,
      MilvusOption.SnapshotManifests -> SegmentListJson.encodeManifestItems(
        Seq(
          ManifestItemJson(
            30L,
            """{"ver":7,"base_path":"files/insert_log/10/20/30"}"""
          )
        )
      )
    )
    // No storage configuration is supplied: the incomplete snapshot must be
    // rejected before opening the store or loading native libraries.
    val result =
      new OptionStringsSnapshotSource(MilvusOption(options)).snapshot()
    assert(result.isLeft)
    val error = result.left.toOption.get
    assert(error.isInstanceOf[IllegalArgumentException])
    assert(
      error.getMessage.contains(
        s"Nonempty ${MilvusOption.SnapshotManifests} is not supported"
      )
    )
    assert(error.getMessage.contains(s"use ${MilvusOption.SnapshotPath}"))
  }

  test(
    "internal snapshot planning tags V3 partitions with partition ID string"
  ) {
    val snapshot = snapshotOf(
      v3 = Seq(
        ManifestItemJson(
          30L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
        ),
        ManifestItemJson(
          31L,
          "{\"ver\":8,\"base_path\":\"files/insert_log/10/21/31\"}"
        )
      ),
      partitionIds = Seq(20L, 21L)
    )
    val partitions = scanWithOptions(new ju.HashMap[String, String]())
      .inputPartitions(snapshot)
    assert(partitions.length == 2)
    val first = partitions(0).asInstanceOf[MilvusInputPartition]
    val second = partitions(1).asInstanceOf[MilvusInputPartition]
    assert(first.task.partitionId == 20L)
    assert(first.task.segmentId == 30L)
    assert(first.task.readVersionOrLatest == 7L)
    assert(second.task.partitionId == 21L)
    assert(second.task.segmentId == 31L)
    assert(second.task.readVersionOrLatest == 8L)
  }

  test(
    "internal snapshot planning falls back to default partition ID for unexpected V3 paths"
  ) {
    val snapshot = snapshotOf(
      v3 = Seq(
        ManifestItemJson(
          30L,
          "{\"ver\":7,\"base_path\":\"files/unexpected/10/20/30\"}"
        )
      ),
      partitionIds = Seq(20L, 21L)
    )
    val partitions = scanWithOptions(new ju.HashMap[String, String]())
      .inputPartitions(snapshot)
    assert(partitions.length == 1)
    val partition = partitions.head.asInstanceOf[MilvusInputPartition]
    assert(partition.task.partitionId == 20L)
    assert(partition.task.segmentId == 30L)
    assert(partition.task.readVersionOrLatest == 7L)
  }

  private def log(id: Long) = DeltaLogFile(id, s"files/delta/$id", 5L)

  test("snapshot planner attaches a StorageV3 segment's delete files") {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val partitions = scan.inputPartitions(
      snapshotOf(
        v3 = Seq(
          ManifestItemJson(
            30L,
            "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
          )
        ),
        partitionIds = Seq(20L)
      ),
      DeleteFileListing.empty.copy(v3BySegment = Map(30L -> Seq(log(3))))
    )

    val partition = partitions.head.asInstanceOf[MilvusInputPartition]
    assert(partition.task.segmentId == 30L)
    assert(partition.task.deletes == DeleteSource.Files(Seq(log(3))))
    assert(partition.task.appliesDeletes)
  }

  test(
    "internal snapshot planning pins an unresolved StorageV3 manifest version"
  ) {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val initial = snapshotOf(
      v3 = Seq(
        ManifestItemJson(
          30L,
          """{"ver":7,"base_path":"files/insert_log/10/20/30"}"""
        )
      ),
      partitionIds = Seq(20L)
    )
    val snapshot = initial.copy(segments =
      initial.segments.map(
        _.copy(
          layout = SegmentLayout.Manifest("files/insert_log/10/20/30", -1L)
        )
      )
    )
    val partitions = scan.inputPartitions(
      snapshot,
      DeleteFileListing.empty.copy(v3ReadVersions = Map(30L -> 11L))
    )

    val partition = partitions.head.asInstanceOf[MilvusInputPartition]
    assert(partition.task.readVersionOrLatest == 11L)
  }

  test(
    "snapshot planner hands StorageV3 segments the L0 delete files of their partition"
  ) {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val partitions = scan.inputPartitions(
      snapshotOf(
        v3 = Seq(
          ManifestItemJson(
            30L,
            "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
          ),
          ManifestItemJson(
            31L,
            "{\"ver\":7,\"base_path\":\"files/insert_log/10/21/31\"}"
          )
        ),
        partitionIds = Seq(20L)
      ),
      DeleteFileListing(
        v3BySegment = Map(30L -> Seq(log(3))),
        inheritedBySegment = Map(
          30L -> Seq(log(1), log(2)),
          31L -> Seq(log(1))
        ),
        v3ReadVersions = Map.empty
      )
    )

    val first = partitions(0).asInstanceOf[MilvusInputPartition]
    val second = partitions(1).asInstanceOf[MilvusInputPartition]
    assert(
      first.task.deletes == DeleteSource.Files(Seq(log(1), log(2), log(3)))
    )
    assert(second.task.deletes == DeleteSource.Files(Seq(log(1))))
  }

}
