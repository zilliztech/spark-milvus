package com.zilliz.spark.connector.read

import java.{util => ju}

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

import com.zilliz.milvus.client.api.MilvusCollectionInfo
import com.zilliz.milvus.storage.compat.backup.BackupMetaReader
import com.zilliz.milvus.storage.delete.MilvusDeletePlan
import com.zilliz.milvus.storage.read.plan.{DeleteSource, InputSpec}
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.{
  MilvusSnapshotReader,
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin,
  StorageV2ManifestItem,
  V2ColumnGroup,
  V2SegmentInfo
}
import com.zilliz.spark.connector.options.{
  BackupSelection,
  MilvusOption,
  StorageOptions
}
import com.zilliz.spark.connector.table.MilvusTable
import com.zilliz.spark.connector.read.plan.SnapshotPartitions
import com.zilliz.milvus.storage.snapshot.SegmentLayout

class MilvusScanClientSnapshotTest extends AnyFunSuite {
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

  private def scanWithOptions(
      rawOptions: ju.HashMap[String, String]
  ): MilvusScan = {
    new MilvusScan(
      StructType(Seq(StructField("RowID", LongType, nullable = false))),
      new CaseInsensitiveStringMap(withStorageDefaults(rawOptions))
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

  /** A snapshot from its parts, the way every planner sees one. */
  private def snapshotOf(
      v3: Seq[StorageV2ManifestItem] = Seq.empty,
      v2: Seq[V2SegmentInfo] = Seq.empty,
      partitionIds: Seq[Long] = Seq(20L)
  ): Snapshot =
    SnapshotCatalog
      .fromLists(
        name = "t",
        collectionId = 10L,
        createdAt = None,
        partitionIds = partitionIds,
        schemaBytes = java.util.Base64.getDecoder.decode(emptySchemaBytes),
        v3Items = v3,
        v2Segments = v2,
        bucket = "",
        origin = SnapshotOrigin.Options
      )
      .fold(e => throw e, identity)

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
      StorageOptions.resolveClientSnapshotLocation(
        "files/snapshots/1/metadata/2.json",
        "a-bucket"
      ) == "s3a://a-bucket/files/snapshots/1/metadata/2.json"
    )
  }

  test("resolveClientSnapshotLocation normalizes s3 scheme to s3a") {
    assert(
      StorageOptions.resolveClientSnapshotLocation(
        "s3://a-bucket/files/snapshots/1/metadata/2.json",
        "ignored"
      ) == "s3a://a-bucket/files/snapshots/1/metadata/2.json"
    )
  }

  test("resolveClientSnapshotLocation rejects unsupported schemes") {
    Seq("gs://a-bucket/files/snapshot.json", "file:///tmp/snapshot.json")
      .foreach { location =>
        val err = intercept[IllegalArgumentException] {
          StorageOptions.resolveClientSnapshotLocation(location, "ignored")
        }
        assert(
          err.getMessage.contains("Unsupported snapshot s3_location scheme")
        )
      }
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

  test("snapshotBucket returns None for unsupported (non-S3) schemes") {
    // Non-S3 locations carry no bucket to configure; explicit scheme
    // validation lives in resolveClientSnapshotLocation.
    assert(
      StorageOptions.snapshotBucket("gs://a-bucket/files/snapshot.json") == None
    )
    assert(StorageOptions.snapshotBucket("file:///data/backup/b1") == None)
  }

  test("backupMaxJsonBytes honors milvus.snapshot.max.json.bytes") {
    val withLimit = new ju.HashMap[String, String]()
    withLimit.put(MilvusOption.SnapshotMaxJsonBytes, "1048576")
    assert(
      StorageOptions.backupMaxJsonBytes(
        new CaseInsensitiveStringMap(withLimit)
      ) ==
        1048576L
    )
    assert(
      StorageOptions.backupMaxJsonBytes(
        new CaseInsensitiveStringMap(new ju.HashMap[String, String]())
      ) ==
        MilvusSnapshotReader.MaxSnapshotJsonBytes
    )
  }

  // A backup read takes its bucket from milvus.backup.dir, which is what
  // reference-cn.md promises, so fs.bucket_name is absent on purpose. Planning
  // used to validate the un-bucketed configuration before looking at what the
  // plan contained, and failed with "fs.bucket_name must be set" even though
  // every partition it produced used the derived bucket.
  //
  // The other planning suites go through withStorageDefaults, which fills
  // fs.bucket_name in; this case must not, or it tests nothing.
  test("backup planning derives its bucket and never reads the raw options") {
    val options = new ju.HashMap[String, String]()
    options.put(MilvusOption.BackupDir, "s3a://backup-bucket/backup/b1")
    options.put(MilvusOption.MilvusCollectionName, "demo")
    options.put("fs.address", "localhost:9000")
    options.put("fs.access_key_id", "ak")
    options.put("fs.access_key_value", "sk")
    assert(!options.containsKey("fs.bucket_name"))

    val scan = new MilvusScan(
      StructType(Seq(StructField("RowID", LongType, nullable = false))),
      new CaseInsensitiveStringMap(options)
    )

    val segment = V2SegmentInfo(
      segmentId = 1L,
      partitionId = 0L,
      numOfRows = 10L,
      storageVersion = 2L,
      columnGroups = Seq(
        V2ColumnGroup(
          fieldIds = Seq(100L),
          filePaths = Seq("backup/b1/binlogs/1/100/1"),
          fileRowCounts = Seq(10L)
        )
      )
    )

    val partitions = SnapshotPartitions.build(
      scan.ctx,
      snapshotOf(v2 = Seq(segment), partitionIds = Seq(0L)),
      forceCanonicalBucket = Some("backup-bucket")
    )

    assert(partitions.length == 1)
    val spec = partitions.head
      .asInstanceOf[
        com.zilliz.spark.connector.read.MilvusPackedV2InputPartition
      ]
      .spec
    assert(spec.properties("fs.bucket_name") == "backup-bucket")
  }

  test("backup createReaderFactory is self-contained without prior planning") {
    import com.fasterxml.jackson.databind.node.IntNode
    val schema = BackupMetaReader.BackupCollectionSchema(
      name = "demo",
      fields = Seq(
        BackupMetaReader.BackupFieldSchema(
          fieldId = 100L,
          name = "id",
          isPrimaryKey = true,
          rawDataType = Some(IntNode.valueOf(5))
        )
      )
    )
    val meta = BackupMetaReader.BackupInfo(
      name = "b1",
      collectionBackups = Seq(
        BackupMetaReader.CollectionBackup(
          collectionName = "demo",
          collectionId = 444L,
          schema = Some(schema)
        )
      )
    )
    val options = new ju.HashMap[String, String]()
    options.put(MilvusOption.BackupDir, "s3a://bucket/backup/b1")
    options.put(MilvusOption.MilvusCollectionName, "demo")
    val scan = new MilvusScan(
      StructType(Seq(StructField("RowID", LongType, nullable = false))),
      new CaseInsensitiveStringMap(options),
      preParsedBackupMeta = Some(meta)
    )
    // No prior planInputPartitions call: the factory must compute its delete
    // context on its own (no planning side effect).
    val factory = scan.createReaderFactory()
    assert(
      factory.isInstanceOf[
        com.zilliz.spark.connector.read.MilvusPartitionReaderFactory
      ]
    )
  }

  test(
    "backup createReaderFactory fails loudly when the meta re-read fails"
  ) {
    // preParsedBackupMeta = None (table init failed) and the factory's fallback
    // re-read also fails. The planner would have stamped inherited-delete
    // markers, so the factory must NOT hand an empty plan — it must fail loudly
    // rather than silently returning deleted rows.
    val options = new ju.HashMap[String, String]()
    options.put(MilvusOption.BackupDir, "/tmp/nonexistent-backup-xyz")
    options.put(MilvusOption.MilvusCollectionName, "demo")
    val scan = new MilvusScan(
      StructType(Seq(StructField("RowID", LongType, nullable = false))),
      new CaseInsensitiveStringMap(options),
      preParsedBackupMeta = None
    )
    val err = intercept[IllegalStateException] {
      scan.createReaderFactory()
    }
    assert(err.getMessage.contains("re-read backup meta"))
  }

  test("resolveBackupCollection matches by name and database, never .head") {
    def coll(name: String, id: Long, db: String = "") =
      BackupMetaReader.CollectionBackup(
        collectionName = name,
        collectionId = id,
        dbName = db
      )
    val multi = BackupMetaReader.BackupInfo(
      name = "b1",
      collectionBackups =
        Seq(coll("orders", 1L, "db1"), coll("orders", 2L, "db2"))
    )
    assert(
      BackupSelection
        .resolveBackupCollection(multi, "db1", "orders")
        .map(_.collectionId) ==
        Right(1L)
    )
    assert(
      BackupSelection
        .resolveBackupCollection(multi, "db2", "orders")
        .map(_.collectionId) ==
        Right(2L)
    )
    assert(BackupSelection.resolveBackupCollection(multi, "db1", "nope").isLeft)
    assert(BackupSelection.resolveBackupCollection(multi, "", "orders").isLeft)
    assert(BackupSelection.resolveBackupCollection(multi, "", "").isLeft)

    // "default" database is equivalent to an empty db_name (older backups omit
    // it), in both directions.
    val defaultDb = BackupMetaReader.BackupInfo(
      name = "d",
      collectionBackups = Seq(
        BackupMetaReader.CollectionBackup(
          collectionName = "orders",
          collectionId = 9L,
          dbName = "" // omitted by milvus-backup
        )
      )
    )
    assert(
      BackupSelection
        .resolveBackupCollection(defaultDb, "default", "orders")
        .map(_.collectionId) == Right(9L)
    )
    val namedDefault = BackupMetaReader.BackupInfo(
      name = "d2",
      collectionBackups = Seq(
        BackupMetaReader.CollectionBackup(
          collectionName = "orders",
          collectionId = 10L,
          dbName = "default"
        )
      )
    )
    assert(
      BackupSelection
        .resolveBackupCollection(namedDefault, "", "orders")
        .map(_.collectionId) == Right(10L)
    )

    // Explicit "default" selects the default-db candidate even when another
    // database has a same-named collection (request side is not flattened).
    val mixed = BackupMetaReader.BackupInfo(
      name = "mixed",
      collectionBackups = Seq(
        BackupMetaReader.CollectionBackup(
          collectionName = "orders",
          collectionId = 11L,
          dbName = "" // default
        ),
        BackupMetaReader.CollectionBackup(
          collectionName = "orders",
          collectionId = 12L,
          dbName = "db2"
        )
      )
    )
    assert(
      BackupSelection
        .resolveBackupCollection(mixed, "default", "orders")
        .map(_.collectionId) == Right(11L)
    )
    assert(
      BackupSelection
        .resolveBackupCollection(mixed, "db2", "orders")
        .map(_.collectionId) == Right(12L)
    )

    val single = BackupMetaReader.BackupInfo(
      name = "s",
      collectionBackups = Seq(coll("only", 3L))
    )
    assert(
      BackupSelection
        .resolveBackupCollection(single, "", "")
        .map(_.collectionId) ==
        Right(3L)
    )
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

  test("resolveConnectorS3Bucket trims configured bucket") {
    assert(
      StorageOptions.resolveConnectorS3Bucket(
        Map(StorageProperties.BucketName -> " connector-bucket ")
      ) == "connector-bucket"
    )
  }

  test("resolveConnectorS3Bucket rejects missing or blank bucket") {
    Seq(Map.empty[String, String], Map(StorageProperties.BucketName -> " "))
      .foreach { options =>
        val err = intercept[IllegalArgumentException] {
          StorageOptions.resolveConnectorS3Bucket(options)
        }
        assert(err.getMessage.contains(StorageProperties.BucketName))
      }
  }

  test("buildSnapshotHadoopConf disables S3A FileSystem cache") {
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(StorageProperties.BucketName, "connector-bucket")
    val conf = scanWithOptions(rawOptions).ctx.hadoopConf(
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

    val conf = scanWithOptions(rawOptions).ctx.hadoopConf(
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

    val conf = scanWithOptions(rawOptions).ctx.hadoopConf(
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
    val conf = scanWithOptions(new ju.HashMap[String, String]()).ctx.hadoopConf(
      "s3a://snapshot-bucket/files/snapshots/1/metadata/2.json"
    )
    assert(conf.get("fs.s3a.impl.disable.cache") == "true")
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
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 100L
    )
    assert(
      schema("binary_vec").metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
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

  test("snapshot mode does not overwrite existing milvus.data_type") {
    val schema = snapshotTableSchema(
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
          ),
          StructField("legacy_bytes", ArrayType(ByteType), nullable = true)
        )
      ),
      ""
    )

    assert(
      schema("binary_vec").metadata.getLong(
        FieldMetadata.MilvusDataTypeMetadataKey
      ) == 999L
    )
    assert(schema("binary_vec").metadata.getLong("custom.flag") == 7L)
    assert(
      !schema("binary_vec").metadata.contains(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      )
    )
    assert(
      !schema("legacy_bytes").metadata.contains(
        FieldMetadata.MilvusDataTypeMetadataKey
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

  test("snapshot option keys use dotted lowercase suffixes") {
    assert(
      MilvusOption.SnapshotMaxJsonBytes == "milvus.snapshot.max.json.bytes"
    )
    assert(MilvusOption.SnapshotPath == "milvus.snapshot.path")
    assert(MilvusOption.ClientSnapshotName == "milvus.client.snapshot.name")
  }

  test("parsePositiveLongOption rejects non-numeric and non-positive values") {
    Seq("not-a-number", "0", "-1").foreach { value =>
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

  test("every read mode caches its planned input partitions") {
    val clientOptions = new ju.HashMap[String, String]()
    clientOptions.put(MilvusOption.MilvusUri, "http://localhost:19530")
    clientOptions.put(MilvusOption.MilvusCollectionName, "c")
    assert(scanWithOptions(clientOptions).shouldCacheInputPartitions)

    val partitionScopedOptions = new ju.HashMap[String, String]()
    partitionScopedOptions.put(MilvusOption.MilvusUri, "http://localhost:19530")
    partitionScopedOptions.put(MilvusOption.MilvusCollectionName, "c")
    partitionScopedOptions.put(MilvusOption.MilvusPartitionName, "p1")
    assert(scanWithOptions(partitionScopedOptions).shouldCacheInputPartitions)

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
    assert(first.spec.segmentId == 30L)
    assert(first.spec.readVersionOrLatest == 7L)
    assert(second.partitionName == "21")
    assert(second.spec.segmentId == 31L)
    assert(second.spec.readVersionOrLatest == 8L)
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
    assert(partition.spec.segmentId == 30L)
    assert(partition.spec.readVersionOrLatest == 7L)
  }

  test("snapshot planner attaches StorageV3 manifest delete plans") {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val deletePlan = MilvusDeletePlan.fromLongPks(Map(7L -> 100L))
    val partitions = SnapshotPartitions.build(
      scan.ctx,
      snapshotOf(v3 = Seq(
        StorageV2ManifestItem(
          30L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
        )
      ), partitionIds = Seq(20L)),
      v3DeletePlans = Map(30L -> deletePlan)
    )

    val partition = partitions.head.asInstanceOf[MilvusStorageV3InputPartition]
    assert(partition.spec.segmentId == 30L)
    assert(partition.spec.deletePlan == deletePlan)
    assert(partition.spec.appliesDeletes)
  }

  test(
    "snapshot planner pins StorageV3 raw manifest path to resolved version"
  ) {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val partitions = SnapshotPartitions.build(
      scan.ctx,
      snapshotOf(v3 = Seq(
        StorageV2ManifestItem(
          30L,
          "files/insert_log/10/20/30"
        )
      ), partitionIds = Seq(20L)),
      v3ReadVersions = Map(30L -> 11L)
    )

    val partition = partitions.head.asInstanceOf[MilvusStorageV3InputPartition]
    assert(partition.spec.readVersionOrLatest == 11L)
  }

  test("snapshot planner applies inherited L0 delete plans to StorageV3") {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val v3Plan = MilvusDeletePlan.fromLongPks(Map(7L -> 100L))
    val inheritedPlans = Map(
      -1L -> MilvusDeletePlan.fromLongPks(Map(8L -> 120L)),
      20L -> MilvusDeletePlan.fromLongPks(Map(9L -> 140L))
    )

    val partitions = SnapshotPartitions.build(
      scan.ctx,
      snapshotOf(v3 = Seq(
        StorageV2ManifestItem(
          30L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
        ),
        StorageV2ManifestItem(
          31L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/21/31\"}"
        )
      ), partitionIds = Seq(20L)),
      v3DeletePlans = Map(30L -> v3Plan),
      inheritedDeletePlansByPartition = inheritedPlans
    )

    val first = partitions(0).asInstanceOf[MilvusStorageV3InputPartition]
    val second = partitions(1).asInstanceOf[MilvusStorageV3InputPartition]
    assert(first.spec.deletePlan.containsLongPk(7L, 50L))
    assert(first.spec.deletePlan.containsLongPk(8L, 100L))
    assert(first.spec.deletePlan.containsLongPk(9L, 130L))
    assert(second.spec.deletePlan.containsLongPk(8L, 100L))
    assert(!second.spec.deletePlan.containsLongPk(9L, 130L))
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
    assert(partition.spec.segmentId == 30L)
    assert(partition.spec.partitionId == 20L)
  }

  test(
    "snapshot planner keeps inherited delete plan reference out of per-segment plan"
  ) {
    val inherited = MilvusDeletePlan.fromLongPks(Map(7L -> 100L))
    val segmentPlan = MilvusDeletePlan.fromLongPks(Map(9L -> 200L))
    val partition = MilvusPackedV2InputPartition(
      InputSpec(
        segmentId = 30L,
        partitionId = 20L,
        layout = SegmentLayout.ColumnGroups(
          Seq(
            V2ColumnGroup(
              fieldIds = Seq(100L, 1L),
              filePaths = Seq("files/insert_log/10/20/30/100/1.parquet"),
              fileRowCounts = Seq(1L)
            )
          )
        ),
        schemaBytes = java.util.Base64.getDecoder.decode(emptySchemaBytes),
        properties = Map.empty,
        deletes = DeleteSource.Materialized(segmentPlan)
      ),
      MilvusOption(
        new CaseInsensitiveStringMap(new ju.HashMap[String, String]())
      ),
      inheritedDeletePlanPartitionId = Some(20L)
    )

    assert(partition.spec.deletePlan == segmentPlan)
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

    val partitions = SnapshotPartitions.build(
      scan.ctx,
      snapshotOf(v2 = Seq(
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
      ), partitionIds = Seq(20L)),
      v2DeletePlans = Map(30L -> ownPlan),
      inheritedDeletePlansByPartition = inheritedPlans
    )

    val first = partitions(0).asInstanceOf[MilvusPackedV2InputPartition]
    val second = partitions(1).asInstanceOf[MilvusPackedV2InputPartition]
    assert(first.spec.deletePlan == ownPlan)
    assert(first.inheritedDeletePlanPartitionId.contains(20L))
    assert(second.spec.deletePlan == MilvusDeletePlan.empty)
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

    val partitions = SnapshotPartitions.build(
      scan.ctx,
      snapshotOf(v2 = Seq(
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
      ), partitionIds = Seq(20L)),
      v2DeletePlans = Map(30L -> ownPlan),
      inheritedDeletePlansByPartition = inheritedPlans,
      inlineInheritedDeletePlans = true
    )

    val first = partitions(0).asInstanceOf[MilvusPackedV2InputPartition]
    val second = partitions(1).asInstanceOf[MilvusPackedV2InputPartition]
    assert(first.inheritedDeletePlanPartitionId.isEmpty)
    assert(first.spec.deletePlan.containsLongPk(7L, 50L))
    assert(first.spec.deletePlan.containsLongPk(8L, 100L))
    assert(first.spec.deletePlan.containsLongPk(9L, 130L))
    assert(second.inheritedDeletePlanPartitionId.isEmpty)
    assert(second.spec.deletePlan.containsLongPk(7L, 50L))
    assert(!second.spec.deletePlan.containsLongPk(8L, 100L))
  }

  test("snapshot planner dedups V2 column groups by slot before planning") {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    // A segment that went through add-field + backfill: the old multi-field
    // group (slot 3) still reports field 100 from its own schema, and the newer
    // single-field group (slot 100) reports it too. buildSnapshotPartitions
    // must strip the overlapping field from the older slot.
    val partitions = SnapshotPartitions.build(
      scan.ctx,
      snapshotOf(v2 = Seq(
        V2SegmentInfo(
          segmentId = 30L,
          partitionId = 20L,
          numOfRows = 2L,
          storageVersion = 2L,
          columnGroups = Seq(
            V2ColumnGroup(
              fieldIds = Seq(100L, 0L, 1L),
              filePaths = Seq("files/insert_log/10/20/30/3/1.parquet"),
              fileRowCounts = Seq(2L),
              slotFieldId = 3L
            ),
            V2ColumnGroup(
              fieldIds = Seq(100L),
              filePaths = Seq("files/insert_log/10/20/30/100/1.parquet"),
              fileRowCounts = Seq(2L),
              slotFieldId = 100L
            )
          )
        )
      ), partitionIds = Seq(20L)),
      inheritedDeletePlansByPartition = Map.empty,
      inlineInheritedDeletePlans = true
    )

    val partition = partitions.head.asInstanceOf[MilvusPackedV2InputPartition]
    val groups = partition.spec.layout match {
      case SegmentLayout.ColumnGroups(gs) => gs
      case other => fail(s"expected a column group layout, got $other")
    }
    assert(groups.size == 2)
    // Old slot keeps only its unique fields; the shared field 100 is read from
    // the newest slot.
    assert(groups.find(_.slotFieldId == 3L).get.fieldIds == Seq(0L, 1L))
    assert(groups.find(_.slotFieldId == 100L).get.fieldIds == Seq(100L))
  }
}

