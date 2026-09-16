package com.zilliz.spark.connector.read

import java.{util => ju}
import java.io.IOException

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
import com.zilliz.milvus.storage.compat.backup.BackupSnapshotSource
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.io.{FailingObjectStore, FileInfo, ObjectStore}
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
  SnapshotOrigin,
  V2ColumnGroup
}
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson,
  SnapshotJson
}
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.milvus.storage.stats.PrimaryKeyStats
import com.zilliz.spark.connector.options.{
  MilvusOption,
  OptionStringsSnapshotSource,
  StorageOptions
}
import com.zilliz.spark.connector.table.MilvusTable

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

  /** A scan whose snapshot comes from the 1.x option strings, as a read without
    * `milvus.snapshot.path` resolves it.
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
      v2: Seq[Segment] = Seq.empty,
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
        v2Segments = v2,
        bucket = bucket,
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
      snapshotSchemaBytes: Option[String] = None,
      selectedFieldIds: Option[String] = None
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
    selectedFieldIds.foreach(ids =>
      options += MilvusOption.ReaderFieldIDs -> ids
    )
    val milvusOption = MilvusOption(options.toMap)
    val snapshot = new OptionStringsSnapshotSource(milvusOption)
      .snapshot()
      .fold(e => throw e, identity)
    MilvusTable(snapshot, milvusOption, Some(baseSchema)).schema()
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

  test("resolveClientSnapshotLocation strips a Milvus endpoint authority") {
    assert(
      StorageOptions.resolveClientSnapshotLocation(
        "s3://minio:9000/a-bucket/files/snapshots/1/metadata/2.json",
        "a-bucket",
        "minio:9000"
      ) == "s3a://a-bucket/files/snapshots/1/metadata/2.json"
    )
    assert(
      StorageOptions.resolveClientSnapshotLocation(
        "s3://storage.internal/a-bucket/files/snapshots/1/metadata/2.json",
        "a-bucket",
        "https://storage.internal"
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
        SnapshotJson.MaxBytes
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
      rowIdSchema,
      new CaseInsensitiveStringMap(options),
      snapshotOf(partitionIds = Seq(0L), bucket = "backup-bucket")
    )

    val segment = Segment.v2(
      id = 1L,
      partitionId = 0L,
      rows = 10L,
      columnGroups = Seq(
        V2ColumnGroup(
          fieldIds = Seq(100L),
          filePaths = Seq("backup/b1/binlogs/1/100/1"),
          fileRowCounts = Seq(10L)
        )
      )
    )

    val partitions = scan.inputPartitions(
      snapshotOf(
        v2 = Seq(segment),
        partitionIds = Seq(0L),
        bucket = "backup-bucket"
      )
    )

    assert(partitions.length == 1)
    val task = partitions.head
      .asInstanceOf[
        com.zilliz.spark.connector.read.MilvusV2InputPartition
      ]
      .task
    assert(task.properties("fs.bucket_name") == "backup-bucket")
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
    val segment = Segment.v2(
      id = 1L,
      partitionId = 0L,
      rows = 10L,
      columnGroups = Seq(
        V2ColumnGroup(
          fieldIds = Seq(100L),
          filePaths = Seq("files/1.parquet"),
          fileRowCounts = Seq(10L)
        )
      )
    )

    val task = scan
      .inputPartitions(
        snapshotOf(
          v2 = Seq(segment),
          partitionIds = Seq(0L),
          bucket = "snapshot-bucket"
        )
      )
      .head
      .asInstanceOf[MilvusV2InputPartition]
      .task

    assert(task.properties(StorageProperties.BucketName) == "snapshot-bucket")
    assert(task.properties(StorageProperties.Address) == "minio:9000")
    assert(task.properties(StorageProperties.UseVirtualHost) == "false")
    assert(task.properties(StorageProperties.AccessKeyId) == "ak")
  }

  test("planning carries the local storage mode into executor tasks") {
    val segment = Segment.v2(
      id = 1L,
      partitionId = 0L,
      rows = 1L,
      columnGroups = Seq(
        V2ColumnGroup(
          fieldIds = Seq(100L),
          filePaths = Seq("/tmp/1.parquet"),
          fileRowCounts = Seq(1L)
        )
      )
    )
    val scan = new MilvusScan(
      rowIdSchema,
      new CaseInsensitiveStringMap(new ju.HashMap[String, String]()),
      snapshotOf(
        v2 = Seq(segment),
        partitionIds = Seq(0L),
        bucket = ""
      )
    )

    val task = scan
      .inputPartitions(scan.snapshot)
      .head
      .asInstanceOf[MilvusV2InputPartition]
      .task

    assert(
      task.properties(StorageProperties.StorageType) ==
        StorageProperties.StorageTypeLocal
    )
    assert(!task.properties.contains(StorageProperties.BucketName))
    assert(!task.properties.contains(StorageProperties.Address))
  }

  test("planning carries validated read limits into executor tasks") {
    val segment = Segment.v2(
      id = 1L,
      partitionId = 0L,
      rows = 1L,
      columnGroups = Seq(
        V2ColumnGroup(Seq(100L), Seq("/tmp/1.parquet"), Seq(1L))
      )
    )
    val rawOptions = new ju.HashMap[String, String]()
    rawOptions.put(MilvusOption.ReadBatchMaxRows, "2048")
    rawOptions.put(MilvusOption.ReadBatchMaxBytes, "16777216")
    rawOptions.put(MilvusOption.ReadArrowMaxBytes, "67108864")
    val snapshot = snapshotOf(
      v2 = Seq(segment),
      partitionIds = Seq(0L),
      bucket = ""
    )

    val task = new MilvusScan(
      rowIdSchema,
      new CaseInsensitiveStringMap(rawOptions),
      snapshot
    ).inputPartitions(snapshot).head.asInstanceOf[MilvusV2InputPartition].task

    assert(task.limits == ReadLimits(2048, 16777216L, 67108864L))
  }

  test("backup createReaderFactory is self-contained without prior planning") {
    val options = new ju.HashMap[String, String]()
    options.put(MilvusOption.BackupDir, "s3a://bucket/backup/b1")
    options.put(MilvusOption.MilvusCollectionName, "demo")
    val segment = Segment.v2(
      id = 1L,
      partitionId = 0L,
      rows = 10L,
      columnGroups = Seq(
        V2ColumnGroup(
          fieldIds = Seq(100L),
          filePaths = Seq("backup/b1/binlogs/1/100/1"),
          fileRowCounts = Seq(10L)
        )
      )
    )
    val scan = new MilvusScan(
      rowIdSchema,
      new CaseInsensitiveStringMap(options),
      snapshotOf(v2 = Seq(segment), partitionIds = Seq(0L), bucket = "bucket")
    )
    // No prior planInputPartitions call: the factory resolves its delete
    // context from the snapshot the scan holds.
    val factory = scan.createReaderFactory()
    assert(
      factory.isInstanceOf[
        com.zilliz.spark.connector.read.MilvusPartitionReaderFactory
      ]
    )
  }

  test("a backup whose meta cannot be read fails at the source, loudly") {
    val failure = new IOException("backup metadata is unavailable")
    val source = new BackupSnapshotSource(
      store = new FailingObjectStore(failure),
      backupDir = "/backup/b1",
      databaseName = "",
      collectionName = "demo",
      applyDeletes = true,
      maxJsonBytes = 1L << 20,
      withSegments = false
    )
    val result = source.snapshot()
    assert(result.isLeft)
    assert(result.left.get.getMessage.contains("backup meta"))
    assert(
      result.left.get.getMessage.contains("/backup/b1/meta/full_meta.json")
    )
    assert(result.left.get.getCause eq failure)
  }

  test("a backup read needs an object storage dir") {
    val source = new BackupSnapshotSource(
      store = new FailingObjectStore(
        new AssertionError("invalid backup URI must be rejected before reading")
      ),
      backupDir = "/backup/b1",
      databaseName = "",
      collectionName = "demo",
      applyDeletes = true,
      maxJsonBytes = 1L << 20,
      withSegments = true
    )
    val result = source.snapshot()
    assert(result.isLeft)
    assert(result.left.get.isInstanceOf[IllegalArgumentException])
    assert(result.left.get.getMessage.contains("object storage URI"))
    assert(result.left.get.getCause == null)
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
      BackupSnapshotSource
        .selectCollection(multi, "db1", "orders")
        .map(_.collectionId) ==
        Right(1L)
    )
    assert(
      BackupSnapshotSource
        .selectCollection(multi, "db2", "orders")
        .map(_.collectionId) ==
        Right(2L)
    )
    assert(BackupSnapshotSource.selectCollection(multi, "db1", "nope").isLeft)
    assert(BackupSnapshotSource.selectCollection(multi, "", "orders").isLeft)
    assert(BackupSnapshotSource.selectCollection(multi, "", "").isLeft)

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
      BackupSnapshotSource
        .selectCollection(defaultDb, "default", "orders")
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
      BackupSnapshotSource
        .selectCollection(namedDefault, "", "orders")
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
      BackupSnapshotSource
        .selectCollection(mixed, "default", "orders")
        .map(_.collectionId) == Right(11L)
    )
    assert(
      BackupSnapshotSource
        .selectCollection(mixed, "db2", "orders")
        .map(_.collectionId) == Right(12L)
    )

    val single = BackupMetaReader.BackupInfo(
      name = "s",
      collectionBackups = Seq(coll("only", 3L))
    )
    assert(
      BackupSnapshotSource
        .selectCollection(single, "", "")
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

  test("reordered non-contiguous field ids reach every storage-line task") {
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
      v2 = Seq(
        Segment.v2(
          id = 30L,
          partitionId = 20L,
          rows = 1L,
          columnGroups = Seq(
            V2ColumnGroup(
              fieldIds = Seq(100L, 103L),
              filePaths = Seq("files/segment.parquet"),
              fileRowCounts = Seq(1L)
            )
          )
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

  test("every storage line uses the bucket fixed by the snapshot") {
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
    ).inputPartitions(snapshot).head.asInstanceOf[MilvusV3InputPartition].task

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
    val v2 = Segment.v2(
      id = 30L,
      partitionId = 20L,
      rows = 1L,
      columnGroups = Seq(
        V2ColumnGroup(
          Seq(100L),
          Seq("files/insert_log/10/20/30/100/1"),
          Seq(1L)
        )
      )
    )
    val snapshots = Seq(
      "V2" -> snapshotOf(v2 = Seq(v2)),
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

  test("vector search leaves every V2 predicate in Spark") {
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
    val options = new ju.HashMap[String, String]()
    options.put(MilvusOption.VectorSearchQueryVector, "[1.0]")
    options.put(MilvusOption.VectorSearchTopK, "1")
    val builder = new MilvusScanBuilder(
      schema,
      new CaseInsensitiveStringMap(options),
      snapshotOf()
    )
    val predicate = new Predicate(
      ">",
      Array[Expression](
        Expressions.column("score"),
        Expressions.literal(5L)
      )
    )

    assert(
      builder.pushPredicates(Array(predicate)).sameElements(Array(predicate))
    )
    assert(builder.pushedPredicates().isEmpty)
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
    val statistics = values.map { value =>
      val builder = new PrimaryKeyStats.Builder(
        primaryKey.fieldID,
        primaryKey.dataType
      )
      builder.addLong(value)
      value -> builder.build()
    }
    statistics.foreach { case (value, stats) =>
      values.filterNot(_ == value).foreach { other =>
        assert(!stats.mightContainLong(other))
      }
    }
    val stored = scala.collection.mutable.Map(
      statistics.map { case (value, stats) =>
        s"stats/$value" -> stats.toBytes
      }: _*
    )
    var storeOpens = 0
    var statisticsReads = 0

    val segments = values.zipWithIndex.map { case (value, index) =>
      Segment.v2(
        id = 30L + index,
        partitionId = 20L,
        rows = 1L,
        columnGroups = Seq(
          V2ColumnGroup(
            Seq(primaryKey.fieldID),
            Seq(s"data/${30L + index}"),
            Seq(1L)
          )
        ),
        statistics = SegmentStatistics.Listed(
          Map(primaryKey.fieldID -> Seq(s"stats/$value"))
        )
      )
    }
    val snapshot = snapshotOf(
      v2 = segments,
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
            statisticsReads += 1
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
          override def write(key: String, data: Array[Byte]): Unit =
            throw new UnsupportedOperationException("read-only test store")
          override def createDir(key: String, recursive: Boolean): Unit =
            throw new UnsupportedOperationException("read-only test store")
          override def delete(key: String): Unit = stored.remove(key)
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

    stored.clear()
    scan.filter(Array(in(202L, 303L)))
    assert(plannedSegmentIds() == Seq(31L))
    assert(storeOpens == 1)
    assert(statisticsReads == 3)
    val cached = scan.planInputPartitions()
    scan.filter(Array(in(202L, 303L)))
    assert(scan.planInputPartitions() eq cached)
  }

  test("optional Bloom loading cannot hide an invalid V2 delete contract") {
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
    val segment = Segment
      .v2(
        id = 30L,
        partitionId = 20L,
        rows = 1L,
        columnGroups = Seq(
          V2ColumnGroup(Seq(100L), Seq("data/30"), Seq(1L))
        )
      )
      .copy(
        deletes = DeleteFiles.Unknown,
        statistics = SegmentStatistics.Unknown
      )
    val snapshot = snapshotOf(
      v2 = Seq(segment),
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
    )

    val error = intercept[IllegalStateException](scan.planInputPartitions())
    assert(error.getMessage.contains("delete-file state is unknown"))
  }

  test("vector TopK scans do not advertise or accept runtime filtering") {
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
          name = "vector",
          dataType = io.milvus.grpc.schema.DataType.FloatVector,
          typeParams = Seq(io.milvus.grpc.common.KeyValuePair("dim", "1"))
        )
      )
    )
    val snapshot = snapshotOf(schemaBytes = collection.toByteArray)
    val raw = new ju.HashMap[String, String]()
    raw.put(MilvusOption.VectorSearchQueryVector, "[1.0]")
    raw.put(MilvusOption.VectorSearchTopK, "1")
    val options = new CaseInsensitiveStringMap(raw)
    val fullSchema = MilvusTable(snapshot, MilvusOption(options), None).schema()
    val scan = new MilvusScanBuilder(fullSchema, options, snapshot)
      .build()
      .asInstanceOf[MilvusScan]
    val runtime = new Predicate(
      "=",
      Array[Expression](Expressions.column("id"), Expressions.literal(1L))
    )

    assert(scan.filterAttributes().isEmpty)
    scan.filter(Array(runtime))
    assert(scan.currentPrimaryKeyFilter.isEmpty)
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
      v2 = Seq(
        Segment.v2(
          id = 30L,
          partitionId = 20L,
          rows = 1L,
          columnGroups = Seq(
            V2ColumnGroup(
              Seq(100L, 101L),
              Seq("files/segment.parquet"),
              Seq(1L)
            )
          )
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

  test("snapshot option keys use dotted lowercase suffixes") {
    assert(
      MilvusOption.SnapshotMaxJsonBytes == "milvus.snapshot.max.json.bytes"
    )
    assert(MilvusOption.SnapshotPath == "milvus.snapshot.path")
    assert(MilvusOption.ClientSnapshotName == "milvus.client.snapshot.name")
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
    assert(err.getMessage.contains(MilvusOption.SnapshotV2Segments))
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

  test("snapshot planner tags V3 partitions with partition ID string") {
    val manifestJson = SegmentListJson.encodeManifestItems(
      Seq(
        ManifestItemJson(
          30L,
          "{\"ver\":7,\"base_path\":\"files/insert_log/10/20/30\"}"
        ),
        ManifestItemJson(
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
    val partitions = scanFromOptions(rawOptions).planInputPartitions()
    assert(partitions.length == 2)
    val first = partitions(0).asInstanceOf[MilvusV3InputPartition]
    val second = partitions(1).asInstanceOf[MilvusV3InputPartition]
    assert(first.partitionName == "20")
    assert(first.task.segmentId == 30L)
    assert(first.task.readVersionOrLatest == 7L)
    assert(second.partitionName == "21")
    assert(second.task.segmentId == 31L)
    assert(second.task.readVersionOrLatest == 8L)
  }

  test(
    "snapshot planner falls back to default partition ID for unexpected V3 paths"
  ) {
    val manifestJson = SegmentListJson.encodeManifestItems(
      Seq(
        ManifestItemJson(
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
    val partitions = scanFromOptions(rawOptions).planInputPartitions()
    assert(partitions.length == 1)
    val partition = partitions.head.asInstanceOf[MilvusV3InputPartition]
    assert(partition.partitionName == "20")
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

    val partition = partitions.head.asInstanceOf[MilvusV3InputPartition]
    assert(partition.task.segmentId == 30L)
    assert(partition.task.deletes == DeleteSource.Files(Seq(log(3))))
    assert(partition.task.appliesDeletes)
  }

  test(
    "snapshot planner pins StorageV3 raw manifest path to resolved version"
  ) {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val partitions = scan.inputPartitions(
      snapshotOf(
        v3 = Seq(
          ManifestItemJson(
            30L,
            "files/insert_log/10/20/30"
          )
        ),
        partitionIds = Seq(20L)
      ),
      DeleteFileListing.empty.copy(v3ReadVersions = Map(30L -> 11L))
    )

    val partition = partitions.head.asInstanceOf[MilvusV3InputPartition]
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
        v2BySegment = Map.empty,
        inheritedByPartition = Map(-1L -> Seq(log(1)), 20L -> Seq(log(2))),
        v3ReadVersions = Map.empty
      )
    )

    val first = partitions(0).asInstanceOf[MilvusV3InputPartition]
    val second = partitions(1).asInstanceOf[MilvusV3InputPartition]
    assert(
      first.task.deletes == DeleteSource.Files(Seq(log(1), log(2), log(3)))
    )
    assert(second.task.deletes == DeleteSource.Files(Seq(log(1))))
  }

  test("snapshot planner accepts V2-only snapshot segments") {
    val v2Json = SegmentListJson.encodeV2Segments(
      Seq(
        Segment.v2(
          id = 30L,
          partitionId = 20L,
          rows = 1L,
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
    val partitions = scanFromOptions(rawOptions).planInputPartitions()
    assert(partitions.length == 1)
    val partition = partitions.head.asInstanceOf[MilvusV2InputPartition]
    assert(partition.task.segmentId == 30L)
    assert(partition.task.partitionId == 20L)
  }

  test(
    "snapshot planner hands StorageV2 segments their own and their partition's delete files"
  ) {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    val partitions = scan.inputPartitions(
      snapshotOf(
        v2 = Seq(
          Segment.v2(
            id = 30L,
            partitionId = 20L,
            rows = 1L,
            columnGroups = Seq(
              V2ColumnGroup(
                fieldIds = Seq(100L),
                filePaths = Seq("files/insert_log/10/20/30/100/1.parquet"),
                fileRowCounts = Seq(1L)
              )
            )
          ),
          Segment.v2(
            id = 31L,
            partitionId = 21L,
            rows = 1L,
            columnGroups = Seq(
              V2ColumnGroup(
                fieldIds = Seq(100L),
                filePaths = Seq("files/insert_log/10/21/31/100/1.parquet"),
                fileRowCounts = Seq(1L)
              )
            )
          )
        ),
        partitionIds = Seq(20L)
      ),
      DeleteFileListing(
        v3BySegment = Map.empty,
        v2BySegment = Map(30L -> Seq(log(4))),
        inheritedByPartition = Map(-1L -> Seq(log(1)), 20L -> Seq(log(2))),
        v3ReadVersions = Map.empty
      )
    )

    // Nothing is resolved on the executor from a marker any more: every
    // partition carries the files it applies, and reads them itself.
    val first = partitions(0).asInstanceOf[MilvusV2InputPartition]
    val second = partitions(1).asInstanceOf[MilvusV2InputPartition]
    assert(
      first.task.deletes == DeleteSource.Files(Seq(log(1), log(2), log(4)))
    )
    assert(second.task.deletes == DeleteSource.Files(Seq(log(1))))
  }

  test("snapshot planner dedups V2 column groups by slot before planning") {
    val scan = scanWithOptions(new ju.HashMap[String, String]())
    // A segment that went through add-field + backfill: the old multi-field
    // group (slot 3) still reports field 100 from its own schema, and the newer
    // single-field group (slot 100) reports it too. The planner
    // must strip the overlapping field from the older slot.
    val partitions = scan.inputPartitions(
      snapshotOf(
        v2 = Seq(
          Segment.v2(
            id = 30L,
            partitionId = 20L,
            rows = 2L,
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
        ),
        partitionIds = Seq(20L)
      )
    )

    val partition = partitions.head.asInstanceOf[MilvusV2InputPartition]
    val groups = partition.task.layout match {
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
