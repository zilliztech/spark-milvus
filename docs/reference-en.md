# Milvus Spark Connector Parameter Reference

This document provides a comprehensive guide to all parameter configurations for the Milvus Spark Connector.

## Version Compatibility

**This connector requires Milvus 2.6 or later** (Storage V2).

For Milvus 2.5 and earlier versions, please use the `legacy` branch which is no longer actively maintained.

## Overview

Milvus Spark Connector provides the **`milvus`** data source format for reading and writing Milvus data.

Additionally, a convenient `MilvusDataReader` utility class is provided to simplify collection data reading operations.

## 1. `MilvusDataReader` Convenient Reading Method

`MilvusDataReader` provides a convenient method to read collection data.

```scala
import com.zilliz.spark.connector.{MilvusDataReader, MilvusDataReaderConfig, MilvusOption}

// Basic usage
val milvusDF = MilvusDataReader.read(
  spark,
  MilvusDataReaderConfig(
    uri = "http://localhost:19530",
    token = "your-token",
    collectionName = "your_collection"
  )
)

// Usage with additional options
val milvusDFWithOptions = MilvusDataReader.read(
  spark,
  MilvusDataReaderConfig(
    uri = "http://localhost:19530",
    token = "your-token",
    collectionName = "your_collection",
    options = Map(
      MilvusOption.MilvusDatabaseName -> "your_database",
      MilvusOption.MilvusPartitionName -> "your_partition"
    )
  )
)
```

### 1.1 MilvusDataReaderConfig Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `uri` | String | Yes | Milvus server connection URI |
| `token` | String | Yes | Milvus authentication token |
| `collectionName` | String | Yes | Collection name |
| `options` | Map[String, String] | No | Additional configuration options, supports the following parameters |

### 1.2 Supported options Parameters

**Basic Connection Parameters:**
- `MilvusOption.MilvusDatabaseName` - Database name
- `MilvusOption.MilvusPartitionName` - Partition name

**S3 Storage Parameters:**
- `MilvusOption.S3Endpoint` - S3 service endpoint
- `MilvusOption.S3BucketName` - S3 bucket name
- `MilvusOption.S3RootPath` - S3 root path
- `MilvusOption.S3AccessKey` - S3 access key
- `MilvusOption.S3SecretKey` - S3 secret key
- `MilvusOption.S3UseSSL` - Whether to use SSL connection ("true"/"false")
- `MilvusOption.S3PathStyleAccess` - Whether to use path-style access ("true"/"false")
  - **Note**: For Alibaba Cloud OSS, this must be manually set to "false"
  - Other S3-compatible storage typically doesn't require setting this parameter

### 1.3 S3 Configuration Example

```scala
// Alibaba Cloud OSS Configuration Example
val ossOptions = Map(
  MilvusOption.S3Endpoint -> "oss-cn-hangzhou.aliyuncs.com",
  MilvusOption.S3BucketName -> "your-bucket-name",
  MilvusOption.S3RootPath -> "your-root-path",
  MilvusOption.S3AccessKey -> "your-access-key",
  MilvusOption.S3SecretKey -> "your-secret-key",
  MilvusOption.S3UseSSL -> "true",
  MilvusOption.S3PathStyleAccess -> "false",  // Must be set to false for OSS
  MilvusOption.MilvusDatabaseName -> "default"
)

// AWS S3 Configuration Example
val s3Options = Map(
  MilvusOption.S3Endpoint -> "s3.amazonaws.com",
  MilvusOption.S3BucketName -> "your-bucket-name",
  MilvusOption.S3RootPath -> "your-root-path",
  MilvusOption.S3AccessKey -> "your-access-key",
  MilvusOption.S3SecretKey -> "your-secret-key",
  MilvusOption.S3UseSSL -> "true",
  MilvusOption.MilvusDatabaseName -> "default"
  // S3PathStyleAccess typically doesn't need to be set
)
```

### 1.4 How It Works

Each Spark partition is one segment. For a segment with `storage_version = 3`, `MilvusV3PartitionReader` opens the segment manifest through milvus-storage's C interface and pulls Arrow batches; for `storage_version = 2`, `MilvusV2PartitionReader` opens the column-group parquet files the same way. Delete files are read on the driver into a delete plan, and the reader drops deleted rows by primary key and timestamp.

## 2. `milvus` Format Parameters

### 2.1 Connection Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.MilvusUri` | String | Conditional | - | Milvus server connection URI, format: `http://host:port` or `https://host:port`. Required for client mode; not required for snapshot/backup mode. Client mode also needs `fs.root_path` set to the Milvus `minio.rootPath` (an instance id on Zilliz Cloud, `files` on a default self-managed Milvus): the snapshot directory is `<root>/snapshots/<collection id>/metadata/` (offline reads). |
| `MilvusOption.MilvusToken` | String | No | "" | Milvus server authentication token |
| `MilvusOption.MilvusDatabaseName` | String | No | "" | Database name, defaults to default database |

### 2.2 SSL/TLS Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.MilvusServerPemPath` | String | No | "" | Server certificate file path (one-way TLS) |
| `MilvusOption.MilvusClientKeyPath` | String | No | "" | Client private key file path (mutual TLS) |
| `MilvusOption.MilvusClientPemPath` | String | No | "" | Client certificate file path (mutual TLS) |
| `MilvusOption.MilvusCaPemPath` | String | No | "" | CA certificate file path (mutual TLS) |

### 2.3 Data Operation Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.MilvusCollectionName` | String | Conditional | - | Collection name. Required for client mode; in backup mode required only when the backup holds more than one collection. |
| `MilvusOption.MilvusPartitionName` | String | No | "" | Partition name, operates on all partitions when empty |
| `MilvusOption.MilvusCollectionID` | String | No | "" | Collection ID, usually auto-retrieved |
| `MilvusOption.MilvusPartitionID` | String | No | "" | Partition ID, usually auto-retrieved |
| `MilvusOption.MilvusSegmentID` | String | No | "" | Segment ID, for reading specific segments |
| `MilvusOption.ReaderFieldIDs` | String | No | "" | Comma-separated field ID list, for reading specific fields |
| `milvus.read.vector.raw` | Boolean | No | false | Output type for vector columns. With the default `false`, vectors are converted to native Spark types (`FloatVector`/`Float16Vector`/`BFloat16Vector` to `ArrayType(FloatType)`, `Int8Vector` to `ArrayType(ShortType)`, `SparseFloatVector` to `MapType(LongType, FloatType)`). Set to `true` and vector columns come out as `BinaryType`, the bytes exactly as stored, for the caller to decode using `dim` and the element type. That path does no per-element conversion, which suits batch jobs that hand the bytes straight to a native library |
| `milvus.read.columnar` | Boolean | No | false | How the scan delivers rows. With the default `false` Spark gets one row at a time. Set to `true` and it gets whole batches (`ColumnarBatch`), with vector columns typed as `milvus.read.vector.raw` decides. A batch with deleted rows is delivered as the surviving rows, because Spark's `ColumnarBatch` has no way to mark a row invalid |


### 2.5 Offline Backup Read Parameters

Read a binlog-format milvus-backup export as a DataFrame without any Milvus
client connection. A plain `milvus-backup create` on released versions
(v0.5.x) already produces binlog format; the `--format binlog` flag exists
only on milvus-backup's master branch (which targets Milvus 3.x and defaults
to snapshot). See `docs/backup-datasource-design.md` for the full design.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.BackupDir` | String | No | "" | `milvus.backup.dir` — the backup directory, e.g. `s3a://bucket/backup/<name>`. **S3 only** (`s3://` is normalized to `s3a://`); local/`file://` dirs are rejected at planning because the packed reader requires S3. |
| `MilvusOption.MilvusDatabaseName` | String | No | "" | Database the collection lives in. Passing `"default"` selects the default-database collection (matching a meta that records `""` or `"default"`); leaving the option empty performs single-candidate / ambiguity resolution instead — with both `default.orders` and `db2.orders` present, pass `"default"` (or `"db2"`) to disambiguate. |
| `MilvusOption.MilvusCollectionName` | String | Conditional | - | Collection name inside the backup (matched with the database name, never `.head`). Required when the backup holds more than one collection. |
| `MilvusOption.SnapshotPath` | String | No | - | `milvus.snapshot.path` — a snapshot JSON in the snapshot directory (`s3a://bucket/files/snapshots/<coll>/metadata/<id>.json` or a key relative to `fs.bucket_name`). Reads it without a Milvus service: schema, partitions and segments all come from that file. Cannot be combined with `milvus.snapshot.manifests`. |
| `MilvusOption.ClientSnapshotName` | String | No | latest | `milvus.client.snapshot.name` — with `milvus.uri`: read this snapshot of the collection from the snapshot directory instead of the latest one. The connector never creates snapshots; make one with Milvus or `CALL create_snapshot`. |
| `MilvusOption.SnapshotMaxJsonBytes` | Long | No | 67108864 | `milvus.snapshot.max.json.bytes` — max size of the backup `full_meta.json`. |

The Spark read schema is derived from the backup meta unless `.schema()` is
given; a meta that cannot be read fails the read either way. Reading a dynamic collection (`enable_dynamic_field=true`) requires the
backup meta to record the `$meta` field — captured only with milvus-backup
etcd access (`--backup_index_extra`) and **v0.5.13+**. A column group spanning
multiple binlog files is supported (milvus-storage#657 fixed the per-file row
range encoding; see the design doc). One backup shape still aborts the read at
planning time: a collection with struct-array fields (`struct_array_fields`).
S3 credentials use the existing `fs.*` options (`fs.address`,
`fs.access_key_id`, `fs.access_key_value`, ...); the bucket comes from the
`milvus.backup.dir` URI.

## 3. Usage Examples

### 3.1 Reading Data

```scala
val df = spark.read
  .format("milvus")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusToken, "your-token")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option(MilvusOption.MilvusDatabaseName, "your_database")
  .option(MilvusOption.ReaderFieldIDs, "1,2,100,101")  // Read only specified fields
  .load()
```


## 4. Data Schema

### 4.1 Output Schema

The output schema for `milvus` format depends on the Milvus collection schema and includes:

- User-defined fields (based on collection schema)
- `$meta` (StringType) - Dynamic fields (if enabled)

## 5. Important Notes

1. **Version Requirement**: This connector requires Milvus 2.6+ with Storage V2
2. **SSL/TLS Configuration**: Supports both one-way and mutual TLS authentication, configure certificate files as needed
5. **Parameter Constants**: It's recommended to use constants defined in the `MilvusOption` class to avoid string spelling errors

## 6. Supported Data Types

### 6.1 Scalar Types
- Bool
- Int8, Int16, Int32, Int64
- Float, Double
- String, VarChar
- JSON

### 6.2 Vector Types
- FloatVector
- Float16Vector
- BFloat16Vector
- BinaryVector
- Int8Vector
- SparseFloatVector

### 6.3 Complex Types
- Array (supports scalar element types)
