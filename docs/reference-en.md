# Milvus Spark Connector Parameter Reference

This document provides a comprehensive guide to all parameter configurations for the two data source formats in Milvus Spark Connector.

## Overview

Milvus Spark Connector provides two data source formats:

1. **`milvus`** - For reading and writing Milvus data
2. **`milvusbinlog`** - For reading a single field binary log file in a Milvus collection (read-only)

Additionally, a convenient `MilvusDataReader` utility class is provided to simplify collection data reading operations. This utility automatically handles the merging of insert and delete logs, returning the final valid data.

## 1. `MilvusDataReader` Convenient Reading Method

`MilvusDataReader` provides a convenient method to read collection data, automatically handling the merging of insert and delete logs to return the final valid data.

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

`MilvusDataReader` internally performs the following steps:

1. **Read Insert Logs**: Uses `milvus` format to read collection insert data
2. **Read Delete Logs**: Uses `milvusbinlog` format to read delete logs
3. **Data Merging**: Automatically handles delete operations, filtering out deleted records
4. **Return Results**: Returns final valid data with system fields removed (such as `row_id`, `timestamp`)

This approach is particularly suitable for scenarios where you need to obtain the current valid data of a collection without manually handling the complex merging logic of insert and delete logs.

## 2. `milvus` Format Parameters

### 2.1 Connection Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.MilvusUri` | String | Yes | - | Milvus server connection URI, format: `http://host:port` or `https://host:port` |
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
| `MilvusOption.MilvusCollectionName` | String | Yes | - | Collection name |
| `MilvusOption.MilvusPartitionName` | String | No | "" | Partition name, operates on all partitions when empty |
| `MilvusOption.MilvusCollectionID` | String | No | "" | Collection ID, usually auto-retrieved |
| `MilvusOption.MilvusPartitionID` | String | No | "" | Partition ID, usually auto-retrieved |
| `MilvusOption.MilvusSegmentID` | String | No | "" | Segment ID, for reading specific segments |
| `MilvusOption.ReaderFieldIDs` | String | No | "" | Comma-separated field ID list, for reading specific fields |

### 2.4 Write Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.MilvusInsertMaxBatchSize` | Int | No | 5000 | Maximum batch size for single insert operation |
| `MilvusOption.MilvusRetryCount` | Int | No | 3 | Number of retries on operation failure |
| `MilvusOption.MilvusRetryInterval` | Int | No | 1000 | Retry interval in milliseconds |

## 3. `milvusbinlog` Format Parameters

### 3.1 Basic Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.ReaderType` | String | Yes | - | Reader type, supports "insert" or "delete" |
| `MilvusOption.ReaderPath` | String | Conditional | - | Binary log file path, required when not using S3 |

### 3.2 Milvus Connection Parameters (for metadata retrieval)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.MilvusUri` | String | No | "" | Milvus server URI for retrieving collection metadata |
| `MilvusOption.MilvusToken` | String | No | "" | Milvus authentication token |
| `MilvusOption.MilvusDatabaseName` | String | No | "" | Database name |
| `MilvusOption.MilvusCollectionName` | String | No | "" | Collection name |
| `MilvusOption.MilvusPartitionName` | String | No | "" | Partition name |
| `MilvusOption.MilvusCollectionID` | String | No | "" | Collection ID |
| `MilvusOption.MilvusPartitionID` | String | No | "" | Partition ID |
| `MilvusOption.MilvusSegmentID` | String | No | "" | Segment ID |
| `MilvusOption.MilvusFieldID` | String | No | "" | Field ID, required for insert type |

### 3.3 S3 Storage Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.S3FileSystemTypeName` | String | No | "" | S3 filesystem type identifier, fixed value "s3.fs" |
| `MilvusOption.S3Endpoint` | String | No | "localhost:9000" | S3 service endpoint |
| `MilvusOption.S3BucketName` | String | No | "a-bucket" | S3 bucket name |
| `MilvusOption.S3RootPath` | String | No | "files" | S3 root path |
| `MilvusOption.S3AccessKey` | String | No | "minioadmin" | S3 access key |
| `MilvusOption.S3SecretKey` | String | No | "minioadmin" | S3 secret key |
| `MilvusOption.S3UseSSL` | Boolean | No | false | Whether to use SSL connection |
| `MilvusOption.S3PathStyleAccess` | Boolean | No | true | Whether to use path-style access |

## 4. Usage Examples

### 4.1 `milvus` Format Examples

#### Reading Data
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

#### Writing Data
```scala
df.write
  .format("milvus")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusToken, "your-token")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option(MilvusOption.MilvusDatabaseName, "your_database")
  .option(MilvusOption.MilvusInsertMaxBatchSize, "1000")
  .option(MilvusOption.MilvusRetryCount, "5")
  .save()
```

### 4.2 `milvusbinlog` Format Examples

#### Reading Insert Logs
```scala
val insertDF = spark.read
  .format("milvusbinlog")
  .option(MilvusOption.ReaderType, "insert")
  .option(MilvusOption.ReaderPath, "/path/to/insert/logs")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option(MilvusOption.MilvusFieldID, "100")  // Field ID required for insert logs
  .load()
```

#### Reading Delete Logs
```scala
val deleteDF = spark.read
  .format("milvusbinlog")
  .option(MilvusOption.ReaderType, "delete")
  .option(MilvusOption.ReaderPath, "/path/to/delete/logs")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .load()
```

#### Using S3-stored Binary Logs
```scala
val df = spark.read
  .format("milvusbinlog")
  .option(MilvusOption.ReaderType, "insert")
  .option(MilvusOption.S3FileSystemTypeName, "s3.fs")
  .option(MilvusOption.S3Endpoint, "s3.amazonaws.com")
  .option(MilvusOption.S3BucketName, "your-bucket")
  .option(MilvusOption.S3RootPath, "milvus-logs")
  .option(MilvusOption.S3AccessKey, "your-access-key")
  .option(MilvusOption.S3SecretKey, "your-secret-key")
  .option(MilvusOption.S3UseSSL, "true")
  .option(MilvusOption.MilvusCollectionID, "123456")
.option(MilvusOption.MilvusFieldID, "100")
  .load()
```

## 5. Data Schema

### 5.1 `milvus` Format Output Schema

The output schema for `milvus` format depends on the Milvus collection schema and includes the following system fields:

- `row_id` (LongType) - Row ID
- `timestamp` (LongType) - Timestamp
- User-defined fields (based on collection schema)
- `$meta` (StringType) - Dynamic fields (if enabled)

### 5.2 `milvusbinlog` Format Output Schema

The `milvusbinlog` format outputs the following fixed fields:

- `data` (StringType/LongType) - Data content, field value for insert, primary key value for delete
- `timestamp` (LongType) - Timestamp
- `data_type` (IntegerType) - Data type identifier

## 6. Important Notes

1. **Automatic Path Construction**: When providing parameters like `MilvusOption.MilvusCollectionID`, the system automatically constructs binary log paths
2. **Field ID Requirement**: For `milvusbinlog` format with insert type, `MilvusOption.MilvusFieldID` must be specified
3. **SSL/TLS Configuration**: Supports both one-way and mutual TLS authentication, configure certificate files as needed
4. **Batch Size**: Properly setting `MilvusOption.MilvusInsertMaxBatchSize` can optimize write performance
5. **Retry Mechanism**: Built-in retry mechanism improves operation reliability
6. **Parameter Constants**: It's recommended to use constants defined in the `MilvusOption` class to avoid string spelling errors

## 7. Supported Data Types

### 7.1 Scalar Types
- Bool
- Int8, Int16, Int32, Int64
- Float, Double
- String, VarChar
- JSON

### 7.2 Vector Types
- FloatVector
- Float16Vector
- BFloat16Vector
- BinaryVector
- Int8Vector
- SparseFloatVector

### 7.3 Complex Types
- Array (supports scalar element types)
