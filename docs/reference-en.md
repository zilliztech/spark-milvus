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
      MilvusOption.MilvusPartitions -> "100,101",
      MilvusOption.MilvusSegments -> "2001,2002"
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
- `MilvusOption.MilvusPartitions` - Comma-separated numeric partition IDs
- `MilvusOption.MilvusSegments` - Comma-separated numeric segment IDs

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

`getTable` resolves exactly one immutable snapshot. That same `Snapshot` supplies
the schema, selected segments, statistics, and scan plan; a scan never asks the
service or storage for a newer view. The driver closes every object store used
to materialize snapshot metadata on both success and failure, before executor
tasks are serialized. Each Spark partition then reads one data segment. For
`storage_version = 3`, the executor opens the pinned segment
manifest; for `storage_version = 2`, it opens the column-group parquet files
listed by the task; both storage lines share the same row reader. The driver
sends delete-file descriptors, not decoded
primary-key maps. Each executor reads and closes the delete files that apply to
its segment, then drops rows by primary key and timestamp. An unreadable delete
file or a segment that yields fewer rows than its declared count fails the task
instead of returning incomplete data.

### 1.5 Metrics

Every read and write reports what it cost on the C/JVM boundary as task metrics on the scan or write node of the Spark SQL page; nothing has to be switched on. A scan reports `milvus.jni.calls`, `milvus.jni.nanos`, `milvus.arrow.batches`, `milvus.arrow.bytes` (Arrow bytes handed over), `milvus.copies` and `milvus.copied.bytes` (columns the native side had to copy because a batch arrived sliced), `milvus.rows.materialized` (rows turned into Spark rows; zero on the columnar path) and `milvus.arrow.allocated.max` (the Arrow allocator's peak, the maximum over tasks). A write reports the first four and the peak. Bytes read from object storage for segment data are not among them: that read happens inside milvus-storage.

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
| `MilvusOption.MilvusCollectionID` | String | No | "" | Collection ID, usually auto-retrieved |
| `MilvusOption.MilvusPartitions` (`milvus.partitions`) | String | No | unset | Comma-separated numeric partition IDs. The selector is applied after any snapshot source resolves. Every requested ID must exist; duplicates are ignored without changing the first-seen order. |
| `MilvusOption.MilvusSegments` (`milvus.segments`) | String | No | unset | Comma-separated numeric segment IDs. It may be combined with `milvus.partitions`, in which case the scan reads their intersection. Every requested ID must exist. |
| `MilvusOption.ReaderFieldIDs` (`fieldIDs`) | String | No | unset | Comma-separated numeric field IDs, applied during both schema inference and table creation. Each requested ID must exist in the snapshot schema; Spark projection can further prune this set. With an external `.schema()`, its non-metadata fields must select exactly these IDs and their names and Spark types must match the snapshot. |
| `MilvusOption.MilvusExtraColumns` (`milvus.extra.columns`) | String | No | "" | Comma-separated metadata columns. The supported names are `_segment_id`, `_row_offset`, and `_timestamp`; see section 4. |
| `MilvusOption.ReadApplyDeletes` (`milvus.read.apply.deletes`) | Boolean | No | true | Apply all segment-local, partition-level L0, and collection-level L0 deletes visible in the fixed snapshot. Setting this to `false` is explicit opt-out; any provided value besides `true` or `false`, including a blank value, is rejected. |
| `milvus.read.vector.raw` | Boolean | No | false | Output type for vector columns. With the default `false`, vectors are converted to native Spark types (`FloatVector`/`Float16Vector`/`BFloat16Vector` to `ArrayType(FloatType)`, `Int8Vector` to `ArrayType(ShortType)`, `SparseFloatVector` to `MapType(LongType, FloatType)`). Set to `true` and vector columns come out as `BinaryType`, the bytes exactly as stored, for the caller to decode using `dim` and the element type. That path does no per-element conversion, which suits batch jobs that hand the bytes straight to a native library |
| `milvus.read.columnar` | Boolean | No | true | How the scan delivers rows. With the default `true` Spark gets whole Arrow batches (`ColumnarBatch`) that wrap the native buffers without copying, with vector columns typed as `milvus.read.vector.raw` decides; a batch with deleted rows is delivered through a position map over the surviving rows, still without copying. `false` delivers one row at a time. A read with `vector.search.*` options takes the row path regardless, because that stage scores rows. Row and columnar readers use the same expected-row guard. |

Provided selector lists reject blank values, empty entries, and non-numeric
values. Boolean read options
(`milvus.snapshot.mode`, `milvus.read.apply.deletes`,
`milvus.read.vector.raw`, and `milvus.read.columnar`) accept only `true` or
`false`, case-insensitively; a blank value or misspelling is an error rather
than a default. A provided `milvus.snapshot.max.json.bytes` must be a positive
integer. Vector search is
enabled only when `vector.search.query` and `vector.search.topK` are both set:
the query must be a non-empty JSON-style array of finite numbers and `topK`
must be a positive integer. Any provided vector-search option must be non-blank;
a partial or malformed configuration fails during planning.


### 2.4 Write Parameters

`df.write.format("milvus").mode("append")` writes the DataFrame as Milvus
segments straight to object storage; no Milvus service is involved and nothing
is registered. The table is resolved the same way as for a read, so the
collection schema comes from one of: `milvus.uri` plus the collection name
(the latest snapshot of the collection), `milvus.snapshot.path`, or
`milvus.snapshot.schema.bytes` (a schema alone, for a write with no snapshot
at all). Field ids and vector dimensions come from that schema.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `fs.root_path` | String | No | `files` | The job writes under `{root}/staging/{job-id}/`: one segment directory per Spark partition, then `manifest.json` (every segment's path, manifest version and row count) and the marker `_committed`. |
| `MilvusOption.MilvusInsertMaxBatchSize` | Int | No | 1000 | Rows per Arrow batch handed to the native writer. |
| `milvus.writer.variableWidthBytesPerValue` | Double | No | 32.0 | Initial bytes reserved per value of a variable-width column (strings, JSON, binary). |

What the write checks before any task starts: every DataFrame column is a
field of the collection with the same Spark type as a read gives it (vector
columns may also be the raw `BinaryType` bytes); every field except Milvus
function outputs is present (give a nullable field a null column); the
collection does not use `autoID` or a partition key. Only `mode("append")` is
supported. `overwrite` is deliberately not supported: it would make every
existing row of the collection invisible and Milvus has no rollback, so Spark
refuses it at analysis time and no data is touched. For a full refresh, drop
and recreate the collection on the Milvus side, or delete all rows through the
SDK, then append. `errorIfExists`/`ignore` are not supported by Spark for this
kind of source.

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
| `MilvusOption.SnapshotMaxJsonBytes` | Long | No | 67108864 | `milvus.snapshot.max.json.bytes` — positive maximum size of a snapshot JSON or backup `full_meta.json`. |

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

For S3-compatible endpoints, `fs.address` is the canonical endpoint option;
DataFrame options `fs.s3a.endpoint` and `s3.endpoint` are aliases, in that
priority order. Existing Spark/Hadoop `fs.s3a.endpoint` configuration is also
translated to the same native property.
Milvus metadata may spell an object as
`s3://endpoint:port/bucket/key`. The connector recognizes that form only while
decoding Milvus-produced metadata (an explicit authority port, or an authority
host equal to the configured endpoint host), and normalizes it to the same `(bucket,
key)` as `s3a://bucket/key`. A user-supplied standard S3 URI always treats its
authority as the bucket. Path-style access is taken from
`fs.s3a.path.style.access`, then `s3.pathStyleAccess`, then the inverse of
`fs.use_virtual_host`; each setting is strictly boolean.
Snapshot JSON, Avro, V2 footers, V3 manifests, data files, and delete files
must all normalize to the snapshot's one bucket; a cross-bucket reference
fails during planning.

## 3. Usage Examples

### 3.1 Reading Data

```scala
val df = spark.read
  .format("milvus")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusToken, "your-token")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option(MilvusOption.MilvusDatabaseName, "your_database")
  .option(MilvusOption.MilvusPartitions, "100,101")
  .option(MilvusOption.MilvusSegments, "2001,2002")
  .option(MilvusOption.ReaderFieldIDs, "100,101")  // Read only specified collection fields
  .option(MilvusOption.MilvusExtraColumns, "_segment_id,_row_offset,_timestamp")
  .load()
```

### 3.2 Writing Data

```scala
df.write
  .format("milvus")
  .mode("append")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusToken, "your-token")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option("fs.root_path", "files")           // plus the fs.* storage options
  .save()
```

The segments land under `files/staging/<job-id>/` with a job manifest; they
become part of the collection only when registered (a later step, see
`docs/design/capabilities.md` A4). A rerun of a job that already committed
writes nothing.

### 3.3 Registering a backfill with Milvus

A backfill job (`MilvusBackfill.run`) writes a new column group into each
existing segment and commits a job manifest under
`{stagingRoot}/staging/{jobId}/` (`BackfillConfig.stagingRoot`, `jobId`; the
prefix comes back as `BackfillResult.stagingPrefix`). Handing the new
manifest versions to Milvus is a separate call:

```scala
import com.zilliz.spark.connector.procedure.Register

Register.run(
  Map(
    MilvusOption.MilvusUri -> "http://localhost:19530",
    MilvusOption.MilvusToken -> "your-token",
    MilvusOption.MilvusCollectionName -> "your_collection",
    "fs.bucket_name" -> "milvus-bucket"          // plus the other fs.* options
  ),
  stagingPrefix = result.stagingPrefix
)
```

It reads the job manifest, calls Milvus's `BatchUpdateManifest` with every
segment's id and new manifest version, and marks the job registered so a
second call does nothing. Only a job that wrote into existing segments can be
registered this way; a job that created new segments (`df.write`) is refused
until Milvus offers `RegisterSegments`. The SQL form `CALL
milvus.system.register(...)` is not available yet.


## 4. Data Schema

### 4.1 Output Schema

The output schema for `milvus` format depends on the Milvus collection schema and includes:

- Collection fields selected by Spark projection and `fieldIDs`. Each field
  keeps the snapshot's name, field ID, Milvus data type, nullability, key flags,
  and vector dimension in its Spark metadata.
- `$meta` when the collection schema records its authoritative field ID as one
  JSON field marked dynamic. If dynamic fields are enabled but that definition
  is missing or inconsistent, planning fails; the connector never guesses the
  physical field ID.
- Requested metadata columns, appended in the fixed order shown below
  regardless of request order:
  - `_segment_id` (`LongType`, non-null): the data segment ID.
  - `_row_offset` (`LongType`, non-null): the physical row position in that
    segment before delete filtering.
  - `_timestamp` (`LongType`, snapshot nullability): the stored Milvus system
    timestamp field with field ID `1`; it is read from storage, not synthesized.

The connector does not expose a `partition` metadata column. Partition and
segment selection uses `milvus.partitions` and `milvus.segments`.

## 5. Important Notes

1. **Version Requirement**: This connector requires Milvus 2.6+ with Storage V2
2. **SSL/TLS Configuration**: Supports both one-way and mutual TLS authentication, configure certificate files as needed
5. **Parameter Constants**: It's recommended to use constants defined in the `MilvusOption` class to avoid string spelling errors

## 6. Supported Data Types

### 6.1 Scalar Types
- Bool (`BooleanType`)
- Int8, Int16, Int32, Int64 (`ByteType`, `ShortType`, `IntegerType`, `LongType`)
- Float, Double (`FloatType`, `DoubleType`)
- String, VarChar, Text, JSON (`StringType`)

### 6.2 Vector Types
- FloatVector
- Float16Vector
- BFloat16Vector
- BinaryVector
- Int8Vector
- SparseFloatVector

### 6.3 Complex Types
- Array of Bool, Int8, Int16, Int32, Int64, Float, Double, String, or VarChar

This is a closed support list. Geometry, Timestamptz,
ArrayOfVector/struct-array fields, and unknown future Milvus types are not
silently converted to binary or null: schema resolution or value conversion
fails explicitly and reports the unsupported type.
