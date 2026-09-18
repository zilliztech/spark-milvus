# Milvus Spark Connector Parameter Reference

This document provides a comprehensive guide to all parameter configurations for the Milvus Spark Connector.

## Vector search (refactor/v2)

`com.zilliz.spark.connector.read.MilvusSearch.search` takes a query set and
returns a lazy DataFrame holding each query's top-k across the snapshot's
segments:

```scala
val queries = Seq((1L, Array(0.1f, 0.2f)), (2L, Array(0.3f, 0.4f)))
  .toDF("query_id", "vector")
val hits = MilvusSearch.search(
  spark, options, queries, "embedding", 10, "COSINE",
  mode = "index",
  searchParameters = Map("ef" -> "256"),
  filter = Some("category == \"documents\" and rating >= 2.0"),
  outputColumns = Seq("id", "title")
)
hits.orderBy("query_id", "rank").show(false)
```

`options` uses the same snapshot and storage settings as
`spark.read.format("milvus")`. The query set carries `query_id`, a non-null
unique BIGINT, and `vector`: `ARRAY<FLOAT>` for FloatVector, Float16Vector and
BFloat16Vector, `ARRAY<SMALLINT>` with values in −128..127 for Int8Vector, and
`BINARY` for BinaryVector. An overload takes a single `queryVector:
Array[Float]` and searches a set of one row with `query_id = 0`.

The result columns are `query_id`, `rank` (from 1), `_score`, `_segment_id`,
`_row_offset`, followed by `outputColumns`. Each query returns at most K rows
and `rank` is what orders them; the DataFrame itself is unordered, so sort by
`query_id` and `rank` to read it. COSINE and IP rank a larger score first, L2 a
smaller one, and equal scores rank by segment id and then by physical row
offset. `_score` preserves the value Knowhere returned. Indexes containing
vector quantization, such as Cardinal RBQ, can return approximate scores; the
connector does not read the original vectors to recompute them.

`mode = "index"` searches the persisted index the snapshot pinned, over a
non-nullable FloatVector with L2, IP or COSINE, and the query metric must match
the index. `mode = "exact"` computes every distance instead: it takes every
dense vector type, and binary vectors take HAMMING or JACCARD.

The filter runs before search. Supported scalar syntax is comparison
(`==`, `!=`, `<`, `<=`, `>`, `>=`), `in`, `not in`, `is null`, `is not null`,
`and`, `or`, `not`, and parentheses. Unknown fields, incompatible literals and
unsupported syntax fail before execution. Filtering the returned DataFrame
instead filters the already selected hits. JSON, arrays and functions are not
yet supported in this expression subset.

Missing index metadata, and an index whose metric or row count differs from its
segment, fail while the query is planned, naming every such segment; corrupt
files and incompatible formats fail when a task loads the index.
`allowUnindexed = true` scans a segment exactly when the snapshot confirms the
field has no index there. Default is `false`. Each task owns and closes the
indexes it loaded; they are not cached across tasks. HNSW `ef` is the only
supported search parameter and must be an integer at least K. Encrypted indexes
and nullable-vector ID mappings are unsupported. Cardinal `_mem.index.bin`
requires the pinned Cardinal-enabled native build; see
[native build instructions](contributing.md#knowhere-library-loading).

Three options size the job. `milvus.search.queries.max.bytes` (default 1 GiB)
is how large the query set may be before it stops being broadcast from the
driver and travels with the shuffle instead; both paths give the same result.
`milvus.search.group.max.bytes` (default 512 MiB) is what one task answers at a
time, counted as queries × (dimension × element width + K × 28 bytes).
`milvus.search.vectors.max.bytes` (default 2 GiB) is how many bytes of vectors
one task keeps while it answers them, and it also bounds how many segments one
task reads.

The older per-segment `vector.search.*` scan options still exist and return
per-segment candidates rather than a global top-k.

## Version Compatibility

**This connector requires Milvus 2.6 or later** (Storage V2).

For Milvus 2.5 and earlier versions, please use the `legacy` branch which is no longer actively maintained.

## Overview

Milvus Spark Connector provides the **`milvus`** data source format for reading and writing Milvus data.

Additionally, a convenient `MilvusDataReader` utility class is provided to simplify collection data reading operations.

## Catalog Discovery, Table DDL, and Snapshot Time Travel

Register `MilvusCatalog` once to use a Milvus collection as a three-part Spark
table. Spark removes the `spark.sql.catalog.milvus.` prefix and passes the
remaining connection, storage, and read options to the connector.

```scala
spark.conf.set(
  "spark.sql.catalog.milvus",
  "com.zilliz.spark.connector.catalog.MilvusCatalog"
)
spark.conf.set("spark.sql.catalog.milvus.milvus.uri", "http://localhost:19530")
spark.conf.set("spark.sql.catalog.milvus.milvus.token", "your-token")
spark.conf.set("spark.sql.catalog.milvus.fs.address", "s3.us-west-2.amazonaws.com")
spark.conf.set("spark.sql.catalog.milvus.fs.bucket_name", "your-milvus-bucket")
spark.conf.set("spark.sql.catalog.milvus.fs.root_path", "files")
spark.conf.set("spark.sql.catalog.milvus.fs.cloud_provider", "aws")
spark.conf.set("spark.sql.catalog.milvus.fs.region", "us-west-2")
spark.conf.set("spark.sql.catalog.milvus.fs.use_ssl", "true")
spark.conf.set("spark.sql.catalog.milvus.fs.use_iam", "true")

val latest = spark.table("milvus.default.products")
val named = spark.sql(
  "SELECT * FROM milvus.default.products VERSION AS OF 'release-2026-09'"
)
val atTime = spark.sql(
  "SELECT * FROM milvus.default.products TIMESTAMP AS OF '2026-09-16 08:00:00'"
)
```

The same Catalog exposes Milvus databases and collections through Spark's
discovery commands:

```sql
SHOW NAMESPACES IN milvus;
SHOW TABLES IN milvus.default;
SHOW TABLES IN milvus.default LIKE 'product*';
```

A Milvus database is one Spark namespace and a collection is a table in that
namespace. There are no nested namespaces. `SHOW TABLES` therefore requires an
explicit database; `SHOW TABLES IN milvus` does not implicitly select
`default` or combine collections from several databases. An existing database
with no collections returns no rows. Collections are listed from Milvus
metadata even when they do not yet have a readable snapshot. Spark applies the
optional `LIKE` pattern after discovery. Names are preserved as returned by
Milvus, and result order is unspecified.

The identifier must contain exactly one database and one collection. Those two
names override `milvus.database.name` and `milvus.collection.name` in catalog
configuration. An ordinary load selects the latest snapshot; `VERSION AS OF`
matches a snapshot name exactly; `TIMESTAMP AS OF` selects the latest snapshot
whose Milvus HybridTS boundary is not after the instant Spark resolved. The
timestamp literal is interpreted using `spark.sql.session.timeZone` before the
Catalog receives UTC epoch microseconds. The resolved snapshot is fixed for the
returned table and its scans. Replace the storage values for the deployment;
when IAM is unavailable, use `fs.access_key_id` and `fs.access_key_value`
instead of `fs.use_iam=true`.

Time travel selects snapshot metadata; it is not a retention guarantee. The
connector does not retain historical segment files, so compaction or garbage
collection can make a selected older snapshot unreadable.

### CREATE and DROP TABLE

`CREATE TABLE` creates one Milvus collection and then creates an index for each
vector field. The primary key, ambiguous Milvus field types, type parameters,
and vector indexes are explicit table properties:

```sql
CREATE TABLE milvus.default.products (
  id BIGINT NOT NULL,
  title STRING,
  embedding ARRAY<FLOAT>
)
TBLPROPERTIES (
  'milvus.primary.key' = 'id',
  'milvus.field.title.data_type' = 'varchar',
  'milvus.field.title.max_length' = '512',
  'milvus.field.embedding.data_type' = 'float_vector',
  'milvus.field.embedding.dim' = '768',
  'milvus.index.embedding' =
    '{"index_type":"HNSW","metric_type":"COSINE","M":16,"efConstruction":200}'
);

DROP TABLE milvus.default.products;
```

| Property | Value and rules |
|---|---|
| `milvus.primary.key` | Required schema field. It must be non-null and map to Milvus Int64 or VarChar. AutoID is not supported. |
| `milvus.field.<field>.data_type` | Required for Spark String, Array, Binary, and Map fields. Values are case-insensitive snake_case: `varchar`, `text`, `json`, `array`, `float_vector`, `float16_vector`, `bfloat16_vector`, `int8_vector`, `binary_vector`, or `sparse_float_vector`. |
| `milvus.field.<field>.max_length` | Required positive integer for VarChar and `Array<String>`. |
| `milvus.field.<field>.max_capacity` | Required positive integer for Array. |
| `milvus.field.<field>.dim` | Required positive integer for every dense vector. BinaryVector dimensions must also be divisible by 8. |
| `milvus.index.<field>` | Required JSON object for every vector field. `index_type` and `metric_type` are required strings; `index_name` is an optional string. Other entries become Milvus index parameters and must have scalar string, number, or boolean values. |

Here `milvus.index.<field>` is a Catalog table property whose value is one JSON
object for the online CreateIndex operation. It is separate from the DataFrame
write option used for segment index output.

Spark Boolean, Byte, Short, Int, Long, Float, and Double map directly to Milvus
Bool, Int8, Int16, Int32, Int64, Float, and Double; `data_type` is rejected on
those fields. String accepts only `varchar`, `text`, or `json`. A supported
scalar Array accepts `array`; `Array<Float>` additionally accepts
`float_vector`, `float16_vector`, or `bfloat16_vector`, and `Array<Short>`
accepts `int8_vector`. Scalar arrays support Boolean, Short, Int, Long, Float,
Double, and String elements. `Array<Byte>` is rejected because existing reads
expose a Milvus Int8 Array as `Array<Short>`, so accepting it would change the
Spark schema after the first snapshot. Binary accepts only `binary_vector`. Only the exact
`Map<Long, Float>` shape accepts `sparse_float_vector`.

Catalog accepts Spark SQL's default `ArrayType.containsNull=true` and
`MapType.valueContainsNull=true` schema flags because SQL DDL cannot reliably
express element-level NOT NULL and the flags do not prove that null elements
will be written. Top-level field nullability is preserved and still constrains
the primary key. Element-level null support is outside the Catalog DDL contract;
callers must not infer that later writes accept null elements from these flags.

Field references in property names are case-sensitive; only `data_type` values
are case-insensitive. Positive integer properties use canonical decimal form
`[1-9][0-9]*` in the Int range. Index JSON rejects duplicate keys, trailing
content, nulls, arrays, nested objects, blank required values, and duplicate
explicit index names. Table `comment` becomes the collection description and
column comments become field descriptions. `provider`, when present, must be
`milvus`; Spark's `owner` property is ignored. Invalid fields, unknown Milvus
or bookkeeping properties, incompatible types, unsupported column features,
and partition transforms fail before any RPC is sent.

Collection creation and index creation are separate Milvus operations, not one
transaction. If an index request fails after the collection was created, the
collection and any earlier indexes remain. `CREATE TABLE` does not create a
connector snapshot; the table becomes readable through the Catalog only after
Milvus produces a snapshot. CTAS cannot currently complete: after collection
creation there is no connector snapshot that Spark can load for the write, so
the command can fail while leaving an empty collection that must be dropped
explicitly. Collection creation, segment writing, and segment registration also
have no shared transaction.

Before CREATE, a confirmed missing database becomes `NoSuchNamespaceException`
and a confirmed existing collection becomes `TableAlreadyExistsException`.
`DROP TABLE` returns `false` when either the database or collection is confirmed absent.
Authentication, authorization, transport, and service failures remain errors.
Existence checks and mutations are separate remote calls, so conditional DDL
can race with another client; the Milvus response to the mutation is final.

Catalog tables and discovery require client mode (`milvus.uri`). Discovery
contacts only the Milvus service; it does not read snapshot metadata or object
storage. A confirmed missing database is reported as Spark's missing-namespace
error. Authentication, authorization, network, timeout, rate-limit, and other
service failures are reported as errors. Empty results only come from a
successful discovery response. Offline `milvus.snapshot.path` and
`milvus.backup.dir` reads remain on
`format("milvus")`. Namespace CREATE/ALTER/DROP and table ALTER/RENAME remain
unsupported. Table CREATE/DROP does not support AutoID, dynamic fields,
partition keys, table constraints, generated/default/identity columns, or Spark partition
transforms.

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

Table loading (`getTable` or Catalog `loadTable`) resolves exactly one immutable
snapshot. That same `Snapshot` supplies
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
| `MilvusOption.MilvusFilter` (`milvus.filter`) | String | No | unset | Milvus scalar expression for an ordinary table read. It is parsed and validated against the fixed snapshot during planning, evaluated in both row and columnar readers, and combined with Spark predicates and deletes before Limit. It cannot be combined with `vector.search.*`; use `vector.search.filter` for vector search. |
| `MilvusOption.ReadApplyDeletes` (`milvus.read.apply.deletes`) | Boolean | No | true | Apply all segment-local, partition-level L0, and collection-level L0 deletes visible in the fixed snapshot. Setting this to `false` is explicit opt-out; any provided value besides `true` or `false`, including a blank value, is rejected. |
| `milvus.read.vector.raw` | Boolean | No | false | Output type for vector columns. With the default `false`, vectors are converted to native Spark types (`FloatVector`/`Float16Vector`/`BFloat16Vector` to `ArrayType(FloatType)`, `Int8Vector` to `ArrayType(ShortType)`, `SparseFloatVector` to `MapType(LongType, FloatType)`). Set to `true` and vector columns come out as `BinaryType`, the bytes exactly as stored, for the caller to decode using `dim` and the element type. That path does no per-element conversion, which suits batch jobs that hand the bytes straight to a native library |
| `milvus.read.columnar` | Boolean | No | true | How the scan delivers rows. With the default `true` Spark gets whole Arrow batches (`ColumnarBatch`) that wrap the native buffers without copying, with vector columns typed as `milvus.read.vector.raw` decides; a batch with deleted rows is delivered through a position map over the surviving rows, still without copying. `false` delivers one row at a time. A read with `vector.search.*` options takes the row path regardless, because that stage scores rows. Row and columnar readers use the same expected-row guard. |
| `milvus.read.batch.max.rows` | Int | No | 8192 | Positive maximum rows requested from milvus-storage for one record batch; delivered as `reader.record_batch_max_rows`. |
| `milvus.read.batch.max.bytes` | Long | No | 33554432 | Positive target byte limit for one native record batch; delivered as `reader.record_batch_max_size`. The current upstream maximum is 4294967296 (4 GiB). |
| `milvus.read.arrow.max.bytes` | Long | No | 9223372036854775807 | Positive hard limit of the Arrow child allocator owned by one Spark read task. It covers imported/read Arrow buffers on row, columnar and vector paths, but not milvus-storage's separate native memory pool. |

Provided selector lists reject blank values, empty entries, and non-numeric
values. Boolean read options
(`milvus.snapshot.mode`, `milvus.read.apply.deletes`,
`milvus.read.vector.raw`, and `milvus.read.columnar`) accept only `true` or
`false`, case-insensitively; a blank value or misspelling is an error rather
than a default. Positive integer/long options reject blanks, zero, negative,
non-decimal and overflowing values and report both the key and supplied value.
A provided `milvus.snapshot.max.json.bytes` must be a positive integer. Vector search is
enabled only when `vector.search.query` and `vector.search.topK` are both set:
the query must be a non-empty JSON-style array of finite numbers and `topK`
must be a positive integer. Any provided vector-search option must be non-blank;
a partial or malformed configuration fails during planning.
The same strict rules apply to existing connector values:
`milvus.insertMaxBatchSize` (default 5000), `milvus.retry.count` (3),
`milvus.retry.interval` (1000), `s3.maxConnections` (32), and
`s3.preloadPoolSize` (4) are positive integers; `s3.useSSL` and
`s3.pathStyleAccess` are strict Booleans.

#### Spark predicate pushdown

Spark SQL and DataFrame `where` conditions use DataSource V2 predicate
pushdown. Bool fields support equality, inequality, null-safe equality (`<=>`),
`IN`, and null checks. Numeric fields support all six comparisons, `<=>`, `IN`, and null
checks. String, VarChar, and Text fields additionally support prefix and suffix
conditions. These operators preserve Spark SQL three-valued NULL semantics in
both row and columnar reads.

Each predicate tree is pushed only when the connector supports the whole tree.
Unsupported operators, casts, nested references, JSON, Array, Geometry, vector,
and synthetic metadata predicates remain in Spark's plan and are evaluated by
Spark. A predicate-only column is read internally without being added to the
result schema. Reads using `vector.search.*` do not push Spark predicates:
each tree remains residual and Spark evaluates it after vector TopK. To filter
before persisted-index search, use `MilvusSearch.search(..., filter = ...)` or
the corresponding `vector.search.filter` option. The DataSource V1 Filter API
is not supported.

#### Milvus scalar filter

An ordinary table read can use the same scalar Milvus expression subset:

```scala
spark.read
  .format("milvus")
  .options(readOptions)
  .option("milvus.filter", "category == \"documents\" and rating >= 2.0")
  .load()
```

Supported fields are Bool, integer, Float, Double, String, VarChar, and Text.
The syntax includes `==`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`,
`IS NULL`, `IS NOT NULL`, `AND`, `OR`, and `NOT`; Bool comparisons are limited
to `==` and `!=`. Filter-only fields are read
internally but do not enter the result schema. When a Spark `where` condition
is also pushed, both conditions must pass; deletes are then applied and Limit
counts only surviving rows. A blank or malformed expression, an unknown field,
or an incompatible literal fails planning instead of being ignored.

`milvus.filter` is for ordinary scans only and cannot be combined with any
`vector.search.*` option. Vector search uses `vector.search.filter` before
TopK. JSON paths, Array predicates, and `json_contains` are not in the current
scalar subset.


### 2.4 Write Parameters

`df.write.format("milvus").mode("append")` writes the DataFrame as Milvus
segments straight to object storage; no Milvus service is involved and nothing
is registered. The table is resolved the same way as for a read, so the
collection schema comes from one of: `milvus.uri` plus the collection name
(the latest snapshot of the collection), `milvus.snapshot.path`, or
`milvus.snapshot.schema.bytes` (a schema alone, for a write with no snapshot
at all). Field ids and vector dimensions come from that schema.

The current staged segments omit the Milvus system fields RowID (field 0) and
Timestamp (field 1). They therefore cannot be registered with or loaded by
Milvus yet; append registration remains blocked by design decision 22 and a
Milvus `RegisterSegments` API.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `fs.root_path` | String | No | `files` | The job writes under `{root}/staging/{job-id}/`. Before tasks start it writes collection ownership to `owner.json` and refreshes `_heartbeat` every 60 seconds; commit adds the owned `manifest.json` and `_committed`. |
| `MilvusOption.MilvusInsertMaxBatchSize` | Int | No | 5000 | Positive rows per Arrow batch handed to the native writer. |
| `milvus.writer.variableWidthBytesPerValue` | Double | No | 32.0 | Initial bytes reserved per value of a variable-width column (strings, JSON, binary). |
| `milvus.write.file.rolling.bytes` | Long | No | 2147483648 | Positive uncompressed-byte threshold passed to both V2 and V3 native writers as `writer.file_rolling.size`. It controls column-group file rolling; it is not an object-store upload size or an exact final Parquet file size. |

What the write checks before any task starts: every DataFrame column is a
field of the collection with the same Spark type as a read gives it (vector
columns may also be the raw `BinaryType` bytes); every field except Milvus
function outputs is present (give a nullable field a null column); the
collection does not use `autoID` or a partition key. An `Array<Int8>` or
`Array<Int16>` element must be within its type's range, as the Milvus proxy
checks on insert; a value outside it stops the task. Only `mode("append")` is
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
| `MilvusOption.SnapshotPath` | String | No | - | `milvus.snapshot.path` — a snapshot JSON in the snapshot directory: an object-store URI whose authority is the bucket (`s3a://bucket/files/snapshots/<coll>/metadata/<id>.json`; `s3://`, `gs://` and `oss://` likewise), a key relative to `fs.bucket_name`, or the `https://<endpoint>/bucket/files/...` form Milvus's CreateSnapshot prints as `s3_location` (accepted when the host is the configured endpoint and `fs.bucket_name` names that bucket). A location in another bucket than the read's, one that names a bucket while the storage is the local backend, and any other scheme are refused. Reads it without a Milvus service: schema, partitions and segments all come from that file. Cannot be combined with `milvus.snapshot.manifests`. |
| `MilvusOption.ClientSnapshotName` | String | No | latest | `milvus.client.snapshot.name` — for `format("milvus")` with `milvus.uri`, read this named snapshot instead of the latest one. Catalog loads ignore this option: use `VERSION AS OF`. Reads never create snapshots automatically; make one with Milvus or `CALL milvus.system.create_snapshot(...)`. |
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
priority order. Existing Spark/Hadoop `fs.s3a.*` / `fs.oss.*` configuration is
also translated, per bucket first: the endpoint, region, role and static keys,
`connection.ssl.enabled` (`connection.secure.enabled` for OSS) to `fs.use_ssl`
(true when unset, as in Hadoop, unless the endpoint carries its own `http://` or
`https://`), and an explicitly set `path.style.access`. The effective
credential provider chain decides what is used, in order, as it does in Hadoop:
a bucket whose provider is `SimpleAWSCredentialsProvider` uses its keys and not a
globally configured role, and a chain that starts with static keys that are set
uses them. The native layer takes one identity per bucket, so a chain it cannot
take the same way fails and asks for `fs.*` options: a role mixed with another
source, an environment provider ahead of static keys that are set, a role whose
AssumeRole call Hadoop signs with static keys (`fs.s3a.assumed.role.credentials.provider`
naming `SimpleAWSCredentialsProvider`, its default), a provider class the
connector does not know, or a list in `fs.oss.credentials.provider`. With no
provider configured, a role together with static keys fails the same way.
Static keys (`fs.access_key_id` with `fs.access_key_value`) or `fs.role_arn`
given as options decide the identity, and the Hadoop chain is then not judged;
`fs.use_iam=true` keeps a session chain only when it is an AssumedRole provider
alone and otherwise uses the default chain. The OSS keys of a session reading
an S3 bucket, and the S3A keys of one reading an OSS bucket, are skipped when
they supply no endpoint, role or key. Static keys that equal the driver's
`AWS_*` variables may sign an AssumeRole call, because the native default chain
reads the same variables. An explicit `fs.*` option always wins over the
translated value.
A temporary credential (keys plus `fs.s3a.session.token` or `fs.oss.securityToken`)
cannot be translated, because the native storage layer takes no session token.
When the three values are the driver's `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, which Spark copies into those
keys, the keys are left out and the native default chain reads the same
variables. Any other temporary credential is rejected; set `fs.use_iam=true` or
pass long-term keys as `fs.access_key_id` and `fs.access_key_value`.
The endpoint that results is also the one Milvus-produced
`https://<endpoint>/<bucket>/<key>` locations are recognized against, including
an endpoint that only the Hadoop configuration supplies.
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
until Milvus offers `RegisterSegments`.

The same call as SQL. Enable the connector's SQL extension on the session:

```
--conf spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusSparkSessionExtensions
```

then:

```sql
CALL milvus.system.register('your_db.your_collection',
  staging          => 'files/staging/backfill-1789478390101',
  `milvus.uri`     => 'http://localhost:19530',
  `milvus.token`   => 'your-token',
  `fs.bucket_name` => 'milvus-bucket',
  `fs.address`     => 's3.us-west-2.amazonaws.com',
  `fs.use_iam`     => 'true')
```

The first argument is the collection: use `'db.coll'`; for `'coll'`, use
`milvus.database.name` when supplied and use `default` only when it is absent. `staging` is the job's
staging prefix as a key relative to the bucket. Every other argument is a connection or storage option under its usual
key, backquoted because the key contains dots, with the same values a
DataFrame read takes. Values are constants only. The result is a table with
one row per segment: `job_id`, `segment_id`, `manifest_version` and `status`
(`registered`, or `already_registered` when the job had been registered
before). A wrong procedure name, a missing or unknown argument or a wrong type
is refused when the statement is parsed, with the parameters named. Statements
that do not start with `CALL milvus.` are untouched, so the extension can stay
on for every session. The extension works the same on Spark 3.5 and 4.x.

### 3.4 Managing Milvus with `CALL`

The management procedures use the same SQL extension and argument rules shown
above. Procedures that call Milvus must include `milvus.uri` and any required
authentication options; `cleanup_staging` opens only the supplied `fs.*`
storage and does not connect to Milvus. Write the target as `db.collection`. For an unqualified `collection`,
the procedure uses `milvus.database.name` when supplied and uses `default` only
when that option is absent.

| Procedure | Required arguments | Optional arguments | Result |
|-----------|--------------------|--------------------|--------|
| `create_snapshot` | `collection`, `name` | `description`; `compaction_protection_seconds` (default `0`) | One row containing `database`, `collection`, `snapshot`, `description`, `partition_names`, `create_ts`, and `s3_location` |
| `drop_snapshot` | `collection`, `name` | — | One row with `status = dropped` |
| `list_snapshots` | `collection` | — | One row per snapshot name; an empty collection of snapshots returns an empty table |
| `describe_snapshot` | `collection`, `name` | — | The same snapshot metadata columns as `create_snapshot` |
| `create_index` | `collection`, `field`, `index_name` | `index_type` (default `AUTOINDEX`), `metric_type` (default `L2`), `params`, `wait`, `timeout_seconds` | One row with the field, index name, and state: `submitted` without waiting, or `Finished` after successful waiting |
| `drop_index` | `collection`, `index_name` | — | One row with `status = dropped` |
| `load` | `collection` | `wait`, `timeout_seconds` | One row with `state = submitted`, or `LoadStateLoaded` after waiting |
| `release` | `collection` | — | One row with `status = released` |
| `flush` | `collection` | — | One row with `status = submitted`; this means Milvus accepted the request, not that persistence has completed |
| `compact` | `collection` | `wait`, `timeout_seconds` | The compaction ID, plan count, state, and plan-state counts; counts are NULL when the call only submits the work |
| `describe` | `collection` | — | Collection ID, persistent segment count, load state, and one row for each schema field/index pair; index columns are NULL for a field without an index |
| `cleanup_staging` | `collection` | `retention_seconds` (default `604800`, minimum `300`), `dry_run` (default `true`) | One row per child of `{fs.root_path}/staging`: owner, write mode, action/reason, last heartbeat, candidate/deleted file counts, remaining directory count, and `prefix_deleted` |

`create_index`, `load`, and `compact` submit work and return immediately by
default. Set `wait => true` to poll for completion. While waiting,
`timeout_seconds` defaults to 600 and must be positive; supplying it without
`wait => true` is an error. Each polling RPC uses the remaining overall wait
budget as its deadline, so one status request cannot extend the operation past
the configured timeout. A Milvus failure state or an expired timeout fails the
statement instead of returning a successful-looking row.

`register` remains limited to a committed backfill that updates manifests for
existing segments. It does not register segments created by `df.write`.

`cleanup_staging` only selects a job whose versioned owner exactly matches the
requested collection, whose mode is `append`, whose `_registered` marker is
absent, and whose heartbeat and every file modification time are older than the
retention cutoff. It validates any manifest and commit marker that exist and
reads the complete state twice before deletion. Missing, legacy, corrupt,
changing, foreign, fresh, registered, and all backfill jobs are reported as
`preserved`, independently, so one bad job does not hide or authorize another.
For example:

```sql
CALL milvus.system.cleanup_staging('your_db.your_collection',
  retention_seconds => 604800,
  dry_run            => true,
  `fs.bucket_name`   => 'milvus-bucket',
  `fs.address`       => 's3.us-west-2.amazonaws.com',
  `fs.use_iam`       => 'true')
```

Run the dry run first and set `dry_run => false` only after reviewing its rows.
The pinned native filesystem currently exposes file deletion but not recursive
directory deletion. An actual run therefore deletes eligible file objects but
reports remaining object-store directory markers or local empty directories;
`prefix_deleted` is always `false` until milvus-storage exposes that API.


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

### Per-segment vector search in refactor/v2

The existing reader search options call Knowhere BruteForce once per vector
batch, then merge the batch results into each segment's TopK. This scans the
data files; persisted Milvus index files and collection-wide TopK merging are
not implemented by this entry point.

| Option | Meaning |
|---|---|
| `vector.search.query` | Query float array, for example `[0.1,0.2]` |
| `vector.search.topK` | Positive number of results per segment |
| `vector.search.metric` | `L2` (default), `IP`, or `COSINE` |
| `vector.search.column` | Vector field name; default `vector` |

L2 results remain Euclidean distances (square root of Knowhere's squared L2).
IP and COSINE return similarity, ordered largest first. Native float32 scores
can differ slightly from the former JVM double-precision calculation. Deleted
rows and null vectors are excluded before TopK; invalid dimensions, null array
elements, malformed binary values and non-finite elements fail the search.

Build with the validated Knowhere native JAR and start every JVM that uses it
with its own JRE's `libjsig` preloaded; see [native setup](contributing.md#knowhere-library-loading).
Missing libraries or native failures are reported to the caller; this reader
does not fall back to JVM vector distance calculations. The separate legacy
DataFrame/UDF utilities retain their existing implementation.

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

## Native dependency bundle

Build with `-Dmilvus.native.bundle=/absolute/path/to/platform.jar` to include
both JNI libraries and their unified dynamic dependencies in the connector JAR.
The runtime extracts and verifies them once per class loader. This build option
is mutually exclusive with `knowhere.native.jar`; an incompatible explicit
`knowhere.native.path` is rejected. See [building and validation](contributing.md#unified-native-bundle).
