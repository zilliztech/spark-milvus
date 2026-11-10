# Connector reference

The reader consumes fixed Storage V3 snapshots without service calls; the
[column replacement](column-replacement.md) writes new columns of such a
snapshot back from a DataFrame. Both support the Spark/Scala combinations in
[README](../README.md#project-structure).
Use a verified candidate from this source; the previous 2.0 delivery has a
different capability and native-component contract.

## 1. Reading with DataFrames

Read an existing snapshot without connecting to the Milvus service:

```scala
import com.zilliz.spark.connector.options.MilvusOption

val snapshotOptions = Map(
  MilvusOption.SnapshotPath ->
    "s3a://your-milvus-bucket/files/snapshots/123/metadata/snapshot.json",
  "fs.address" -> "s3.us-west-2.amazonaws.com",
  "fs.bucket_name" -> "your-milvus-bucket",
  "fs.root_path" -> "files",
  "fs.cloud_provider" -> "aws",
  "fs.region" -> "us-west-2",
  "fs.use_ssl" -> "true",
  "fs.use_iam" -> "true"
)

val docs = spark.read.format("milvus")
  .options(snapshotOptions)
  .load()

docs.select("id", "title").show(false)
docs.createOrReplaceTempView("docs")
spark.sql("SELECT id, title FROM docs WHERE category = 1").show(false)
```

Replace the snapshot path, storage values and field names with your input.
Schema and segments come from the snapshot; no Milvus URI, token or Java SDK
is needed. Ordinary `select` and `where` operations work on the result.

The caller retains the snapshot and all referenced data, delete and index files
until every job using them finishes. The reader does not create snapshots,
acquire or renew service pins, select a latest/named/as-of version, or delete
files. This AWS example uses the process credential chain on both driver and
executors. Native libraries must be available as described in
[Native dependency bundle](#native-dependency-bundle).

### 1.1 How it works

Table loading resolves one immutable `Snapshot` for its schema, selected
segments, statistics and scan plan. A scan never requests a newer view.
`inferSchema` resolves schema separately before table loading. Every driver
object store opened to materialize metadata closes on success or failure,
before executor tasks are serialized. Reader factories carry presentation
options; each segment task binds its storage identity.

A valid empty snapshot keeps its schema and returns zero rows. Missing required
identity, timestamp or schema, and declared segments without V3 manifests, are
rejected. Each Spark partition reads one data segment through its pinned V3
manifest. The driver sends delete-file descriptors rather than decoded
primary-key maps. Each executor reads and closes the applicable delete files,
then drops rows by primary key and timestamp. An unreadable delete file or a
segment yielding fewer rows than its declared count fails the task.

### 1.2 Metrics

Every scan reports its C/JVM boundary costs on the scan node of the Spark SQL
page: `milvus.jni.calls`, `milvus.jni.nanos`, `milvus.arrow.batches`,
`milvus.arrow.bytes` (Arrow bytes handed over), `milvus.copies` and
`milvus.copied.bytes` (columns copied because a native batch arrived sliced),
`milvus.rows.materialized` (rows converted to Spark rows; zero on the columnar
path), and `milvus.arrow.allocated.max` (allocator peak, maximum over tasks).
Segment-data bytes read from object storage are not included: those reads
happen inside milvus-storage.

## 2. `milvus` format parameters

### 2.1 Fixed snapshot inputs

Use `milvus.snapshot.path` for every nonempty snapshot. Explicit schema options
can describe an empty snapshot only.

| Option | Default | Meaning |
| --- | --- | --- |
| `milvus.snapshot.path` | unset | Fixed snapshot JSON location: a standard object-store URI, bucket-relative key, or local path for a local backend. Schema, partitions, deletes, indexes and segments come from this file. |
| `milvus.snapshot.mode` | inferred | Inferred as `true` from a nonblank path or manifest list. Set `true` explicitly for a schema-only empty snapshot. `false` does not enable a different input mode. |
| `milvus.snapshot.manifests` | unset | Only the empty JSON array `[]` is accepted, with schema bytes or schema JSON below. Nonempty lists are rejected: use `milvus.snapshot.path`. Mutually exclusive with `milvus.snapshot.path`. |
| `milvus.snapshot.schema.bytes` | unset | Base64-encoded protobuf `CollectionSchema` bytes for an empty snapshot. Takes precedence over schema JSON when both are supplied. |
| `milvus.snapshot.schema.json` | unset | Raw snapshot JSON containing the collection schema, used when schema bytes are absent. This is not Spark `StructType` JSON. |
| `milvus.snapshot.collection.id` | 0 when absent | Collection ID for explicit metadata; a supplied value must be a positive Long. |
| `milvus.snapshot.partition.ids` | unset | Comma-separated nonnegative partition IDs for explicit metadata. Use `milvus.partitions` below to select among a resolved snapshot's partitions. |
| `milvus.snapshot.max.json.bytes` | 67108864 | Positive maximum size of a snapshot JSON file, in bytes. |

A standard object-store URI uses `s3a://`, `s3://`, `gs://` or `oss://` with
the bucket as its authority. A bucket-relative key uses `fs.bucket_name`.
Milvus-produced `https://<endpoint>/<bucket>/<key>` and
`minio://<endpoint>/<bucket>/<key>` snapshot locations are
recognized by an explicit endpoint port or a host matching the configured
endpoint. Their bucket must match the configured input. A local backend
accepts local paths or `file:` URIs and rejects bucket-bearing locations.
Unsupported schemes and cross-bucket references fail during planning.

A manifest entry has a numeric `segmentID` (or `segmentIDLong`) and a `manifest`
string containing JSON with `ver` and `base_path`. This is the V3 manifest
representation named `storagev2_manifest_list` in Milvus snapshot JSON; that
wire name does not mean Storage V2 support. Invalid manifest JSON, empty base
paths and negative versions fail during planning; a bare path does not select
the latest version. Version `0` is Milvus Storage's initial empty state and
requires no manifest file. Positive versions read the exact file named by the
snapshot. An Avro manifest with no records is invalid; one record with empty
fields is valid, including an L0 manifest with no column groups.

The snapshot file supplies segment Avro records for the segment level,
partition, source channel, row count and persisted index metadata. Missing,
duplicate or unmatched segment records fail during planning, before segment
selection can hide an L0 record. Existing callers using a nonempty explicit
manifest list must switch to `milvus.snapshot.path`: a list of storage
manifests alone cannot identify L0 segments or their delete scope.
Supplying Spark `.schema()` does not replace the Milvus schema: field names,
types and selection must match the fixed input.

Options not listed in this reference are not read. The generic Spark `path`
argument is not an input: provide `milvus.snapshot.path`. Use the canonical
`fs.*` keys below for storage location and credentials.

### 2.2 Storage location and credentials

Use the same storage location and identity for metadata and segment reads.
Credentials must be available to both driver and executors. With static keys,
set `spark.redaction.regex=(?i)secret|password|token|access[._]?key` before
starting Spark; preserve any additional rules your environment already uses.
The default Spark pattern does not match the underscore spelling of these keys.

| Canonical property | Meaning |
| --- | --- |
| `fs.storage_type` | `remote` by default; `local` for files available at the same path to driver and executors. Local storage does not use a bucket, endpoint or credentials. |
| `fs.address` | Storage endpoint; required for remote storage after option/Hadoop resolution. |
| `fs.bucket_name` | Bucket bound to the input. A standard snapshot URI can supply it; a conflicting bucket fails. |
| `fs.root_path` | Milvus storage root, `files` by default. Set it to the root used by your snapshot. |
| `fs.cloud_provider` | Backend understood by milvus-storage, such as `aws`. |
| `fs.region` | Region used by storage requests. Set it explicitly for your deployment. |
| `fs.use_ssl` | Whether the storage connection uses TLS; an endpoint URL's scheme also participates in resolution as described below. |
| `fs.use_iam` | Use the native default credential chain instead of static keys. |
| `fs.access_key_id`, `fs.access_key_value` | Paired long-term static credentials when neither IAM nor a role is selected. |
| `fs.role_arn`, `fs.session_name`, `fs.external_id` | AssumeRole identity and optional session fields. |
| `fs.use_virtual_host` | Virtual-host access; the inverse of path-style access. |

Additional native `fs.*` properties, such as `fs.ssl_ca_cert`, pass to
milvus-storage and are validated there. No Milvus service TLS configuration is
used. Storage options are per input; reading one input does not change the
session's shared Hadoop configuration.

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
`https://<endpoint>/<bucket>/<key>` and `minio://<endpoint>/<bucket>/<key>`
locations are recognized against, including
an endpoint that only the Hadoop configuration supplies.
Milvus metadata may spell an object as
`s3://endpoint:port/bucket/key`. The connector recognizes that form only while
decoding Milvus-produced metadata (an explicit authority port, or an authority
host equal to the configured endpoint host), and normalizes it to the same `(bucket,
key)` as `s3a://bucket/key`. A user-supplied standard S3 URI always treats its
authority as the bucket. Path-style access is taken from
`fs.s3a.path.style.access`, then `s3.pathStyleAccess`, then the inverse of
`fs.use_virtual_host`; each setting is strictly boolean.
Snapshot JSON, Avro, V3 manifests, data files, and delete files
must all normalize to the snapshot's one bucket; a cross-bucket reference
fails during planning.

### 2.3 Data Operation Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MilvusOption.MilvusPartitions` (`milvus.partitions`) | String | No | unset | Comma-separated numeric partition IDs. The selector is applied after any snapshot source resolves. Every requested ID must exist; duplicates are ignored without changing the first-seen order. |
| `MilvusOption.MilvusSegments` (`milvus.segments`) | String | No | unset | Comma-separated numeric segment IDs. It may be combined with `milvus.partitions`, in which case the scan reads their intersection. Every requested ID must exist. |
| `MilvusOption.ReaderFieldIDs` (`fieldIDs`) | String | No | unset | Comma-separated numeric field IDs, applied during both schema inference and table creation. Each requested ID must exist in the snapshot schema; Spark projection can further prune this set. With an external `.schema()`, its non-metadata fields must select exactly these IDs and their names and Spark types must match the snapshot. |
| `MilvusOption.MilvusExtraColumns` (`milvus.extra.columns`) | String | No | "" | Comma-separated metadata columns. The supported names are `_segment_id`, `_row_offset`, and `_timestamp`; see [Output schema](#output-schema). |
| `MilvusOption.MilvusFilter` (`milvus.filter`) | String | No | unset | Milvus scalar expression for an ordinary table read. It is parsed and validated against the fixed snapshot during planning, evaluated in both row and columnar readers, and combined with Spark predicates and deletes before Limit. |
| `MilvusOption.ReadMode` (`milvus.read.mode`) | String | No | unset | What a load reads. Unset reads the collection's live rows. `physical` reads every stored row, rows marked deleted included, which is what a [column replacement](column-replacement.md) must write back; it cannot be combined with `milvus.read.apply.deletes=true`. `write_result` reads the receipt a column replacement published at the load path instead of collection data. |
| `MilvusOption.ReadIncludeRowId` (`milvus.read.include_row_id`) | Boolean | No | false | Adds `_milvus_row_id` to a `physical` read; see [Output schema](#output-schema). The snapshot must carry a positive `snapshot_info.id`. |
| `MilvusOption.ReadApplyDeletes` (`milvus.read.apply.deletes`) | Boolean | No | true | Apply all segment-local and applicable L0 deletes visible in the fixed snapshot. L0 deletes apply within the same source channel and partition; partition `-1` covers all partitions of that channel. Setting this to `false` is explicit opt-out; any provided value besides `true` or `false`, including a blank value, is rejected. |
| `milvus.read.vector.raw` | Boolean | No | false | Output type for vector columns. With the default `false`, vectors are converted to native Spark types (`FloatVector`/`Float16Vector`/`BFloat16Vector` to `ArrayType(FloatType)`, `Int8Vector` to `ArrayType(ShortType)`, `SparseFloatVector` to `MapType(LongType, FloatType)`). Set to `true` and vector columns come out as `BinaryType`, the bytes exactly as stored, for the caller to decode using `dim` and the element type. That path does no per-element conversion, which suits batch jobs that hand the bytes straight to a native library |
| `milvus.read.columnar` | Boolean | No | true | How the scan delivers rows. With the default `true` Spark gets whole Arrow batches (`ColumnarBatch`) that wrap the native buffers without copying, with vector columns typed as `milvus.read.vector.raw` decides; a batch with deleted rows is delivered through a position map over the surviving rows, still without copying. `false` delivers one row at a time. Row and columnar readers use the same expected-row guard. |
| `milvus.read.batch.max.rows` | Int | No | 8192 | Positive maximum rows requested from milvus-storage for one record batch; delivered as `reader.record_batch_max_rows`. |
| `milvus.read.batch.max.bytes` | Long | No | 33554432 | Positive target byte limit for one native record batch; delivered as `reader.record_batch_max_size`. The current upstream maximum is 4294967296 (4 GiB). |
| `milvus.read.arrow.max.bytes` | Long | No | 9223372036854775807 | Positive hard limit of the Arrow child allocator owned by one Spark read task. It covers imported/read Arrow buffers on row, columnar and vector paths, but not milvus-storage's separate native memory pool. |

Provided selector lists reject blanks, empty entries and non-numeric values.
Boolean read options (`milvus.snapshot.mode`, `milvus.read.apply.deletes`,
`milvus.read.include_row_id`, `milvus.read.vector.raw`, `milvus.read.columnar`)
accept only `true` or `false`,
case-insensitively. Positive integer/long options reject blanks, zero, negative,
non-decimal and overflowing values, reporting the key and supplied value.

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
result schema. The DataSource V1 Filter API is not
supported.

#### Milvus scalar filter

An ordinary table read can use the same scalar Milvus expression subset:

```scala
spark.read
  .format("milvus")
  .options(snapshotOptions)
  .option("milvus.filter", "category == 1 and rating >= 2.0")
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

JSON paths, Array predicates, and `json_contains`
are not in the current scalar subset.

## Output schema

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
- `_milvus_row_id` (non-null struct of `snapshot_id`, `segment_id` and
  `row_offset`, all `LongType`) last, when `milvus.read.include_row_id=true`
  asks for it. It binds the row to the snapshot it was read from and to its
  stored position, and a [column replacement](column-replacement.md) consumes
  it. Callers carry it unchanged and do not interpret its fields.

The connector does not expose a `partition` metadata column. Partition and
segment selection uses `milvus.partitions` and `milvus.segments`.

## Supported data types

### Scalar types

- Bool (`BooleanType`)
- Int8, Int16, Int32, Int64 (`ByteType`, `ShortType`, `IntegerType`, `LongType`)
- Float, Double (`FloatType`, `DoubleType`)
- String, VarChar, Text, JSON (`StringType`)

### Vector types

- FloatVector
- Float16Vector
- BFloat16Vector
- BinaryVector
- Int8Vector
- SparseFloatVector

### Complex types

- Array of Bool, Int8, Int16, Int32, Int64, Float, Double, String, or VarChar

This is a closed support list. Geometry, Timestamptz,
ArrayOfVector/struct-array fields, and unknown future Milvus types are not
silently converted to binary or null: schema resolution or value conversion
fails explicitly and reports the unsupported type.

## Native dependency bundle

Use one matching Storage platform bundle on both driver and executor classpaths.
Native manifest format 2 lives under `native/milvus/2/<platform>/`; each binding
publishes its component requirement under `META-INF/milvus-native/requirements/`.
A missing component, mismatched source revision or corrupt library fails loading.
Thin JARs contain no native libraries. See [releasing](releasing.md) for artifact
selection and [contributing](contributing.md) for native build checks.
