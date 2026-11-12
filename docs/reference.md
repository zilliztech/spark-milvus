# Connector reference

The reader consumes fixed Storage V3 snapshots without service calls; the
[column replacement](column-replacement.md) writes new columns of such a
snapshot back from a DataFrame. Both support the Spark/Scala combinations in
[README](../README.md#requirements-and-compatibility).
Use a published 3.0 version, or a verified candidate built from this source.
Follow the [installation guide](../README.md#installing) to select matching
artifacts; CI candidates are not automatically published to Maven Central.

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

Pass connector options to `.option("key", "value")` or `.options(...)` on the
DataFrame reader or writer. Connector read/write option names are
case-insensitive; use the canonical lowercase spellings for native storage
properties. For a normal read, start with `milvus.snapshot.path` and the
storage settings in [section 2.2](#22-storage-location-and-credentials); the read options in
[section 2.3](#23-data-operation-parameters) are optional.

### 2.1 Fixed snapshot inputs

Use `milvus.snapshot.path` for every nonempty Snapshot. Its file supplies the
schema and data locations, so you do not need to provide schema, collection ID
or partition IDs separately.

| Option | Default | Meaning |
| --- | --- | --- |
| `milvus.snapshot.path` | unset | Fixed snapshot JSON location: a standard object-store URI, bucket-relative key, or local path for a local backend. Schema, partitions, deletes, indexes and segments come from this file. |
| `milvus.snapshot.max.json.bytes` | 67108864 (64 MiB) | Positive maximum size of the snapshot JSON file, in bytes. Increase it only when your snapshot metadata needs a larger limit. |

The following options describe an **empty Snapshot** without a snapshot file.
They are not needed for ordinary reads. When `milvus.snapshot.path` is set,
schema, collection ID and partition metadata are read from that file instead
of these explicit metadata options.

| Option | Default | Meaning |
| --- | --- | --- |
| `milvus.snapshot.mode` | inferred | Inferred as `true` from a nonblank path or manifest list. Set `true` explicitly when supplying only schema metadata. Setting it to `false` rejects a snapshot read; it does not select an online read mode. |
| `milvus.snapshot.manifests` | unset | Only the empty JSON array `[]` is accepted, with schema bytes or schema JSON below. Nonempty lists are rejected: use `milvus.snapshot.path`. Mutually exclusive with `milvus.snapshot.path`. |
| `milvus.snapshot.schema.bytes` | unset | Base64-encoded protobuf `CollectionSchema` bytes for an empty snapshot. Takes precedence over schema JSON when both are supplied. |
| `milvus.snapshot.schema.json` | unset | Raw snapshot JSON containing the collection schema, used when schema bytes are absent. This is not Spark `StructType` JSON. |
| `milvus.snapshot.collection.id` | 0 when absent | Collection ID for the empty snapshot; a supplied value must be a positive Long. |
| `milvus.snapshot.partition.ids` | unset | Comma-separated nonnegative partition IDs for the empty snapshot. Use `milvus.partitions` below to select among a resolved snapshot's partitions. |

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

For snapshot data, supply `milvus.snapshot.path` and call `.load()` without a
path argument. Spark's `.load(path)` selects the result location only when
`milvus.read.mode=write_result` reads a column-replacement receipt. Use the
canonical `fs.*` keys below for storage location and credentials.

### 2.2 Storage location and credentials

Use the same storage location and identity for metadata and segment reads.
Credentials must be available to both driver and executors. With static keys,
set `spark.redaction.regex=(?i)secret|password|token|access[._]?key` before
starting Spark; preserve any additional rules your environment already uses.
The default Spark pattern does not match the underscore spelling of these keys.

| Canonical property | Default / resolution | Meaning |
| --- | --- | --- |
| `fs.storage_type` | `remote` with a resolved bucket; otherwise inferred `local` | Use `local` for files available at the same path to driver and executors. Local storage does not use a bucket, endpoint or credentials. |
| `fs.address` | Explicit option or Hadoop endpoint; no fallback | Storage endpoint. Required for remote storage after configuration is resolved. |
| `fs.bucket_name` | Snapshot URI or explicit configuration | Bucket bound to the input. Required for a bucket-relative remote key; a bucket that conflicts with the URI fails. |
| `fs.root_path` | `files` | Storage root, including the local filesystem root. It is not automatically prepended to remote Snapshot keys: include the full bucket-relative key in the path. |
| `fs.cloud_provider` | Hadoop translation or native default `aws` | Backend recognized by the bundled native library: `aws`, `gcp`, `aliyun`, `azure`, `tencent`, `huawei` or `volcengine`. Backend-specific credentials may require additional native properties. |
| `fs.region` | Hadoop region or fallback `us-west-2` | Region used by storage requests. Set your actual region explicitly. |
| `fs.use_ssl` | Resolved from explicit option, endpoint scheme or Hadoop SSL configuration | Whether storage requests use TLS. Set explicitly to match your endpoint; resolution details follow below. |
| `fs.use_iam` | `true` for remote storage when no explicit setting, complete static-key pair or role is resolved | Use the native credential chain. Explicit `false` disables that fallback and requires static keys unless a role is selected. |
| `fs.access_key_id`, `fs.access_key_value` | Unset or translated Hadoop keys | Paired long-term static credentials, used only when neither IAM nor a role is selected. |
| `fs.role_arn` | Unset or translated Hadoop role | AssumeRole identity. Uses the native chain for source credentials and takes precedence over static keys. |
| `fs.session_name`, `fs.external_id` | Unset | Optional role session name and external ID, where supported by the backend. |
| `fs.use_virtual_host` | Inherited addressing setting or native default `false` | Virtual-host access, the inverse of path-style access. Some cloud backends require virtual-host addressing. |

Set `fs.use_iam=true` to use credentials available in each driver and executor
process, such as an AWS role or AWS environment variables. For explicit
long-term keys, replace it with `fs.use_iam=false` and provide both
`fs.access_key_id` and `fs.access_key_value`. There is no `fs.session_token`
option; use the native credential chain for temporary AWS credentials.

Additional native `fs.*` properties, such as `fs.ssl_ca_cert`, and
`extfs.<name>.*` groups pass to milvus-storage for validation and interpretation.
Supported properties depend on the bundled native version and backend. Each
external group needs its own location and credential settings; IAM fallback
is not added to those groups. They do not enable cross-bucket references within
one Snapshot. Storage options are per input; reading one input does not change
the session's shared Hadoop configuration.

#### Existing Hadoop configuration and aliases

Prefer the canonical names above in new applications. For the bucket, legacy
aliases `fs.bucketName` and `s3.bucketName` are accepted after `fs.bucket_name`,
in that order.

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

All options in this section are optional. By default, a scan reads all live
rows and collection fields in the fixed Snapshot, converts vectors to Spark
types, and uses columnar batches. Spark's `select` and `where` are enough for
most applications; use these options when you need additional control.

#### Selecting rows and columns

| Option | Default | Meaning |
| --- | --- | --- |
| `milvus.partitions` | all partitions | Comma-separated nonnegative partition IDs, such as `10,11`. Every requested ID must exist. |
| `milvus.segments` | all data segments | Comma-separated nonnegative data-segment IDs. Every requested ID must exist. If `milvus.partitions` is also set, every requested segment must belong to those partitions; a segment outside them fails planning. Applicable delete-only segments are retained automatically. |
| `fieldIDs` | all collection fields | Comma-separated nonnegative field IDs. Each ID must exist in the snapshot schema, including its system fields. This limits the initial schema; Spark `select` can narrow it further. An explicit `.schema()` must select exactly these IDs with matching names and Spark types. |
| `milvus.filter` | no filter | Scalar expression such as `category == 1 and rating >= 2.0`. See [Milvus scalar filter](#milvus-scalar-filter) for the supported syntax. It combines with Spark filters and delete filtering before a row limit is applied. |
| `milvus.extra.columns` | none | Comma-separated metadata names: `_segment_id`, `_row_offset`, `_timestamp`. For example, `_segment_id,_row_offset`. See [Output schema](#output-schema). Request `_milvus_row_id` through `milvus.read.include_row_id` instead. |

Partition, segment and field-ID lists reject blanks, empty entries, negative
IDs and non-numeric values. Repeated IDs are ignored, preserving their
first-seen order. Metadata-column lists also ignore repeats; a blank value
requests no extra columns.

#### Read modes and output

| Option | Default | Meaning |
| --- | --- | --- |
| `milvus.read.mode` | unset: live rows | Leave unset for normal analysis. `physical` includes stored rows marked deleted and is required for [column replacement](column-replacement.md). `write_result` reads a replacement receipt using `.load(resultPath)`. Values are case-insensitive; a blank value behaves as unset. `live` is not an accepted value. |
| `milvus.read.apply.deletes` | `true`; `false` in physical mode | Apply segment-local and applicable L0 deletes from the snapshot. `false` includes deleted rows. Physical mode rejects `true`. L0 deletes apply within the same source channel and partition; partition `-1` covers all partitions of that channel. |
| `milvus.read.include_row_id` | `false` | Add `_milvus_row_id` for column replacement. Requires `milvus.read.mode=physical` and a positive `snapshot_info.id` in the snapshot. Keep this column unchanged when writing replacements. |
| `milvus.read.vector.raw` | `false` | `false` converts vectors to Spark arrays or maps as listed under [Supported data types](#supported-data-types). `true` returns stored vector bytes as `BinaryType`, for applications that decode them themselves or pass them to a native library. Binary vectors remain `BinaryType` in either mode. |
| `milvus.read.columnar` | `true` | Deliver Arrow batches to Spark. `false` uses the row-at-a-time reader. This does not change the output schema or delete semantics. |

`write_result` reads a fixed receipt schema, not snapshot data. Supply its
result path and storage options; snapshot selectors and the scan tuning
options below do not control this receipt read.

#### Batch size and memory

| Option | Default | Meaning |
| --- | --- | --- |
| `milvus.read.batch.max.rows` | 8192 | Positive Int: maximum rows requested for one native record batch. Passed to milvus-storage as `reader.record_batch_max_rows`. |
| `milvus.read.batch.max.bytes` | 33554432 (32 MiB) | Positive Long: target byte limit for one native record batch. Maximum 4294967296 (4 GiB). Passed as `reader.record_batch_max_size`; this is not a limit on total task memory. |
| `milvus.read.arrow.max.bytes` | 9223372036854775807 (`Long.MaxValue`) | Positive Long: hard limit of one read task's Arrow child allocator. Covers imported/read Arrow buffers, but not milvus-storage's separate native memory pool or other Spark memory. |

Boolean read options (`milvus.snapshot.mode`, `milvus.read.apply.deletes`,
`milvus.read.include_row_id`, `milvus.read.vector.raw`, `milvus.read.columnar`)
accept only `true` or `false`,
case-insensitively. Positive integer/long options reject blanks, zero, negative,
non-decimal and overflowing values, reporting the key and supplied value. Use
decimal byte counts such as `33554432`, not size strings such as `32m`.

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

### 2.4 Column replacement and receipts

The writer replaces the fields selected in the input DataFrame. It requires
`milvus.write.operation=replace_columns`, the original `milvus.snapshot.path`,
storage settings, and `.mode("append").save(resultPath)`. Use a fresh result
path for each write. The [write option table](column-replacement.md#write-options)
lists all accepted settings and the read options that the writer rejects.

To read the receipt, set `milvus.read.mode=write_result` and call
`.load(resultPath)` with storage options for that address. No Snapshot file is
required for this read. See [Receipt](column-replacement.md#receipt)
for its fixed schema and configuration.

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
