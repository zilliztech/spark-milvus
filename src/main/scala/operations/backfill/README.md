# Backfill API

The Backfill API adds new fields to existing Milvus collections by joining
snapshot rows with new field values and writing per-segment binlog files
directly into Milvus storage. The collection primary key remains the default
join key; snapshot mode can instead use an explicitly selected persisted
physical field. It can run as a programmatic API
from Scala/PySpark, or as a standalone `spark-submit` job (for example on
Apache Spark Operator / Kubernetes).

## Modes

The backfill operation has two read modes for the original collection:

1. **Snapshot mode (recommended)** — `MilvusBackfill.run` is given a Milvus
   snapshot manifest JSON. The selected join column and segment metadata are
   read directly from S3 via the storage-v2 FFI reader. **No connection to a
   running Milvus server is required.** Field IDs are derived from the
   snapshot's collection schema.
2. **Client mode** — when no snapshot path is provided, the connector falls
   back to talking to a Milvus server (`milvus.uri` / `milvus.token`) to fetch
   the schema and segment list. ADDFIELD writes still require field-ID
   mapping, so client mode currently rejects ADDFIELD backfill — provide a
   snapshot.

## Spark-submit entry point: `BackfillApp`

`com.zilliz.spark.connector.operations.backfill.BackfillApp` is the main
class for running backfill as a Spark application:

```bash
spark-submit \
  --class com.zilliz.spark.connector.operations.backfill.BackfillApp \
  spark-connector-assembly-<branch>-amd64-SNAPSHOT.jar \
  --parquet      s3a://source-bucket/new_fields.parquet \
  --snapshot     s3a://milvus-bucket/snapshots/foo.json \
  --s3-endpoint  s3.us-west-2.amazonaws.com \
  --s3-bucket    milvus-bucket \
  --s3-region    us-west-2 \
  [--s3-cloud-provider aws|aliyun|gcp|azure|tencent|huawei] \
  [--s3-access-key AKIA... --s3-secret-key ...] \
  [--s3-use-ssl] \
  [--use-iam] \
  [--source-s3-endpoint   s3.us-east-1.amazonaws.com] \
  [--source-s3-access-key ... --source-s3-secret-key ...] \
  [--source-s3-use-ssl] \
  [--source-use-iam] \
  [--source-s3-region us-east-1] \
  [--batch-size 1024] \
  [--output-result s3a://milvus-bucket/backfill/result.json] \
  [--join-key external_row_id] \
  [--mode replace|coalesce|overwrite]
```

### Authentication and dual-bucket credentials

- **Static credentials**: pass `--s3-access-key` / `--s3-secret-key`. These
  are forwarded to both Spark's Hadoop S3A client (per-bucket
  `fs.s3a.bucket.<bucket>.*`) and the Milvus storage FFI.
- **IAM / IRSA**: pass `--use-iam`, or simply omit both AK/SK. `BackfillApp`
  auto-enables `useIam` when both keys are empty, so no flag is required
  under IRSA. In IAM mode the connector honors the platform-injected Hadoop
  AssumeRole configuration: `fs.s3a.assumed.role.*` on AWS and
  `fs.oss.assumed.role.*` on Alibaba Cloud. A global main-storage role is also
  forwarded to the Milvus storage FFI; otherwise both clients use their default
  IAM chain (env vars / web identity token / instance profile).
- **Cloud provider**: omit `--s3-cloud-provider` for AWS. Pass `aliyun` when
  the Milvus storage bucket is OSS so the native writer uses the Aliyun
  credentials provider and AssumeRole flow.
- **Different bucket for input parquet**: when the parquet file lives in a
  different bucket (or even region/account) from the Milvus storage bucket,
  use the `--source-s3-*` flags. They are written as per-bucket Hadoop S3A
  config, so each bucket can independently use static AK/SK or IAM in the
  same Spark session. Any unset `--source-*` falls back to the main
  credentials.

### Required flags

| Flag           | Description                                       |
|----------------|---------------------------------------------------|
| `--parquet`    | Path to the new-field parquet (join key + target fields) |
| `--snapshot`   | Path to the Milvus snapshot manifest JSON          |
| `--s3-endpoint`| S3 endpoint for the Milvus storage bucket          |
| `--s3-bucket`  | Milvus storage bucket name                         |

### Optional flags

| Flag              | Default     | Description                                                                  |
|-------------------|-------------|------------------------------------------------------------------------------|
| `--mode`          | `coalesce`  | Merge semantics: `replace`, `coalesce` (fill-if-null), or `overwrite`. See "Merge modes" below. |
| `--batch-size`    | `1024`      | Rows per Arrow batch flushed to the writer.                                  |
| `--column-mapping`| *(none)*    | `src1:tgt1,src2:tgt2,...`. Rename/drop Parquet columns to Milvus field names. |
| `--join-key`      | collection PK | Exact persisted snapshot field to use instead of the collection PK. |
| `--output-result` | *(none)*    | Path to write the result JSON.                                               |
| `--s3-cloud-provider` | `aws` | Native storage provider for the Milvus storage bucket: `aws`, `aliyun`, `gcp`, `azure`, `tencent`, or `huawei`. |

## Join keys and column mapping

Omitting `--join-key` preserves the historical primary-key behavior. Without
`--column-mapping`, the input must contain a literal `pk` column, which is
implicitly renamed to the collection PK field.

`--join-key external_row_id` selects that exact persisted snapshot field. With
no mapping, the parquet must contain a column named `external_row_id`. If the
input uses a different name, map it to the selected field:

```bash
--join-key external_row_id \
--column-mapping source_row_id:external_row_id,new_vec:embedding
```

The selected join field is join-only and is not written as a target field. It
must be declared non-nullable in the snapshot schema. The parquet key must be
unique and non-null so each physical source row produces at most one output
row. Source values may repeat: one parquet record is applied to every physical
source row with the same key. Its Spark type must match exactly. Supported
physical-key Milvus types are Int8, Int16, Int32, Int64, String, and VarChar.
Floating-point, JSON, Geometry, Text, Timestamptz, unknown, array/struct/map,
and vector fields are rejected. Logical file/row keys are not supported;
`$row_offset` is used only to restore physical segment order and is not a
stable logical row ID.

Backfill targets must be ordinary writable collection fields. Primary keys,
partition keys, dynamic fields, function-output fields, and Milvus system
fields are rejected before source data is read.

## Vector columns

Backfill accepts the same practical shapes commonly used by Milvus import
clients, plus already-encoded Milvus bytes:

| Milvus type | Accepted Parquet/Spark values |
|-------------|-------------------------------|
| `FloatVector` | Numeric array of length `dim`; JSON array string; or little-endian internal binary (`dim * 4` bytes). |
| `BinaryVector` | Packed integral-byte array or binary of length `dim / 8`; JSON byte-array string. |
| `Float16Vector`, `BFloat16Vector` | Float/double array of length `dim`; encoded byte/short array or binary of length `dim * 2`; JSON numeric-array string. |
| `Int8Vector` | Integral array of length `dim` with values in `[-128, 127]`; JSON integral-array string; or internal binary. |
| `SparseFloatVector` | Map of index to non-negative weight; `{indices, values}` struct; JSON object string; or Milvus sparse binary. |

Before the join, vector values are validated and normalized to Milvus's
per-row byte layout. Non-nullable dense vectors are written as Arrow
`FixedSizeBinary`; nullable dense vectors and sparse vectors are written as
Arrow `Binary`, matching Milvus storage metadata and endianness.

## Merge modes (`--mode`)

Distinct from the read-mode choice above (snapshot vs client), `--mode`
controls how per-row values are merged into each target field. All three
modes use a LEFT JOIN from source (Milvus) to parquet on the resolved join key;
parquet rows whose key is not in the collection are always dropped. They differ on
what happens for matched rows and for source rows with no parquet match:

| Mode        | Default | Matched row (join key in both)   | Source row unmatched by parquet |
|-------------|---------|----------------------------------|---------------------------------|
| `replace`   |         | Parquet wins (null included)     | Target columns set to NULL      |
| `coalesce`  | ✅      | `coalesce(src, parquet)` per field — source wins when non-null | Source preserved |
| `overwrite` |         | Parquet wins (null included)     | Source preserved                |

Typical fits:

- `replace` — fresh backfill of a brand-new field where parquet is the
  full authoritative source.
- `coalesce` — incremental / repair runs that only want to fill gaps.
- `overwrite` — corrective update for a subset of rows; parquet is
  authoritative only for the join keys it covers and untouched rows must be
  preserved.

All modes require exact Parquet-to-Milvus target types; perform required casts
in the input ETL. Additional `coalesce` / `overwrite` caveats (both read
source-side target values):

- Require `--snapshot` — each target field is read from the snapshot so
  the merge can compare against the existing value. Client mode (no
  `--snapshot`) cannot use these and must pass `--mode replace` explicitly.
- Slightly heavier I/O than `replace` because the existing field is read
  per segment.
- On Storage V2 packed segments, target fields must already exist in the
  segment's column groups. Use `replace` for the initial backfill of a newly
  added field.
- For `overwrite`: an explicitly null value in the parquet **will** clobber
  the existing source value on matched rows — the match flag drives the
  projection, not the file column's null-ness.

See `docs/user-guide-snapshot-backfill.md` §6 for deeper discussion and
worked examples.

## Programmatic API

```scala
import org.apache.spark.sql.SparkSession
import com.zilliz.spark.connector.MilvusOption
import com.zilliz.spark.connector.operations.backfill._

val spark = SparkSession.builder().appName("Backfill").getOrCreate()

val config = BackfillConfig(
  // Optional in snapshot mode — leave empty when using --snapshot
  milvusUri      = "",
  milvusToken    = "",
  databaseName   = "default",
  collectionName = "",

  // Milvus storage bucket (writes + snapshot reads)
  s3Endpoint     = "s3.us-west-2.amazonaws.com",
  s3BucketName   = "milvus-bucket",
  s3AccessKey    = "",          // empty => IAM/IRSA
  s3SecretKey    = "",
  s3UseIam       = true,
  s3Region       = "us-west-2",
  s3RootPath     = "files",
  s3UseSSL       = true,

  // Optional: separate bucket for the *input* parquet
  sourceS3Endpoint  = Some("s3.us-east-1.amazonaws.com"),
  sourceS3UseIam    = Some(true),
  sourceS3Region    = Some("us-east-1"),

  batchSize = 1024,

  // Merge mode. Defaults to "coalesce" (fill-if-null). Set to
  // MilvusOption.BackfillModeOverwrite for matched-rows-only overwrite
  // (parquet wins on matched join keys, unmatched source rows preserved), or
  // MilvusOption.BackfillModeReplace for full overwrite (unmatched source
  // rows get null target columns). See "Merge modes" above.
  mode = MilvusOption.BackfillModeCoalesce,

  // Optional. Omit for the collection PK.
  joinKey = BackfillJoinKey.PhysicalField("external_row_id")
)

val result = MilvusBackfill.run(
  spark            = spark,
  backfillDataPath = "s3a://source-bucket/new_fields.parquet",
  snapshotPath     = "s3a://milvus-bucket/snapshots/foo.json",
  config           = config
)

result match {
  case Right(success) =>
    println(success.summary)
    println(success.segmentSummary)
  case Left(error) =>
    println(s"Backfill failed: ${error.message}")
    error.cause.foreach(_.printStackTrace())
}
```

## Configuration reference

### Required

- `s3Endpoint`, `s3BucketName` — Milvus storage bucket.
- `s3AccessKey`, `s3SecretKey` — may be empty when `s3UseIam = true`.
- `s3CloudProvider` — native storage provider for the Milvus storage bucket
  (default `aws`).

### Optional

- `milvusUri`, `milvusToken`, `databaseName`, `collectionName` — required
  only in client mode (no snapshot).
- `partitionName` — backfill a specific partition.
- `s3UseIam` — use the provider IAM chain instead of static AK/SK.
- `s3UseSSL`, `s3RootPath`, `s3Region` — standard S3 options.
- `sourceS3Endpoint`, `sourceS3AccessKey`, `sourceS3SecretKey`,
  `sourceS3UseSSL`, `sourceS3UseIam`, `sourceS3Region` — overrides for the
  input parquet bucket. Any field left as `None` falls back to the
  corresponding main `s3*` value.
- `batchSize` — writer batch size (default 1024).
- `customOutputPath` — override the per-segment output path.
- `joinKey` — `BackfillJoinKey.PrimaryKey` by default, or
  `BackfillJoinKey.PhysicalField("field_name")` for a persisted snapshot field.
- `columnMapping` — parquet-column to Milvus-field mapping. It must include the
  resolved join field as a target when supplied.
- `mode` — merge semantics (default `MilvusOption.BackfillModeCoalesce`).
  Set to `MilvusOption.BackfillModeOverwrite` for matched-rows-only
  overwrite, or `MilvusOption.BackfillModeReplace` for full overwrite
  (unmatched source rows get null target columns). See "Merge modes"
  above for caveats.

## Error handling

`MilvusBackfill.run` returns `Either[BackfillError, BackfillResult]`:

```scala
result match {
  case Right(success)              => /* process result */
  case Left(ConnectionError(m, _)) => println(s"Connection error: $m")
  case Left(SchemaValidationError(m, _)) => println(s"Schema error: $m")
  case Left(DataReadError(p, m, _))  => println(s"Read error at $p: $m")
  case Left(WriteError(seg, p, m, _)) =>
    println(s"Write error for segment $seg at $p: $m")
  case Left(other) => println(s"Error: ${other.message}")
}
```

## Output structure

When using a snapshot, each segment is written under the manifest's
`basePath` (so binlogs land alongside the existing segment files). When no
basePath is available, the default layout is used:

```
s3://{bucket}/{rootPath}/insert_log/{collectionID}/{partitionID}/{segmentID}/new_field/
```

Override via `customOutputPath` if needed.

## How it works

1. Validate S3/writer configuration. Empty AK/SK is allowed (IAM/IRSA).
2. Load the snapshot manifest JSON (per-bucket S3A credentials are
   configured automatically).
3. Resolve the configured join field and field ID from the snapshot schema.
   The default PK can also be resolved through the Milvus client in client mode;
   explicit physical keys require a snapshot.
4. Read the new-field parquet, configuring S3A credentials for the source
   bucket as needed.
5. Read the selected join column + `$segment_id` / `$row_offset` via
   `spark.read.format("com.zilliz.spark.connector.sources.MilvusDataSource")`
   in snapshot mode (FQCN avoids shortName collisions with other connectors).
6. Validate join-key type compatibility and parquet-side cardinality, then
   left-join on the internal normalized key alias. Source keys may repeat.
7. For each segment, repartition with a custom segment partitioner, sort by
   `$row_offset`, and write per-segment binlogs via `MilvusLoonWriter`.
8. Return a `BackfillResult` with manifest paths and per-segment stats.

## Testing helper

```scala
val config = BackfillConfig.forTest(collectionName = "test_collection")
```

`forTest` produces a localhost / Minio config suitable for the integration
tests in `MilvusBackfillTest`. Unit tests for `parseArgs`, the new
`validate()` invariants and `configureHadoopS3ForPath` live in
`BackfillAppTest`.

## API location

```
com.zilliz.spark.connector.operations.backfill.BackfillApp
com.zilliz.spark.connector.operations.backfill.MilvusBackfill
com.zilliz.spark.connector.operations.backfill.BackfillConfig
com.zilliz.spark.connector.operations.backfill.BackfillJoinKey
com.zilliz.spark.connector.operations.backfill.BackfillResult
com.zilliz.spark.connector.operations.backfill.BackfillError
```
