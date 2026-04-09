# Backfill API

The Backfill API adds new fields to existing Milvus collections by joining
the original primary-key column with new field values and writing per-segment
binlog files directly into Milvus storage. It can run as a programmatic API
from Scala/PySpark, or as a standalone `spark-submit` job (for example on
Apache Spark Operator / Kubernetes).

## Modes

The backfill operation has two read modes for the original collection:

1. **Snapshot mode (recommended)** — `MilvusBackfill.run` is given a Milvus
   snapshot manifest JSON. The original PK column and segment metadata are
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
  spark-connector-assembly.jar \
  --parquet      s3a://source-bucket/new_fields.parquet \
  --snapshot     s3a://milvus-bucket/snapshots/foo.json \
  --s3-endpoint  s3.us-west-2.amazonaws.com \
  --s3-bucket    milvus-bucket \
  --s3-region    us-west-2 \
  [--s3-access-key AKIA... --s3-secret-key ...] \
  [--s3-use-ssl] \
  [--use-iam] \
  [--source-s3-endpoint   s3.us-east-1.amazonaws.com] \
  [--source-s3-access-key ... --source-s3-secret-key ...] \
  [--source-s3-use-ssl] \
  [--source-use-iam] \
  [--source-s3-region us-east-1] \
  [--batch-size 1024] \
  [--output-result s3a://milvus-bucket/backfill/result.json]
```

### Authentication and dual-bucket credentials

- **Static credentials**: pass `--s3-access-key` / `--s3-secret-key`. These
  are forwarded to both Spark's Hadoop S3A client (per-bucket
  `fs.s3a.bucket.<bucket>.*`) and the Milvus storage FFI.
- **IAM / IRSA**: pass `--use-iam`, or simply omit both AK/SK. `BackfillApp`
  auto-enables `useIam` when both keys are empty, so no flag is required
  under IRSA. In IAM mode the connector defers to
  `DefaultAWSCredentialsProviderChain` (env vars / web identity token /
  instance profile).
- **Different bucket for input parquet**: when the parquet file lives in a
  different bucket (or even region/account) from the Milvus storage bucket,
  use the `--source-s3-*` flags. They are written as per-bucket Hadoop S3A
  config, so each bucket can independently use static AK/SK or IAM in the
  same Spark session. Any unset `--source-*` falls back to the main
  credentials.

### Required flags

| Flag           | Description                                       |
|----------------|---------------------------------------------------|
| `--parquet`    | Path to the new-field parquet (`pk, field1, ...`) |
| `--snapshot`   | Path to the Milvus snapshot manifest JSON          |
| `--s3-endpoint`| S3 endpoint for the Milvus storage bucket          |
| `--s3-bucket`  | Milvus storage bucket name                         |

## Programmatic API

```scala
import org.apache.spark.sql.SparkSession
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

  batchSize = 1024
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

### Optional

- `milvusUri`, `milvusToken`, `databaseName`, `collectionName` — required
  only in client mode (no snapshot).
- `partitionName` — backfill a specific partition.
- `s3UseIam` — use the AWS default credentials chain instead of static AK/SK.
- `s3UseSSL`, `s3RootPath`, `s3Region` — standard S3 options.
- `sourceS3Endpoint`, `sourceS3AccessKey`, `sourceS3SecretKey`,
  `sourceS3UseSSL`, `sourceS3UseIam`, `sourceS3Region` — overrides for the
  input parquet bucket. Any field left as `None` falls back to the
  corresponding main `s3*` value.
- `batchSize` — writer batch size (default 1024).
- `customOutputPath` — override the per-segment output path.

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
3. Resolve PK field name and field ID from the snapshot schema (or via the
   Milvus client in client mode).
4. Read the new-field parquet, configuring S3A credentials for the source
   bucket as needed.
5. Read the original PK column + `segment_id` / `row_offset` via
   `spark.read.format("com.zilliz.spark.connector.sources.MilvusDataSource")`
   in snapshot mode (FQCN avoids shortName collisions with other connectors).
6. Validate PK type compatibility, then left-join on the PK.
7. For each segment, repartition with a custom segment partitioner, sort by
   `row_offset`, and write per-segment binlogs via `MilvusLoonWriter`.
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
com.zilliz.spark.connector.operations.backfill.BackfillResult
com.zilliz.spark.connector.operations.backfill.BackfillError
```
