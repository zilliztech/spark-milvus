# User Guide — Offline Snapshot Backfill

This guide walks you through using the Spark-Milvus connector to **add a new
field to an existing Milvus collection and populate it for every existing
row**, entirely offline against object storage. Online reads and writes are
not interrupted.

If you want the architecture / internals, see
`design-snapshot-backfill.md`. This doc is task-oriented.

## 1. When to use it

Use snapshot backfill when you need to:

- Add a new **scalar**, **text**, or **JSON** field to a large collection and
  fill in values for rows that already exist.
- Re-compute a scalar/text/JSON field from an external system (e.g. an
  embedding classifier output, a new metadata column, a re-scored relevance
  label) without re-ingesting base data.
- Do the above without load on the Milvus cluster and without rebuilding
  indexes for existing fields.

**Not supported today:**

- Backfilling vector fields, primary-key columns, dynamic fields, or
  function fields.
- Collections whose sealed segments are still in the legacy V1 binlog
  format. You need a cluster that writes storage-V2 or V3 segments.

## 2. What you need

| Requirement                | Notes                                                                                           |
| -------------------------- | ----------------------------------------------------------------------------------------------- |
| Milvus server              | Snapshot + `UpdateSegmentColumnGroups` support required (any recent 2.6+ or internal build).    |
| Object storage             | S3 / MinIO / GCS with S3-compatible endpoint. Must be accessible from both Milvus and Spark.    |
| Spark                      | 3.5.x, JDK 8+. Cluster mode on YARN, Kubernetes (Spark Operator), or standalone all work.       |
| Connector JARs             | `spark-connector-assembly-*.jar`. It already bundles the native `milvus-storage` resources copied into `src/main/resources/native/`; see `CLAUDE.md` for the build order.|
| Parquet of new-field data  | Must contain the resolved join-key column, plus one column per new field.                      |
| Network                    | Spark executors must reach the object store. Milvus gRPC is only needed for step 1 and step 4.  |

## 3. The flow at a glance

```
┌─── on Milvus ────────────────────────────────────────────────────┐
│ 1. CreateSnapshot(collection)         → snapshot.json on S3      │
│ 2. AddCollectionField(new_field)      → schema gains the field   │
└──────────────────────────────────────────────────────────────────┘
                │
                ▼   snapshot.json + your parquet
┌─── Spark job ────────────────────────────────────────────────────┐
│ 3. Run BackfillApp                                               │
│    → writes new binlogs to S3                                    │
│    → emits backfill_result.json                                  │
└──────────────────────────────────────────────────────────────────┘
                │
                ▼   backfill_result.json
┌─── on Milvus ────────────────────────────────────────────────────┐
│ 4. For each segment: UpdateSegmentColumnGroups(...)              │
│ 5. (optional) CreateIndex(new_field)                             │
│ 6. QueryNode reopens segments automatically                      │
└──────────────────────────────────────────────────────────────────┘
```

Step 3 is the only part this guide covers in detail; the Milvus-side calls
are one-liners in your SDK of choice.

## 4. Prepare your Parquet input

The Parquet file must contain:

- The **join-key** column. By default the job joins on the collection primary
  key and expects a parquet column named `pk`; use `--column-mapping` if your
  source has a different name.
- One column per **new field** you want to backfill. Column names must
  match the Milvus field names (after any column mapping).

Example (new fields `category` and `score`, PK column in source is `doc_id`):

| doc_id | category | score |
| ------ | -------- | ----- |
| 1      | "news"   | 0.83  |
| 2      | "tech"   | 0.91  |

At submit time: `--column-mapping doc_id:pk_field,category:category,score:score`.

To join on a different persisted snapshot field, pass
`--join-key external_row_id`. With no mapping, the parquet must contain that
exact column name. With a differently named input column:

```text
--join-key external_row_id \
--column-mapping source_row_id:external_row_id,category:category,score:score
```

The physical field must exist exactly (including case) in the snapshot schema.
The parquet key must be non-null and unique so one source row cannot fan out
into multiple output rows. The snapshot field may repeat; the same parquet
record is applied to every matching physical source row. The join field is not
a target field, so you cannot backfill it in the same operation. Supported
physical-key Milvus types are Int8/16/32/64, String, and VarChar.
Floating-point, JSON, Geometry, Text, Timestamptz, unknown, vector, array, map,
and struct keys are rejected. Logical file/row keys are not supported.
`$row_offset` only restores segment write order and is not a stable row identity.

**Type rules:**

- Join-key type in Parquet must match the selected snapshot field type exactly
  (int64 ↔ Int64, varchar ↔ String).
- In `--mode coalesce` (default) or `--mode overwrite`, Parquet types must
  match the Milvus field types **exactly** — no widening. See "Modes" below.
- In `--mode replace`, Parquet types must be compatible with the Milvus
  field types (Spark will cast where sensible).

Missing join keys in the Parquet: behaviour depends on `--mode` (see §6).
Extra parquet keys not present in the collection are silently ignored.

## 5. Step-by-step

### 5.1 Take a snapshot

Call `CreateSnapshot` via any Milvus SDK. Example with `pymilvus`:

```python
from pymilvus import MilvusClient

client = MilvusClient(uri="http://milvus:19530")
client.create_snapshot(
    collection_name="my_collection",
    snapshot_name="bkfill_20260417",
    compaction_protection_seconds=86400,   # pin files for 24h
)

info = client.describe_snapshot(snapshot_name="bkfill_20260417")
snapshot_path = info["s3_location"]      # → s3a://bucket/snapshots/<coll>/metadata/<id>.json
```

`compaction_protection_seconds` tells Milvus to keep segment files pinned
for N seconds, long enough for backfill to run. Pick a value larger than
your expected job runtime; it is cheap.

### 5.2 Add the new field

```python
from pymilvus import DataType

client.add_collection_field(
    collection_name="my_collection",
    field_name="category",
    data_type=DataType.VARCHAR,
    max_length=64,
)
client.add_collection_field(
    collection_name="my_collection",
    field_name="score",
    data_type=DataType.FLOAT,
)
```

After this call, `describe_collection` will show the new fields, but
existing rows have no values for them.

> **Tip.** Do this **after** `CreateSnapshot`, not before — otherwise the
> snapshot contains the added field but no data for it, which confuses
> `coalesce` mode. Order: snapshot first, then add field.

### 5.3 Submit the Spark job

Minimal `spark-submit` example (standalone / YARN):

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.zilliz.spark.connector.operations.backfill.BackfillApp \
  --conf spark.executor.memory=8g \
  --conf spark.executor.memoryOverhead=8g \
  spark-connector-assembly-<branch>-amd64-SNAPSHOT.jar \
  --snapshot   s3a://bucket/snapshots/123/metadata/456.json \
  --parquet    s3a://bucket/input/new_fields.parquet \
  --s3-endpoint s3.us-west-2.amazonaws.com \
  --s3-bucket   bucket \
  --s3-root-path files \
  --s3-region   us-west-2 \
  --use-iam \
  --column-mapping doc_id:pk_field,category:category,score:score \
  --batch-size 1024 \
  --output-result s3a://bucket/backfill/result_20260417.json
```

For a physical key, add `--join-key external_row_id` and make the corresponding
column-mapping target `external_row_id`.

On Kubernetes with Spark Operator, see `examples/05-spark-application.yaml`.

Small datasets can use the PySpark demo wrapper:

```bash
cd examples
bash backfill_demo.sh \
  --parquet    data.parquet \
  --snapshot   snapshot.json \
  --s3-endpoint localhost:9000 \
  --s3-bucket  a-bucket \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin \
  --s3-root-path files
```

### 5.4 Read the result

The job emits `backfill_result.json` to `--output-result`. Shape:

```json
{
  "success": true,
  "collectionId": 465607967356115279,
  "segmentsProcessed": 5,
  "totalRowsWritten": 50000,
  "executionTimeMs": 123456,
  "newFieldNames": ["category", "score"],
  "segments": {
    "447000000000000001": {
      "rowCount": 10000,
      "executionTimeMs": 2500,
      "outputPath": "s3a://bucket/files/insert_log/...",
      "manifestPaths": ["s3a://bucket/.../_metadata/manifest-6.avro"],
      "version": 6
    },
    "448000000000000002": {
      "rowCount": 10000,
      "executionTimeMs": 2300,
      "outputPath": "s3a://bucket/files/insert_log/...",
      "manifestPaths": [],
      "storage_version": 2,
      "column_groups": [
        {
          "field_ids": [103],
          "binlog_files": ["insert_log/.../103/1234567"],
          "row_count": 10000
        }
      ]
    }
  }
}
```

Each segment entry is one of:

- **V3** — `version` + `manifestPaths`.
- **V2** — `storage_version: 2` + `column_groups[]`.

### 5.5 Commit to Milvus

Iterate the `segments` map and, for each entry, call
`UpdateSegmentColumnGroups` on Milvus. The shape depends on V2 vs V3:

```python
# Pseudocode — use your SDK's equivalent
for seg_id, seg in result["segments"].items():
    if "column_groups" in seg:          # V2
        client.update_segment_column_groups(
            segment_id=int(seg_id),
            column_groups={
                cg["field_ids"][0]: {
                    "fieldID": cg["field_ids"][0],
                    "binlogs": [{"log_path": p} for p in cg["binlog_files"]],
                }
                for cg in seg["column_groups"]
            },
        )
    else:                                # V3
        client.update_segment_manifest(
            segment_id=int(seg_id),
            manifest_version=seg["version"],
        )
```

Each call is idempotent at segment granularity; if one fails, retry that
one only.

### 5.6 (Optional) Build an index on the new field

Backfill does **not** build indexes. If you need one:

```python
client.create_index(
    collection_name="my_collection",
    field_name="category",
    index_type="Trie",       # or whatever suits the field
)
```

Milvus will index the new binlogs and re-reopen the segments when done.

### 5.7 Verify

- `describe_collection` → confirm schema includes new fields.
- `query(expr="pk in [...]", output_fields=["category", "score"])` → confirm values.
- `get_segment_info` → confirm every segment's `DataVersion` (or
  `manifest_version`) moved forward.

## 6. Modes: replace vs coalesce vs overwrite

Pass `--mode coalesce` (default), `--mode overwrite`, or `--mode replace`.

All three behave identically for matched rows that already have non-null
target values — they differ in two axes: what happens for matched rows where
one side (src or parquet) is null, and what happens for rows whose join key is only
present on one side.

| Mode           | Matched row (join key in both)                   | Source row unmatched by parquet | Typical use                                                               |
| -------------- | ------------------------------------------------ | ------------------------------- | ------------------------------------------------------------------------- |
| **replace**    | Parquet wins (null included)                     | Target columns set to NULL      | Fresh backfill of a brand-new field where parquet is the full source.     |
| **coalesce**   | `coalesce(existing, new)` per field — src wins   | Source preserved                | Incremental / repair runs that only want to fill gaps.                    |
| **overwrite**  | Parquet wins (null included)                     | Source preserved                | Corrective update for a subset of rows (parquet is authoritative only for the keys it covers). |

Parquet rows whose join key is not in the collection are always dropped (left-join
from source side).

**`coalesce` / `overwrite` caveats** (shared — both read source-side values):

- Parquet types must match the collection field types **exactly**
  (e.g. Int64 vs Int32 will be rejected). Spark's type widening would
  silently change the stored Arrow type otherwise.
- Reads the target field(s) from the base segments — slightly heavier I/O
  than `replace`.
- For `coalesce`: if the target field has no prior binlogs (you just added
  it), reads return all nulls and `coalesce` degrades to parquet-wins
  behaviour. Safe, but no win over `replace` in that case.
- For `overwrite`: an explicitly null value in the parquet **will** clobber
  the existing source value on matched rows — the match flag drives the
  projection, not the file column's null-ness.

## 7. Full flag reference

### Required

| Flag              | Description                                                             |
| ----------------- | ----------------------------------------------------------------------- |
| `--parquet`       | Path to user Parquet with join-key + new-field columns. `s3a://` or local. |
| `--snapshot`      | Path to `snapshot.json` produced by Milvus `CreateSnapshot`.            |
| `--s3-endpoint`   | S3 endpoint for Milvus storage (where segments live).                   |
| `--s3-bucket`     | Bucket for Milvus storage.                                              |

Omitting `--snapshot` switches the job to **client mode**, which requires
`--milvus-uri` and `--collection`. This path is discouraged for production.

### S3 auth (Milvus storage)

| Flag               | Description                                                          |
| ------------------ | -------------------------------------------------------------------- |
| `--s3-access-key`  | Access key. Leave empty to force IAM/IRSA.                           |
| `--s3-secret-key`  | Secret key.                                                          |
| `--use-iam`        | Force the IAM credentials chain (IRSA on EKS, instance profile, etc.)|
| `--s3-use-ssl`     | Enable TLS.                                                          |
| `--s3-root-path`   | Milvus `rootPath` (default: `files`).                                |
| `--s3-region`      | Default: `us-east-1`.                                                |

### Source bucket override (user Parquet in a different account / region)

All of the above, prefixed with `--source-`: `--source-s3-endpoint`,
`--source-s3-access-key`, `--source-s3-secret-key`, `--source-use-iam`,
`--source-s3-use-ssl`, `--source-s3-region`. If none are given, the
primary S3 config is reused for the input read.

### Writer

| Flag                 | Default        | Description                                                      |
| -------------------- | -------------- | ---------------------------------------------------------------- |
| `--batch-size`       | `1024`         | Rows per Arrow batch flushed to the writer.                      |
| `--column-mapping`   | *(none)*       | `src1:tgt1,src2:tgt2,...`. Rename/drop Parquet columns to Milvus field names. Must include the resolved join field as one target. |
| `--join-key`         | collection PK  | Exact persisted snapshot field to use for matching. Physical keys require snapshot mode.    |
| `--mode`             | `coalesce`     | `replace` \| `coalesce` \| `overwrite`. See §6 for semantics.    |
| `--output-result`    | *(none)*       | Path to write the result JSON. Strongly recommended.             |

### Client mode (no snapshot)

| Flag               | Description                                                         |
| ------------------ | ------------------------------------------------------------------- |
| `--milvus-uri`     | gRPC URI, e.g. `http://milvus:19530`.                               |
| `--milvus-token`   | Optional bearer token.                                              |
| `--database`       | Optional database name.                                             |
| `--collection`     | Collection name. Required in client mode.                           |
| `--partition`      | Optional.                                                           |

## 8. Performance & sizing

- **Partition count = segment count.** The connector pins one segment per
  Spark partition via `SegmentPartitioner`. Executor count ~= number of
  segments being processed concurrently.
- **Memory.** Arrow buffers + AWS SDK multipart upload queue can hold
  hundreds of MB per writer. Set `spark.executor.memoryOverhead` to at
  least equal the heap if you see `Direct buffer memory` or Arrow
  `RootAllocator` OOMs; see `docs/bugfix-backfill-direct-memory-oom.md`.
  Start at `memoryOverhead=8g`.
- **Batch size.** Default `1024` is a safe starting point. Lower it
  (`--batch-size 500`) if executors OOM; raise it (up to ~5000) for
  throughput if memory is ample.
- **S3 throughput.** Writes are the bottleneck. If Spark executors sit
  with idle CPU while uploads drain, increase executor parallelism or
  move the job closer to the storage region.
- **Runtime.** Typical numbers: ~1M rows per executor-minute for two
  single-field numeric columns on MinIO. Highly dependent on field width
  (long text is slower), batch size, and network.

## 9. Troubleshooting

| Symptom                                             | Likely cause & fix                                                                                              |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `InvalidSnapshot: missing manifest_list and storagev2_manifest_list` | Snapshot was produced before storage-V2/V3 landed on that collection, or the snapshot file is truncated. Re-run `CreateSnapshot`. |
| `Join-key type mismatch`                            | Parquet key type doesn't exactly match the selected snapshot field. Cast it in your ETL.                        |
| `duplicate join-key values` / `null join key`       | The parquet join key is invalid. Deduplicate it and remove null-key rows before retrying.                          |
| `Physical join-key field ... was not found`         | `--join-key` is case-sensitive and must name a persisted snapshot field.                                        |
| `Field name X not found in collection schema`       | Your column-mapping targets a field that isn't in the collection. Verify schema and mapping.                    |
| `--mode=coalesce/overwrite requires ... types to match snapshot` | Your Parquet column is e.g. `Int32` but the field is `Int64`. Cast in your ETL, or switch to `--mode replace` if you're OK with Spark widening.  |
| `OutOfMemoryError: Direct buffer memory`            | Increase `spark.executor.memoryOverhead` (start at 8g), lower `--batch-size`. See OOM bugfix doc.              |
| `NotSerializableException: java.util.Optional`      | You're on an old connector build — an Arrow exception is being masked. Update to a build that unwraps it.       |
| All backfilled rows have the same value             | Missing `.copy()` after a shuffle in a custom fork. The mainline connector handles this; upstream fix only.    |
| Job succeeds, queries still show NULL for new field | You forgot step 5.5 (`UpdateSegmentColumnGroups`) or the QueryNode hasn't reopened yet. Check `DataVersion`.    |
| `s3:// not registered`                              | Hadoop doesn't auto-register the `s3` scheme. Use `s3a://` throughout (the connector normalizes automatically, but some pre-flight tools do not).|
| Compaction nuked my segment files mid-run           | `compaction_protection_seconds` was too short or not set. Re-run the snapshot with a larger TTL.                |

## 10. Kubernetes notes

See `docs/k8s-backfill-troubleshooting.md` and
`examples/05-spark-application.yaml` for the full Spark Operator recipe.
Common gotchas:

- **Native libraries.** The connector's assembly JAR bundles native `.so`s,
  but Spark Operator templates may strip `LD_LIBRARY_PATH`. Make sure it
  includes `src/main/resources/native/linux-x86_64` (or the extracted
  location inside the container).
- **IRSA.** Pass `--use-iam` and drop `--s3-access-key` / `--s3-secret-key`
  to use the service-account role.
- **`mainClass`.** Always
  `com.zilliz.spark.connector.operations.backfill.BackfillApp`.

## 11. Related docs

- `design-snapshot-backfill.md` — full architecture / internals.
- `snapshot-backfill-workflow.md` — operational runbook (Chinese).
- `v2-backfill-implementation.md` — V2 (non-manifest) segment support.
- `backfill-mode-plan.md` — design of `--mode coalesce`.
- `bugfix-backfill-data-corruption.md` — the UnsafeRow / Arrow buffer trap.
- `bugfix-backfill-direct-memory-oom.md` — memory sizing.
- `k8s-backfill-troubleshooting.md` — Kubernetes / Spark Operator.
