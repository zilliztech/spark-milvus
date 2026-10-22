# Milvus Spark Connector Parameter Reference

The public `milvus` data source reads fixed **Storage V3** snapshot files or
explicit V3 schema/manifest metadata and supports vector search over those
inputs. It opens storage files directly and does not call the Milvus service.
Snapshot preparation, selection and retention belong to the caller.

The public table advertises only `BATCH_READ`. A DataFrame write is rejected by
Spark before it writes segment or snapshot files. Database and collection names
alone are not a public read input.

## Version compatibility

Compatibility is determined by the files and selected Spark/Scala build, not by
the Milvus service version alone. Every data segment declared by a public snapshot
must have Storage V3 metadata and a corresponding V3 manifest. V2 and unknown
storage versions fail during planning.

| Spark line | Scala build versions |
| --- | --- |
| 3.5 | 2.12.20 or 2.13.16 |
| 4.0, 4.1, 4.2 | 2.13.16 |

The single `spark` module selects a line through `-DsparkVersion` and
`-DscalaVersion`. See the [release guide](releasing.md) for published lines,
platforms and runtime requirements. The 1.x line ended at v1.6.0 and is no longer
maintained.

<a id="31-reading-data"></a>

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
  .option("milvus.search.params", "ef=256")
  .load()

docs.select("id", "title").show(false)
docs.createOrReplaceTempView("docs")
spark.sql("SELECT id, title FROM docs WHERE category = 1").show(false)
```

Replace the snapshot path, storage values and field names with your input. The
`milvus.search.params` option above is for HNSW searches; omit it for ordinary
reads or choose parameters supported by your indexes. Schema and segments come
from the snapshot; no Milvus URI, token or Java SDK is needed. Ordinary `select`
and `where` operations work on the result. To use vector search, configure the
[read/search extension](#offline-read-and-search-extension) before creating the
session.

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

Use a snapshot path for normal reads. Explicit schema/manifest options are for
callers that already possess complete V3 metadata.

| Option | Default | Meaning |
| --- | --- | --- |
| `milvus.snapshot.path` | unset | Fixed snapshot JSON location: a standard object-store URI, bucket-relative key, or local path for a local backend. Schema, partitions, deletes, indexes and segments come from this file. |
| `milvus.snapshot.mode` | inferred | Inferred as `true` from a nonblank path or manifest list. Set `true` explicitly for a schema-only empty snapshot. `false` does not enable a different input mode. |
| `milvus.snapshot.manifests` | unset | JSON array of V3 manifest entries; mutually exclusive with `milvus.snapshot.path`. Requires schema bytes or schema JSON below. |
| `milvus.snapshot.schema.bytes` | unset | Base64-encoded protobuf `CollectionSchema` bytes for explicit metadata. Takes precedence over schema JSON when both are supplied. |
| `milvus.snapshot.schema.json` | unset | Raw snapshot JSON containing the collection schema, used when schema bytes are absent. This is not Spark `StructType` JSON. |
| `milvus.snapshot.collection.id` | 0 when absent | Collection ID for explicit metadata; a supplied value must be a positive Long. |
| `milvus.snapshot.partition.ids` | unset | Comma-separated nonnegative partition IDs for explicit metadata. Use `milvus.partitions` below to select among a resolved snapshot's partitions. |
| `milvus.snapshot.max.json.bytes` | 67108864 | Positive maximum size of a snapshot JSON file, in bytes. |

A standard object-store URI uses `s3a://`, `s3://`, `gs://` or `oss://` with
the bucket as its authority. A bucket-relative key uses `fs.bucket_name`.
Milvus-produced `https://<endpoint>/<bucket>/<key>` snapshot locations are
accepted when the host and bucket match the configured input. A local backend
accepts local paths or `file:` URIs and rejects bucket-bearing locations.
Unsupported schemes and cross-bucket references fail during planning.

A manifest entry has a numeric `segmentID` (or `segmentIDLong`) and a `manifest`
string containing JSON with `ver` and `base_path`. This is the V3 manifest
representation named `storagev2_manifest_list` in Milvus snapshot JSON; that
wire name does not mean Storage V2 support. Explicit metadata has only the
information supplied by the caller; use the full snapshot input when you need
its delete/index metadata. Supplying Spark `.schema()` does not replace the
Milvus schema: field names, types and selection must match the fixed input.

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
| `MilvusOption.MilvusFilter` (`milvus.filter`) | String | No | unset | Milvus scalar expression for an ordinary table read. It is parsed and validated against the fixed snapshot during planning, evaluated in both row and columnar readers, and combined with Spark predicates and deletes before Limit. On the base of a NEAREST BY the connector executes, it excludes rows before the top k. |
| `MilvusOption.ReadApplyDeletes` (`milvus.read.apply.deletes`) | Boolean | No | true | Apply all segment-local, partition-level L0, and collection-level L0 deletes visible in the fixed snapshot. Setting this to `false` is explicit opt-out; any provided value besides `true` or `false`, including a blank value, is rejected. |
| `milvus.read.vector.raw` | Boolean | No | false | Output type for vector columns. With the default `false`, vectors are converted to native Spark types (`FloatVector`/`Float16Vector`/`BFloat16Vector` to `ArrayType(FloatType)`, `Int8Vector` to `ArrayType(ShortType)`, `SparseFloatVector` to `MapType(LongType, FloatType)`). Set to `true` and vector columns come out as `BinaryType`, the bytes exactly as stored, for the caller to decode using `dim` and the element type. That path does no per-element conversion, which suits batch jobs that hand the bytes straight to a native library |
| `milvus.read.columnar` | Boolean | No | true | How the scan delivers rows. With the default `true` Spark gets whole Arrow batches (`ColumnarBatch`) that wrap the native buffers without copying, with vector columns typed as `milvus.read.vector.raw` decides; a batch with deleted rows is delivered through a position map over the surviving rows, still without copying. `false` delivers one row at a time. Row and columnar readers use the same expected-row guard. |
| `milvus.read.batch.max.rows` | Int | No | 8192 | Positive maximum rows requested from milvus-storage for one record batch; delivered as `reader.record_batch_max_rows`. |
| `milvus.read.batch.max.bytes` | Long | No | 33554432 | Positive target byte limit for one native record batch; delivered as `reader.record_batch_max_size`. The current upstream maximum is 4294967296 (4 GiB). In an exact search it is also the base block one engine call takes: 32 MiB when unset; the batched distance entry tiles the block internally, so the block only sets the fixed cost per call and the task's memory. The choice is logged as `exact scan batch: …`. |
| `milvus.read.arrow.max.bytes` | Long | No | 9223372036854775807 | Positive hard limit of the Arrow child allocator owned by one Spark read task. It covers imported/read Arrow buffers on row, columnar and vector paths, but not milvus-storage's separate native memory pool. |

Provided selector lists reject blanks, empty entries and non-numeric values.
Boolean read options (`milvus.snapshot.mode`, `milvus.read.apply.deletes`,
`milvus.read.vector.raw`, `milvus.read.columnar`) accept only `true` or `false`,
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
result schema. On the base of a NEAREST BY the connector executes, a condition
pushed this way applies before the top k. The DataSource V1 Filter API is not
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

On the base of a NEAREST BY the connector executes, `milvus.filter` excludes
rows before the top k as well. JSON paths, Array predicates, and `json_contains`
are not in the current scalar subset.

## Vector search

A vector search is written as a NEAREST BY join: Spark 4.2's own, and on
Spark 3.5, 4.0 and 4.1 the connector's `nearestByJoin` and `nearest_by_join`,
which preserve the same join arguments and result shape (the two sections
below); numerical ranking differences are described below. When the base is a Milvus table, the connector executes the join: it
finds each query's top k in every segment of the snapshot, through the
persisted index the snapshot pinned (APPROX) or by computing every distance
(EXACT), merges them into the global top k and reads the base columns of the
rows it selected.

APPROX loads the HNSW family (`HNSW`, `HNSW_SQ`, `HNSW_PQ`, `HNSW_PRQ`),
the IVF family (`IVF_FLAT`, `IVF_SQ8`, `IVF_PQ`) and `FLAT`, over the element type the column carries. A
nullable column is indexed over the rows that have a value, which its index
files record in a `valid_data` bitmap; without that bitmap the search fails.
Indexes containing vector quantization can return
approximate scores, and the hits are ranked by them; the connector does not
read the original vectors to recompute them. An index whose segment,
partition or row count differs from the segment the snapshot pinned fails
while the join is planned, naming every such segment; corrupt files and
incompatible formats fail when a task loads the index. Each task owns and
closes the indexes it loaded; they are not cached across tasks. Search
parameters follow the index family, in the base's `milvus.search.params`: the
HNSW family takes `ef`, an integer at least K, defaulting to `max(64, K)`; the
IVF family takes `nprobe`, a positive integer defaulting to 16; a flat index
takes none. Public native bundles use open-source Knowhere. Encrypted indexes
and formats requiring Cardinal are outside the public read contract.

Set `spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin`
before starting Spark, together with the session extension shown below. The
plugin loads the native libraries on each executor at startup and is required
for the connector to execute NEAREST BY joins. A cluster-provided
`spark.plugins.defaultList` containing the same plugin also satisfies this
requirement. Without it, the join uses the Spark fallback rather than the
connector's search execution, which can compare every query with every base
row. Check `hits.explain(true)` for `input=Milvus table` or `input=DataFrame`
and inspect the search counters in the Spark SQL UI; a successful query alone
does not establish that persisted indexes were used.

Executors that read persisted indexes from object storage should run with
glibc's malloc tuned, through the executor environment:
`spark.executorEnv.MALLOC_ARENA_MAX=2`,
`spark.executorEnv.MALLOC_MMAP_THRESHOLD_=1048576` and
`spark.executorEnv.MALLOC_TRIM_THRESHOLD_=134217728`. An index is read as
many 8 MiB range requests on several native threads, and glibc's default
dynamic mmap threshold keeps the freed blocks in per-thread arenas, so an
executor that searches chunk after chunk keeps growing until the container
limit is hit. A fixed threshold switches that off. Indexes read from local
disk do not need it.

The options below size the job. They are read options of the base, given as
`milvus.search.params` is: `.option(...)` on the fixed DataFrame read. To use
SQL, register that DataFrame as a temporary view. An ordinary DataFrame input
that is not a direct Milvus scan takes the defaults.
`milvus.search.queries.max.bytes` (default 1 GiB) decides where the query rows
go. When the query side's rows fit it, they are fetched to the driver in one
job, numbered and packed there and broadcast, and every executor keeps the
packed vectors whole; each hit finds its query row by number. A larger query
side stays on the executors: its vectors are broadcast when they alone fit,
and otherwise the executors pack them by query group into a shuffle output,
which a task reads one group at a time, holding at most two; the hits then
meet their query rows in a join after the last stage. Every way gives the same
result. Read by group, a job reads the query vectors once per segment set in
total, and the executors' local disks need room for about two copies of them.
`milvus.search.group.max.bytes` limits one task's query-group heap estimate.
Serial search counts each query's vector row plus K × 48 bytes for primitive
and packed candidates. An index search holding every core of a multicore
executor reserves overlapping search and output: each query counts
`2 × (vector row bytes + 8 + 128) + K × 72` bytes for two groups' vectors, ids,
queue metadata, the current primitive/packed candidates and the next merger.
This estimate applies to the explicit option too. A single query, or an explicit
limit too small for one overlapping query pair, uses the serial estimate.
Such a task searches at most
one group ahead when it retains its segment set; query-resident and streaming
tasks execute serially and may leave the overlap reserve unused. Spark accounts
for its shuffle pages separately.
Left unset, the planner takes the larger of 512 MiB and 8 queries per task
core, capped at half the task's query heap budget and 2 GiB, with a floor of one
query. The `Search budget` log line reports the resulting query count. At
768 float32 dimensions and K=100,000, a 24 GiB-heap, 32-core executor still
targets 256 queries per call; an explicit 512 MiB holds 74 with overlap
reserved, compared with 111 under the serial estimate.

Milvus candidate records use a fixed 12-byte encoding: a UInt32 segment
ordinal, a UInt32 physical row offset and a Float32 score. One dictionary for
all segments in the search sorts their original signed Long ids and is
broadcast once; each task retains only its own subset of those global
ordinals. Physical row counts include deleted rows. A known physical extent
above 2^32 fails planning. Unknown extents may execute, and every encoded
offset must be in [0, 2^32); overflow fails without truncation or a wider
fallback. Every search uses Float32 candidate scores before local top-k
selection and throughout merging. Distance calculations may still accumulate
in Double; L2 keeps the squared-distance ranking scale and rounds the square
to Float32. Values that round together use segment id and row offset to break
ties, including large L2 squares that overflow to infinity and small squares
that underflow to zero. This can change near-tie selection compared with Double
scores. No encoding option is needed. Group and merge planning retain the
conservative 24-byte candidate estimate and the 48/72-byte reserves above, so
smaller records do not increase group sizes or reduce merge partitions.
DataFrame inputs use their nonnegative Spark partition id as the ordinal and
the same checked UInt32 row offsets and Float32 scores. They carry rows after
the 12-byte candidate and a 4-byte row length: a 16-byte header plus row bytes.
Their planning still reserves the previous 28-byte header plus row bytes.

`milvus.search.collect.threads` (default 0, the cores the task holds) is how many
threads a search task checks, collects and packs a group's candidates on between
two Knowhere calls; at large K that work is a third of the task, and on one
thread it left the other cores idle. `milvus.search.calls.in.flight` (default
2, at most 8) is how many segments of a held set a task searches at once within
one query group: the second call's queries fill the tail of the first and the
collection of the first's answers, which took a 32-core executor from 89% to
95-97% busy at K=10,000 and from 77% to 92-97% at K=100,000; the calls share
Knowhere's threads, so each one takes longer and the gain is the idle time they
fill, not twice the throughput. 1 is the sequential search. `milvus.search.query.ranges` (default 0,
the planner's choice) cuts the first stage into that many query ranges per
segment set, so that the tasks fill the slots in whole waves; each range task
loads its segment set again, which pays off when the search itself takes far
longer than the load.
`milvus.search.segments.max.bytes` (default automatic) is how many bytes of
segment data **one executor** keeps off the heap for a search: base vectors for
EXACT, indexes for APPROX. Unset, it follows the executor's memory:
`(memory limit − heap − 1 GiB) × 0.5` for EXACT and
`memory limit − heap − 2 GiB` for APPROX. The limit is the cgroup's or the
machine's in local mode, and on a cluster it is the container Spark asks for
(`spark.executor.memory` + memoryOverhead + offHeap). It is never below 64 MiB a
task. The search tasks running on one executor share it. On a cluster, APPROX
runs one search task per executor at a time; EXACT divides the
budget by `spark.executor.cores` ÷ `spark.task.cpus`; a local master divides the
n of `local[n]` by `spark.task.cpus`. Queries have a budget of their own on the
heap: the part Spark does not manage, `(heap − 300 MiB) × (1 −
spark.memory.fraction)`, divided the same way. When a task's queries fit that
budget, the task keeps its queries and reads each segment once. Otherwise it
keeps its segment set off the heap and the query groups pass through it one
by one; when the set does not fit either, the plan cuts more sets. A segment
set that turns out larger than the budget no longer fails the task: every query
group reads the set again (I/O × groups). The driver's `Search budget` log
line states both budgets, the order chosen, the number of sets and whether any
set streams. Segment data lives in Arrow's or Knowhere's off-heap memory, which
`-Xmx` does not bound.

On a cluster, APPROX needs no `spark.task.cpus` setting. The connector
declares that each task of the index search stage takes every core of its executor (an exact search runs its tasks on
`spark.task.cpus` cores each), and that each task of the stage packing the
query groups takes as many cores as keeps the groups packed at once inside the
heap. The other stages run one task per core, as by default. This needs
dynamic allocation off; in local mode or with dynamic allocation on, set
`spark.task.cpus` to the executor's cores as before.

`milvus.search.group.max.bytes` stays **per task**: it also decides how many
query groups there are, which is the shape of the plan and not only memory. One
task that keeps its segment set peaks at its segment budget plus one query
group, plus the next group being read when the query set is read by group. A
task that keeps its queries peaks at the matrices and top-k state of all its
queries plus one segment and its load peak. A broadcast query set is also kept
whole on every executor.

The node that executes the join counts into SQL metrics of these names, which
the SQL tab shows on the node and the stage page lists once a task ends; while
the join runs, the driver log reports them once a heartbeat. They are
`milvus.search.segment.searches`, `milvus.search.read.bytes` and
`milvus.search.read.nanos`, `milvus.search.index.bytes` and
`milvus.search.index.load.nanos`, `milvus.search.bitmap.nanos`,
`milvus.search.knowhere.calls` and `milvus.search.knowhere.nanos`,
`milvus.search.compared.pairs` (EXACT: the query × visible-row pairs scored
so far; 0 for APPROX), `milvus.search.exact.rescans` (EXACT L2 over float32:
the batch × query pairs searched again pair by pair because the batched
entry's error could hide a nearer row; near zero for most data, high for
vectors far from the origin and near each other), `milvus.search.candidates`,
and `milvus.search.take.rows` and `milvus.search.take.nanos`. `bruteForce` and index
searches run in Knowhere's own thread pool, so their time is in
`milvus.search.knowhere.nanos` rather than in the task's CPU time; the batched
float32 exact entry runs on the task thread, so its time is in both.

### Offline read and search extension

For fixed V3 snapshot or schema/manifest reads and searches, set
`spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions`
and `spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin`.
The read extension registers vector functions, NEAREST BY and connector search
strategies. Plain DataFrame reads do not require a search extension. Spark
Connect uses the same fixed input options and supports the SQL search path
described below. Configure the extension and plugin on the Connect server.

### NEAREST BY on Spark 4.2

On Spark 4.2 a search is Spark's own NEAREST BY join. With the
session extension installed
(`spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions`)
and `com.zilliz.spark.connector.extensions.MilvusSparkPlugin` in
`spark.plugins`, a join whose base is a Milvus table is executed by the
connector. Here `docs` is the temporary view created in the
[fixed snapshot example](#1-reading-with-dataframes), with search parameters set
on that read; `queries` is a temporary view of the query DataFrame:

```sql
SELECT q.qid, d.id, d.title
FROM queries q
JOIN (SELECT * FROM docs WHERE category = 1) d
  APPROX NEAREST 10 BY SIMILARITY vector_cosine_similarity(q.vec, d.embedding)
```

In a DataFrame it is `queries.nearestByJoin(docs,
call_function("vector_cosine_similarity", queries("vec"), docs("embedding")),
10, "approx", "similarity")`. The result is Spark's NEAREST BY result: each row
is one query row joined with one base row, at most k rows a query, and no score
column; select the ranking expression again for the score.

- What is taken over: the ranking is `vector_l2_distance` (BY DISTANCE),
  `vector_cosine_similarity` or `vector_inner_product` (BY SIMILARITY), one
  argument computed from the base's columns and the other from the query's
  columns or constants; `spark.sql.crossJoin.enabled` is true and neither side
  is a stream. A base that is a Milvus table -- below the join only aliases,
  views, column projections and filters the Milvus scan takes whole, and the
  vector a FloatVector, Float16Vector or BFloat16Vector field -- is searched as
  described below. Any other base is read as a DataFrame and scanned exactly:
  EXACT on any base, and APPROX when the base contains a Milvus table (a filter
  the scan cannot take, a computed column, a limit, a join), computed exactly
  then. APPROX over a base without a Milvus table is left to Spark, or to that
  base's own data source. A ranking or direction the connector does not
  compute is executed by Spark (a cross join, then the top k of each query),
  with a line in the driver log saying why. `explain` names the input:
  `input=Milvus table`, or `input=DataFrame` with the reason.
- A DataFrame input has no field dimension. The connector searches the query
  vectors of the commonest length among those it can search, and leaves the
  others, as well as query vectors with a NULL element, to Spark itself. A
  base row whose vector is NULL or has a NULL element ranks nothing; one of
  another length fails with `VECTOR_DIMENSION_MISMATCH`, as Spark's function
  does. Its search options are the defaults, and with more query groups than
  one the base is computed once for each group.
- APPROX searches the index of each segment that has a usable one and scans
  the others exactly: no index on the field, no index metadata in the
  snapshot, an index built for another metric and an index type the connector
  does not load all leave a segment without a usable index. EXACT scans every
  segment. `explain` says how many segments APPROX takes through an index and
  why the others are scanned; the node shows the search's counters, its output
  rows, how the query rows divided and how many segments each way took in the
  SQL tab of the Spark UI.
- `milvus.search.params` is an option of the base read: comma-separated
  `name=value` pairs, `ef` for the HNSW family and `nprobe` for the IVF family;
  a parameter the index does not take fails the search. A DataFrame gives it
  with `.option(...)` on the read.
- A filter inside the base applies before the top k; a filter above the join
  applies after it.
- A query vector that is NULL, or has the field's dimension and a NULL
  element, has a NULL ranking value and finds nothing: INNER drops the query
  row, LEFT OUTER keeps it with NULL base columns. A query vector of another
  length (the empty one included) fails with `VECTOR_DIMENSION_MISMATCH`, as in
  Spark, unless the base has no row.
- Query rows whose vector holds NaN or an infinite value, or values outside the
  range where Knowhere and Spark compute the same (for L2 and the inner
  product, a magnitude of 2^33 or more; for COSINE, a largest magnitude below
  2^-25 or at least 2^17, the zero vector included), are executed by Spark
  itself and their rows join the result. When there are such rows the base is
  read once more, for Spark's cross join of those rows with it.
- Base vectors holding NaN, an infinite value or values outside that range are
  scored by Spark's own function during an exact scan, so they rank as in
  Spark: NaN above every number, last BY DISTANCE and first BY SIMILARITY. An
  index search ranks by the scores the index returns.
- Queries on float16 and bfloat16 fields are converted to the field's element
  type before Knowhere sees them, as Milvus does; their scores differ from
  Spark's by more than float32 rounding, and rows of close scores may come in
  another order than Spark's.
- An Int8Vector field is searched as a DataFrame input when the ranking casts
  it, `vector_l2_distance(q.vec, cast(d.embedding AS ARRAY<FLOAT>))`.

### NEAREST BY on Spark 3.5, 4.0 and 4.1

These lines have no NEAREST BY. The same session extension gives them
`nearestByJoin` with Spark 4.2's signature, a table function
`nearest_by_join` for SQL, and the three vector functions. The result and the
rules are those of the section above: a join over a Milvus table is executed by
the connector under the same conditions. Other joins use the same join semantics
through operators available on these lines; floating-point ranking can differ as
described above.

```scala
import com.zilliz.spark.connector.implicits._   // also harmless on 4.2
import org.apache.spark.sql.functions.call_function

val hits = queries.nearestByJoin(docs,
  call_function("vector_cosine_similarity", queries("vec"), docs("embedding")),
  10, "approx", "similarity")                     // a sixth argument "left_outer" keeps every query
```

Java writes `new MilvusDataFrame(queries).nearestByJoin(docs, ranking, 10,
"approx", "similarity")`; `MilvusDataFrame` is in
`com.zilliz.spark.connector.implicits`.

```sql
SELECT query.qid, base.id, base.title
FROM nearest_by_join(
  TABLE(queries),
  TABLE(SELECT * FROM docs WHERE category = 1),
  'vector_cosine_similarity(query.vec, base.embedding)',
  10, 'approx', 'similarity')                     -- a seventh argument 'left_outer' keeps every query
```

- The ranking is a string, parsed by the session's parser. In it, and in the
  columns the function returns, the query side is named `query` and the base
  `base`. Arguments can also be given by name: `query`, `base`, `ranking`,
  `num_results`, `mode`, `direction`, `join_type`.
- `mode` is `approx` or `exact` and `direction` is `distance` or `similarity`,
  in any case; the join type is `inner` (the default) or `left_outer`, in any
  case and with or without the underscore; k is between 1 and 100,000. An
  argument out of these, cross joins disabled, a streaming side and a ranking
  that cannot be ordered fail with an `IllegalArgumentException` whose message
  is Spark 4.2's, after the name of Spark 4.2's error condition, such as
  `[NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE]`.
- `vector_l2_distance`, `vector_cosine_similarity` and `vector_inner_product`
  take two `ARRAY<FLOAT>` and return a `FLOAT`, with Spark 4.2's rules: a NULL
  argument or a NULL element gives NULL, arrays of different lengths fail with
  `[VECTOR_DIMENSION_MISMATCH]`, two empty arrays give 0 for L2 and the inner
  product and NULL for the cosine, and so does a zero vector for the cosine.
  The sums are taken in double and rounded to float once, as Spark 4.3 does
  them, so values that overflow or underflow Spark 4.2.0's float sums do not.
- These lines have no other implementation to leave APPROX to, so the
  connector also takes APPROX over a base without a Milvus table, computed
  exactly as a DataFrame input.
- A join the connector does not execute, such as a ranking that is not one of
  the three functions, gives each query row a `uuid()`, joins it with every
  base row, and keeps the first k of each query by `row_number()` over a
  window. Like Spark 4.2's own execution, it forms every pair of a query row
  and a base row; the driver log says why the connector did not execute the
  join.
- On a Spark Connect session of 4.0 or 4.1, `nearestByJoin` fails and points
  to the table function, which works there. PySpark has no `nearestByJoin` on
  these lines yet.

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

Published entry artifacts resolve a JVM artifact and a matching native platform
bundle; see the [release guide](releasing.md#maven-coordinates-and-runtime-requirements).
Public bundles contain open-source Knowhere and milvus-storage. For a local fat
JAR, select the Spark line and use `spark/assembly` with
`-Dmilvus.native.bundle=/absolute/path/to/platform.jar`. All Spark lines use the
same packaging rules and leave Spark-supplied Arrow and Jackson modules out.
Root assembly is not an entry point.

The runtime verifies and extracts the unified native bundle once per class
loader, checking engine source pins and platform compatibility before JNI
initialization. An incompatible explicit `knowhere.native.path` is rejected.
See [building and validation](contributing.md#unified-native-bundle).

One JVM loads one copy of the connector. In local mode (`--master local[*]`),
leave `spark.executor.userClassPathFirst` unset: the executor runs in the
driver's JVM, and a second connector class loader conflicts when native
libraries are used. Cluster executors run in their own JVMs.

Preload the **running JRE's** `libjsig.so` in both the driver and every executor,
using `LD_PRELOAD` and `spark.executorEnv.LD_PRELOAD` with the paths appropriate
to each machine. See the [runtime launch example](releasing.md#thin-modules-for-downstream-libraries).
