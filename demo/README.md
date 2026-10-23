# Milvus Spark Connector demos

Each feature has independent Python and Java examples. The connector examples
use the committed Milvus Storage V3 sample by default, so they need no Milvus
service, object store or credentials. An optional REST example looks up an
existing snapshot's path on a service that exposes the snapshot API. All service
and storage operations are read-only; the examples never create or change
snapshots, segments, deletes or indexes.

Run the commands below from the repository root. [run.sh](run.sh) selects the
language and example, and configures Spark for the connector examples. Python
sources live in [python/](python/); Java sources live in
[java/src/main/java/com/zilliz/spark/connector/demo/](java/src/main/java/com/zilliz/spark/connector/demo/).

## Examples

| Python | Java | What it demonstrates |
|---|---|---|
| [00_snapshot_path.py](python/00_snapshot_path.py) | [SnapshotPath.java](java/src/main/java/com/zilliz/spark/connector/demo/SnapshotPath.java) | Describe an existing snapshot through REST and use its returned path in connector options. |
| [01_read_snapshot.py](python/01_read_snapshot.py) | [ReadSnapshot.java](java/src/main/java/com/zilliz/spark/connector/demo/ReadSnapshot.java) | Read schema and rows, and verify that deleted rows are absent. |
| [02_filter_project.py](python/02_filter_project.py) | [FilterProject.java](java/src/main/java/com/zilliz/spark/connector/demo/FilterProject.java) | Apply scalar predicates, select columns, and handle a nullable category. |
| [03_sql_analytics.py](python/03_sql_analytics.py) | [SqlAnalytics.java](java/src/main/java/com/zilliz/spark/connector/demo/SqlAnalytics.java) | Register a temporary view and use SQL to count rows and group by category. |
| [04_exact_search.py](python/04_exact_search.py) | [ExactSearch.java](java/src/main/java/com/zilliz/spark/connector/demo/ExactSearch.java) | Run exact top-k L2 search using SQL in Python and the public DataFrame API in Java. |
| [05_filtered_search.py](python/05_filtered_search.py) | [FilteredSearch.java](java/src/main/java/com/zilliz/spark/connector/demo/FilteredSearch.java) | Filter the base before top-k selection; the default predicate is `category = 1`. |
| [06_index_search.py](python/06_index_search.py) | [IndexSearch.java](java/src/main/java/com/zilliz/spark/connector/demo/IndexSearch.java) | Search persisted HNSW indexes in approximate mode, with `ef=max(32,k)` for the sample. |
| [07_vector_functions.py](python/07_vector_functions.py) | [VectorFunctions.java](java/src/main/java/com/zilliz/spark/connector/demo/VectorFunctions.java) | Evaluate L2 distance, cosine similarity, and inner product on literal vectors. |
| [08_partition_selection.py](python/08_partition_selection.py) | [PartitionSelection.java](java/src/main/java/com/zilliz/spark/connector/demo/PartitionSelection.java) | Select partition `20`, segment `30`, fields `100,102`, and inspect row metadata. |

## Prepare the runtime

For the connector examples, use Linux x86_64 or aarch64, Java 21, Python 3, and
Spark 4.0.1 built for Scala 2.13. PySpark comes from that Spark distribution. See the
[environment instructions](../README.md#environment) for the toolchain.
The launcher defaults to `MASTER=local[2]`; the committed sample requires a
local Spark master because its files live in the checkout.

The connector examples also need the connector and native Storage/Knowhere
libraries for the current platform. Choose one artifact source:

- **Local assembly:** build `spark/assembly` with a matching unified native
  bundle. The native JAR and its `.jar.properties` sidecar must be present; the
  bundle must match the checkout's pinned source revisions. Follow the
  [native build guide](../native/build/README.md) to build or select it.
- **Verified public candidate:** use an existing candidate containing the
  Spark 4.0 JVM JAR and the matching Linux native JAR. See the
  [release guide](../docs/releasing.md) for candidate layout and verification.

For a local assembly:

```bash
export JAVA_HOME=/absolute/path/to/jdk-21
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_HOME=/absolute/path/to/spark-4.0.1-bin-hadoop3-scala2.13
git submodule update --init milvus-proto milvus-storage knowhere
sbt -DsparkVersion=4.0 \
  -Dmilvus.native.bundle=/absolute/path/to/native.jar spark/assembly
```

The output is under `spark/target/scala-2.13/`. Set `CONNECTOR_JAR` to the
actual `spark-milvus-4.0-bundle-*.jar` produced by that command; replace the
placeholder below with its filename:

```bash
unset CANDIDATE
export CONNECTOR_JAR="/absolute/path/to/spark-milvus-4.0-bundle-<version>.jar"
```

For a candidate instead, keep the same Java and Spark settings and select its
directory:

```bash
unset CONNECTOR_JAR
export CANDIDATE=/absolute/path/to/verified-public-candidate
```

Set exactly one of `CONNECTOR_JAR` and `CANDIDATE`. The launcher configures
the read session extension, `MilvusSparkPlugin`, and Java's `libjsig` signal
chaining. For Java examples, it compiles the sources with the configured JDK
and existing Spark/connector JARs, then runs the selected class. No Maven build
or dependency download is needed; temporary Java build outputs are removed
when the launcher exits. Help works before configuring Spark or native libraries:

```bash
./demo/run.sh --help
./demo/run.sh python 04_exact_search.py --help
./demo/run.sh java ExactSearch --help
```

The REST path lookup has separate requirements: the Python version uses only
Python's standard library, and the Java version needs `JAVA_HOME` for JDK 21
and `SPARK_HOME` for its bundled Jackson JARs. Neither path lookup starts a
Spark session or needs a connector/native JAR. See
[Look up an existing snapshot path](#look-up-an-existing-snapshot-path).

## Read, filter and analyze the sample

```bash
./demo/run.sh python 01_read_snapshot.py
./demo/run.sh python 02_filter_project.py
./demo/run.sh python 03_sql_analytics.py
./demo/run.sh java ReadSnapshot
./demo/run.sh java FilterProject
./demo/run.sh java SqlAnalytics
```

The [sample](../core/src/test/resources/read-fixtures/collections/nearest/README.md)
contains two segments, each with a persisted HNSW index. Its columns are `id`
(long), `vector` (two floats) and `category` (nullable long). Of the 16 stored
rows, IDs `3002` and `3105` are deleted; 14 remain visible. The examples verify
the sample's file checksums; the read example also compares the visible rows
with the committed expectations.

There are five live rows in category `0`, eight in category `1`, and one with
a null category (`id=3001`). The SQL example's total is `14`; grouping by
category retains the null group. `category = 1` excludes that null row, while
`category IS NULL` selects it explicitly.

## Run exact vector search

```bash
./demo/run.sh python 04_exact_search.py
./demo/run.sh java ExactSearch
```

The default query vectors are `qid=1: [0.0, 0.0]` and
`qid=2: [7.0, 1.0]`, with `--k 3`. The expected hits, in increasing distance
order per query, are:

```text
   qid            id      distance
     1          3000      0.000000
     1          3001      1.000000
     1          3100      1.030776
     2          3107      0.250000
     2          3106      0.750000
     2          3007      1.000000
```

`distance` is Euclidean L2 distance. Each language's independent oracle uses
squared L2 for ranking, which preserves the same ordering. It checks every query's neighbors
and accepts equal-distance ties without requiring a particular ID order. The
search examples report `recallAtK` for each query and native search metrics;
exact search must match the oracle.

## Filter before ranking

```bash
./demo/run.sh python 05_filtered_search.py --explain
./demo/run.sh java FilteredSearch --explain
```

This example applies `category = 1` to the base before top-k selection. The
eight matching live rows are eligible; deleted rows, other categories and the
null category cannot be returned. The expected exact hits are:

```text
   qid            id      distance
     1          3101      1.600781
     1          3003      3.000000
     1          3103      3.400368
     2          3107      0.250000
     2          3007      1.000000
     2          3006      1.414214
```

Use `--where 'category = 0'` to try another predicate. `--explain` prints the
execution plan; look for `MilvusNearestByJoin` and its Milvus input. PySpark
calls the connector's `nearest_by_join` SQL table function on this Spark line.
Java constructs `MilvusDataFrame` and calls `nearestByJoin` with a ranking
`Column`, then selects query IDs, matched row IDs and their distances.
The [search reference](../docs/reference-en.md#vector-search) describes its
API and execution semantics.

## Search persisted indexes

```bash
./demo/run.sh python 06_index_search.py --explain
./demo/run.sh java IndexSearch --explain
```

This example selects approximate search and configures HNSW `ef=max(32,k)` for
the committed sample. It
requires positive indexed-segment, index-byte and native-call metrics, then
reports recall against an exact oracle. Approximate indexes are not
required to return the exact ranking. A successful approximate run confirms
index execution and valid results; reported recall describes their accuracy.
For a custom snapshot, omit `milvus.search.params` to use the connector's
index-specific defaults, or set it in the options JSON for that index family.
HNSW `ef` must be at least `k`; IVF uses `nprobe` instead.

## Vector functions and partition selection

```bash
./demo/run.sh python 07_vector_functions.py
./demo/run.sh python 08_partition_selection.py
./demo/run.sh java VectorFunctions
./demo/run.sh java PartitionSelection
```

The vector-functions example applies `vector_l2_distance`,
`vector_cosine_similarity` and `vector_inner_product` directly to literal
arrays, which is useful for understanding their scores before building a
search query.

The partition example combines `milvus.partitions=20`, `milvus.segments=30`,
and `fieldIDs=100,102` as read options. Partition `20` contains both sample
segments; selecting segment `30` returns seven live rows. Field `100` is `id`
and field `102` is `category`, so the vector column is omitted. The extra columns
`_segment_id`, `_row_offset` and `_timestamp` identify stored rows; row offsets
retain the gap left by deleted ID `3002`. See the
[read options](../docs/reference-en.md) for selecting partitions, segments
and fields in other snapshots.

## Look up an existing snapshot path

For a disposable open-source Milvus deployment, the
[live validation procedure](live-validation.md) prepares a small V3 collection
and snapshot, then runs path lookup, reads and searches in both languages.

If your Milvus endpoint exposes the snapshot REST API, these examples describe
an existing named snapshot with:

```http
POST /v2/vectordb/snapshots/describe
Content-Type: application/json

{"dbName":"default","collectionName":"documents","snapshotName":"spark-demo"}
```

They take `data.s3Location` from a successful response and use its exact value
as `milvus.snapshot.path`. For example, this response:

```json
{"code":0,"data":{"s3Location":"s3://your-bucket/files/snapshots/123/metadata/snapshot.json"}}
```

produces these connector options before merging your storage settings:

```json
{"milvus.snapshot.path":"s3://your-bucket/files/snapshots/123/metadata/snapshot.json"}
```

The value is the snapshot metadata location, not the metadata JSON contents,
collection name or Milvus service URL. The mapping comes from Milvus's
[REST snapshot handler](https://github.com/milvus-io/milvus/blob/master/internal/distributed/proxy/httpserver/snapshot_handler_v2.go)
and the pinned [DescribeSnapshot protobuf](../milvus-proto/proto/milvus.proto).
API availability depends on the deployment and version; this example does not
assume every Milvus release or cloud endpoint supports it. The returned path
must identify a Milvus Storage V3 snapshot readable by the connector. An empty
location fails the example; it does not construct a path from the snapshot name.

The helper only describes the existing snapshot. It does not create, export,
pin or extend the retention of data. Arrange the snapshot and its file retention
before running it. `MILVUS_TOKEN` authenticates the REST request; object-storage
credentials remain a separate requirement for the later Spark read.

Set `MILVUS_URI` to the service's base URL and `MILVUS_TOKEN` through your
environment. Alternatively pass the URL with `--uri`. Both languages accept:

| Option | Meaning |
|---|---|
| `--uri URL` | Service base URL; defaults to `MILVUS_URI`. |
| `--database NAME` | Database name; defaults to `default`. |
| `--collection NAME` | Required collection name. |
| `--snapshot NAME` | Required existing snapshot name. |
| `--timeout SECONDS` | Request timeout; defaults to `30`. |
| `--storage-options FILE` | Optional JSON of `fs.*` / `extfs.*` storage options and a snapshot-path placeholder to replace. |
| `--output FILE` | Optional new JSON file for connector options, created with mode `0600`. |

Without `--output`, the helper prints connector options as JSON. With `--output`,
it writes that JSON to a file, replacing any template `milvus.snapshot.path`
with the returned location. Choose a new filename in an existing private
directory; existing files are not overwritten. Complete the bucket, endpoint,
region and identity settings in
[config/s3-options.json](config/s3-options.json) for your storage first, or use
a private copy of that template. For example, run one path lookup:

```bash
export MILVUS_URI=https://your-milvus-endpoint.example
# Supply MILVUS_TOKEN through your environment if authentication is required.
./demo/run.sh python 00_snapshot_path.py \
  --database default --collection documents --snapshot spark-demo \
  --storage-options demo/config/s3-options.json --output /private/read-options.json
```

The equivalent Java command is:

```bash
./demo/run.sh java SnapshotPath \
  --database default --collection documents --snapshot spark-demo \
  --storage-options demo/config/s3-options.json --output /private/read-options.json
```

After configuring the connector runtime, use the generated file from either
language:

```bash
./demo/run.sh python 01_read_snapshot.py --options /private/read-options.json
./demo/run.sh java ReadSnapshot --options /private/read-options.json
```

## Use your own snapshot

Every connector snapshot example accepts `--options /private/read-options.json`
and then uses that existing V3 snapshot instead of the committed sample. The literal
vector-functions example needs no snapshot options.
Retain every metadata, segment, delete and index file referenced by your
snapshot for the full run.

Start from [config/local-options.json](config/local-options.json) for local
files or [config/s3-options.json](config/s3-options.json) for S3 using the native
default credential chain. Copy the template outside the checkout, replace the
placeholder paths and storage settings, and configure the process identity on
both driver and executors. The S3 template contains no secrets.
For other storage settings, see the
[storage reference](../docs/reference-en.md#22-storage-location-and-credentials).
Driver and executors must have access to the files with the same identity.

Use [config/queries.json](config/queries.json) as the query format: an array of
objects with unique integer `qid` values and float `vector` arrays. Match
the vector dimension to your snapshot. Search examples require `--queries`
when `--options` is supplied:

```bash
./demo/run.sh python 01_read_snapshot.py --options /private/read-options.json
./demo/run.sh java ReadSnapshot --options /private/read-options.json
./demo/run.sh python 04_exact_search.py \
  --options /private/read-options.json --queries /data/queries.json \
  --id-column id --vector-column vector --k 3
./demo/run.sh java ExactSearch \
  --options /private/read-options.json --queries /data/queries.json \
  --id-column id --vector-column vector --k 3
```

Search examples `04`, `05` and `06`, and their Java counterparts, accept
`--queries`, `--id-column`, `--vector-column`, `--k`, `--max-rows`, `--where`
and `--explain`. Their default
columns are `id` and `vector`, and their default `k` is `3`. `--queries` can
also override the sample's built-in queries without `--options`.

Examples `02` and `03`, and their Java counterparts, use the sample's column
names `id`, `vector` and `category`. Example `08` and `PartitionSelection` use
the sample's partition, segment and field IDs as defaults;
replace `milvus.partitions`, `milvus.segments` and `fieldIDs` in your options
JSON and keep the `id` column for its display. Use snapshots with matching
schema/IDs or edit those examples for your own data.

The search oracle accepts up to `--max-rows` (default and maximum 10,000)
base rows after filtering and 1–100 query vectors. It collects one extra row
to detect inputs exceeding that limit and rejects empty inputs. Use the
[connector APIs](../docs/reference-en.md) directly for larger workloads.

For a cluster, use custom `--options`, provide `--queries` for search examples,
and set `MASTER` for your deployment. Local paths must exist at the same
location on the driver and every executor. All nodes also need compatible
native libraries and the Java `libjsig` path configured by the launcher.
