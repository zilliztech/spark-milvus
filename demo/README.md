# Spark-Milvus examples

These examples cover reading Collection data and updating existing field values
with Spark. For writes, start with [Column replacement](#column-replacement).

The runnable reader examples below use existing Storage V3 snapshots with
Spark 4.0.1, Scala 2.13 and Java 21. In the committed sample the default read
returns 14 live rows. The basic read example also sets `milvus.read.mode=physical`
and reads all 16 stored rows, including the two marked deleted.

| Python | Java | Operation |
|---|---|---|
| `01_read_snapshot.py` | `ReadSnapshot` | Read and validate snapshot rows |
| `02_filter_project.py` | `FilterProject` | Filter and project fields |
| `03_sql_analytics.py` | `SqlAnalytics` | Aggregate through Spark SQL |
| `08_partition_selection.py` | `PartitionSelection` | Prune partitions with `WHERE _partition_id IN (...)` and read row metadata |
| `00_snapshot_path.py` | `SnapshotPath` | Look up the path of an already existing snapshot |

## Run a reader example

Build the selected [assembly](../docs/application-development.md#building-the-connector-from-source), or use a verified release
candidate. The assembly must contain the Storage native libraries for the
local Linux x86_64 or aarch64 platform.

```bash
export JAVA_HOME=/path/to/jdk-21
export SPARK_HOME=/path/to/spark-4.0.1
export CONNECTOR_JAR=/absolute/path/to/connector-assembly.jar
./demo/run.sh python 01_read_snapshot.py
./demo/run.sh java FilterProject --explain
./demo/run.sh python 08_partition_selection.py
```

Alternatively set `CANDIDATE` to a verified candidate directory and leave
`CONNECTOR_JAR` unset. The launcher verifies the candidate before selecting its
Spark JVM and platform JARs. It configures Java signal chaining on both driver
and executors. Reading needs no Spark extension or plugin.
On Linux, candidate verification requires `readelf` from `binutils`, even when
the selected example uses Spark 4.0: verification audits the complete candidate,
including its Spark 3.5 Arrow supplement.

The default sample is local to this checkout and requires a local Spark master.
For a cluster, use an existing snapshot with shared object-storage access:

```bash
export MASTER=spark://cluster.example:7077
./demo/run.sh python 02_filter_project.py --options /private/read-options.json --where 'category = 1'
./demo/run.sh java PartitionSelection --options /private/read-options.json --where '_partition_id IN (10, 11)'
```

The options file is a JSON object containing `milvus.snapshot.path` and the
necessary `fs.*` settings. To include rows marked deleted in a custom read, add
`"milvus.read.mode": "physical"` to that file. To inspect the original vector bytes
in `ReadSnapshot` or `01_read_snapshot.py`, add `"milvus.read.vector.raw": "true"`.
See [reference](../docs/reference.md) and
[cluster setup](../docs/cluster-setup.md). Do not commit credentials. Use
`--help` on each example for its arguments. Verification collects at most
10,000 rows and is intended for small samples.

## Resolve an existing snapshot path

The standalone `SnapshotPath` helpers call the snapshot describe endpoint to
resolve a snapshot that the caller has already created. They are demo helpers,
not connector runtime APIs, and never create a snapshot. Pass `--help` for
URI, database, collection, snapshot, storage-options and output arguments.
The reader examples take the returned path and storage configuration directly.

## Column replacement

For a Scala example that updates existing field values, see the
[column replacement example](../docs/column-replacement.md#example). The guide
also covers physical reads, external updates, write results and recovery.
The write produces new segment revisions and a receipt; registering them with
a running Milvus service is outside these examples.
