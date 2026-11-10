# Milvus Spark Connector demos

These examples read existing Storage V3 snapshots. They use Spark 4.0.1,
Scala 2.13 and Java 21. The committed sample includes 14 visible rows after
applying deletes; examples check results against that fixed input.

| Python | Java | Operation |
|---|---|---|
| `01_read_snapshot.py` | `ReadSnapshot` | Read and validate snapshot rows |
| `02_filter_project.py` | `FilterProject` | Filter and project fields |
| `03_sql_analytics.py` | `SqlAnalytics` | Aggregate through Spark SQL |
| `08_partition_selection.py` | `PartitionSelection` | Select a partition and expose row metadata |
| `00_snapshot_path.py` | `SnapshotPath` | Look up the path of an already existing snapshot |

## Run a reader example

Build the selected [assembly](../README.md#building), or use a verified release
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

The default sample is local to this checkout and requires a local Spark master.
For a cluster, use an existing snapshot with shared object-storage access:

```bash
export MASTER=spark://cluster.example:7077
./demo/run.sh python 02_filter_project.py --options /private/read-options.json --where 'category = 1'
```

The options file is a JSON object containing `milvus.snapshot.path` and the
necessary `fs.*` settings. See [reference](../docs/reference.md) and
[cluster setup](../docs/cluster-setup.md). Do not commit credentials. Use
`--help` on each example for its arguments. Verification collects at most
10,000 rows and is intended for small samples.

## Resolve an existing snapshot path

The standalone `SnapshotPath` helpers call the snapshot describe endpoint to
resolve a snapshot that the caller has already created. They are demo helpers,
not connector runtime APIs, and never create a snapshot. Pass `--help` for
URI, database, collection, snapshot, storage-options and output arguments.
The reader examples take the returned path and storage configuration directly.

## Replacing columns

Replacing stored columns from a DataFrame is described in
[column replacement](../docs/column-replacement.md). Its result is new segment
revisions and a receipt; registering them with a running Milvus service is
outside these examples.

The [image guide](../docker/README.md) and
[demo Helm chart](../deploy/helm/spark-milvus-demo/README.md) package the reader
examples for containers and Kubernetes.
