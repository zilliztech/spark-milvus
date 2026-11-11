# Spark-Milvus

Use Milvus data in your Apache Spark applications. Spark-Milvus reads an
existing Milvus **Snapshot** directly from object storage into a DataFrame, so
you can join it with other datasets, analyze scalar and vector fields, and run
batch transformations with Spark SQL, Scala or PySpark.

A Snapshot describes the collection's data at a fixed point in time. You provide
its JSON file path and storage credentials; the connector discovers the schema
and reads the referenced files without querying a running Milvus service.

To get started, [add the dependency](#installing),
[read your Snapshot](#using-the-connector) and
[submit the application](#running-your-application). For more control, see
[supported options](#supported-options). You can also
[replace existing columns](#replacing-columns) with values computed in Spark.

This guide covers the 3.0 connector line for Milvus 3.0. Earlier releases do not
provide this API. Installation examples use `<published-3.0-version>`: replace
it with a 3.0 version listed in
[GitHub Releases](https://github.com/zilliztech/spark-milvus/releases) after
publication. CI candidates are not automatically available on Maven Central.

## Requirements and compatibility

Before writing your application, prepare:

- An existing Milvus 3.0 Snapshot path and access to its object storage. The
  connector reads that fixed data state; your application must keep the
  Snapshot and all referenced files available until the job finishes.
- A Spark distribution and a connector artifact with matching Spark and Scala
  versions. The examples below use Spark 4.0, Scala 2.13 and Java 21.
- Storage access on both the Spark driver and executors, plus a matching native
  platform library on their classpaths.

| Spark | Scala binary version | Available dependency choices |
|---|---|---|
| 3.5 | 2.12, 2.13 | Thin modules plus a native platform |
| 4.0 | 2.13 | Universal, platform-specific or thin |
| 4.1 | 2.13 | Thin modules plus a native platform |
| 4.2 | 2.13 | Universal, platform-specific or thin |

Native platforms are glibc Linux x86_64 and ARM64, and macOS on Apple silicon.
Linux requires glibc 2.35 or later; macOS requires version 14 or later. See
[artifact selection](docs/releasing.md#using-release-artifacts) for the full
runtime requirements and thin-module coordinates.

## Installing

### Maven

For a Spark 4.0 / Scala 2.13 application, add the connector dependency and mark
Spark as provided by your runtime. Set `spark.version` to the version installed
on your cluster.

```xml
<properties>
  <spark.version>4.0.1</spark.version>
  <spark-milvus.version>&lt;published-3.0-version&gt;</spark-milvus.version>
</properties>

<dependencies>
  <dependency>
    <groupId>com.zilliz</groupId>
    <artifactId>spark-milvus-4.0_2.13</artifactId>
    <version>${spark-milvus.version}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.13</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>
  </dependency>
</dependencies>
```

### sbt

```scala
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "com.zilliz" % "spark-milvus-4.0_2.13" % "<published-3.0-version>",
  "org.apache.spark" %% "spark-sql" % "4.0.1" % Provided
)
```

The universal dependency resolves the connector JVM implementation and every
released native platform; the loader selects the current platform. For a
Linux x86_64 deployment, use `spark-milvus-4.0-linux-x86_64_2.13` to resolve
only that platform. Use `linux-aarch64` for Linux ARM64 or `darwin-aarch64` for
Apple silicon. Keep the same connector version across all artifacts.

PySpark applications use these JVM artifacts too. Add them with `--packages`
when submitting the Python script; there is no separate connector Python
package to install.

## Using the connector

The connector is a Spark DataSource named `milvus`. It discovers the schema
from the Snapshot and requires no session extension or plugin.

Choose either the Scala or PySpark example below. Both print the schema and
five rows from your Snapshot. Replace the S3 path, endpoint and region with
your own values; the bucket is part of the `s3://` path.

These examples use `fs.use_iam=true`: the native storage library obtains AWS
credentials from its credential chain, such as process environment credentials
or a workload IAM role. Make that identity available to both the driver and
executors. For access-key authentication and other providers, see
[storage options](#snapshot-and-storage).

### Scala

Save this as your application's `ReadMilvus.scala`, then package it using your
Maven or sbt project:

```scala
import org.apache.spark.sql.SparkSession

object ReadMilvus {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Read Milvus Snapshot").getOrCreate()
    try {
      val snapshotOptions = Map(
        "milvus.snapshot.path" -> "s3://example-bucket/snapshots/fixed.json",
        "fs.cloud_provider" -> "aws",
        "fs.address" -> "s3.us-west-2.amazonaws.com",
        "fs.region" -> "us-west-2",
        "fs.use_ssl" -> "true",
        "fs.use_iam" -> "true"
      )

      val rows = spark.read.format("milvus").options(snapshotOptions).load()
      rows.printSchema()
      rows.show(5)
    } finally {
      spark.stop()
    }
  }
}
```

### PySpark

Save this as `read_milvus.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read Milvus Snapshot").getOrCreate()
try:
    snapshot_options = {
        "milvus.snapshot.path": "s3://example-bucket/snapshots/fixed.json",
        "fs.cloud_provider": "aws",
        "fs.address": "s3.us-west-2.amazonaws.com",
        "fs.region": "us-west-2",
        "fs.use_ssl": "true",
        "fs.use_iam": "true",
    }

    rows = spark.read.format("milvus").options(**snapshot_options).load()
    rows.printSchema()
    rows.show(5)
finally:
    spark.stop()
```

Once the read works, use the returned DataFrame with ordinary Spark operations:
select fields, filter rows, join another DataFrame or write to another format.
For example, if your collection has `id`, `category` and `embedding` fields,
add this after `rows.show(5)` inside the `try` block (the syntax works in both):

```python
rows.createOrReplaceTempView("milvus_snapshot")
spark.sql("SELECT id, embedding FROM milvus_snapshot WHERE category = 1").show()
```

Vector fields are available as column values; the default read converts most
vector types into Spark arrays or maps. See
[supported types](docs/reference.md#supported-data-types).

## Running your application

### Resolve dependencies with `--packages`

For a first run on a Linux x86_64 machine with Spark 4.0 installed, run the
PySpark example locally:

```bash
export RELEASE_VERSION='<published-3.0-version>'
export LD_PRELOAD="$JAVA_HOME/lib/libjsig.so"

"$SPARK_HOME/bin/spark-submit" \
  --master 'local[2]' \
  --packages "com.zilliz:spark-milvus-4.0-linux-x86_64_2.13:$RELEASE_VERSION" \
  read_milvus.py
```

For the Scala example, use the same dependency and runtime settings, replacing
`read_milvus.py` with `--class ReadMilvus /path/to/your-application.jar`.

For a cluster run, replace `local[2]` with your cluster's master and set
`--conf spark.executorEnv.LD_PRELOAD=/path/on/workers/to/jdk/lib/libjsig.so`.
Each JVM must preload its own JRE's signal library before startup. The export
above configures a Linux client-mode driver; a remote driver needs the setting
in its own environment too. On macOS, use the `darwin-aarch64` artifact and
that JRE's `lib/libjsig.dylib` through
`DYLD_INSERT_LIBRARIES` on the JVM. The native loader also needs a writable
extraction directory that permits shared-library loading. See
[cluster setup](docs/cluster-setup.md#prepare-every-driver-and-executor) for
worker identities, networking, memory and native runtime configuration.

### Use local JARs with `--jars`

If your cluster cannot resolve Maven dependencies, obtain these two artifacts
from [Maven Central](https://repo.maven.apache.org/maven2/com/zilliz/) at the
same published version and put them under `lib/`:

- `spark-milvus-4.0-jvm_2.13`: the bundled JVM implementation.
- `spark-milvus-native-linux-x86_64`: the matching native platform libraries.

Keep the signal-library settings above and replace `--packages` with:

```bash
--jars "lib/spark-milvus-4.0-jvm_2.13-${RELEASE_VERSION}.jar,lib/spark-milvus-native-linux-x86_64-${RELEASE_VERSION}.jar"
```

Spark distributes the JARs to the driver and executors. The universal and
platform-specific entry JARs rely on their Maven dependencies; passing only an
entry JAR to `--jars` does not install the connector. Thin deployments also need
their declared JVM dependencies; see
[artifact selection](docs/releasing.md#using-release-artifacts).

## Supported options

Pass connector settings with `.option("key", "value")` or `.options(...)` on
the reader or writer, before `.load()` or `.save()`. Use the lowercase storage
keys shown below. These are DataSource options, not Spark session settings.

Start with the Snapshot and storage settings from the example. The other read
options are optional; their defaults suit an ordinary read. The
[Reference](docs/reference.md#2-milvus-format-parameters) lists every connector
option, including empty Snapshot inputs and advanced storage configuration.

### Snapshot and storage

| Option | What to set |
|---|---|
| `milvus.snapshot.path` | Path to the existing Snapshot JSON, for example `s3://example-bucket/snapshots/fixed.json`. Required for a nonempty Snapshot. |
| `fs.bucket_name` | Bucket name if the path is a bucket-relative key. Omit it when the Snapshot URI already supplies the bucket. |
| `fs.address` | Object-storage endpoint, for example `s3.us-west-2.amazonaws.com`. Required for remote storage after configuration is resolved. |
| `fs.cloud_provider` | Storage backend; use `aws` for S3. Other providers are listed in the Reference. |
| `fs.region` | Your bucket's region, for example `us-west-2`. |
| `fs.use_ssl` | `true` for HTTPS; `false` for an HTTP endpoint. Set it explicitly to match your endpoint. |
| `fs.use_iam` | `true` to obtain credentials through the native storage credential chain. Make credentials available to driver and executors. |
| `fs.access_key_id`, `fs.access_key_value` | For static-key authentication, supply both and set `fs.use_iam=false`. Load secrets from your runtime rather than hardcoding them. |

For local files, AssumeRole, path-style endpoints, Hadoop credential translation
and the remaining `fs.*` settings, see the
[storage reference](docs/reference.md#22-storage-location-and-credentials).
When using static keys, also apply its Spark log-redaction setting.

### Reading data

| Option | Default | When to change it |
|---|---|---|
| `milvus.partitions`, `milvus.segments` | All | Select comma-separated numeric partition or segment IDs from the Snapshot. |
| `fieldIDs` | All collection fields | Select numeric Milvus field IDs. For application code, Spark `.select(...)` is usually simpler. |
| `milvus.filter` | Unset | Apply a supported Milvus scalar expression, such as `category == 1`. You can also use Spark `.where(...)`. |
| `milvus.extra.columns` | None | Add any of `_segment_id`, `_row_offset`, `_timestamp`, separated by commas. |
| `milvus.read.mode` | Unset | Leave unset to read rows visible in the Snapshot. Use `physical` for column replacement, or `write_result` to read a write receipt. |
| `milvus.read.include_row_id` | `false` | Set `true` with `physical` to add `_milvus_row_id`, which identifies each stored row for writing back. |
| `milvus.read.apply.deletes` | `true`; `false` in physical mode | Set `false` to include rows marked deleted. Physical mode cannot use `true`. |
| `milvus.read.vector.raw` | `false` | Set `true` to return stored vector bytes as binary columns instead of decoding them into Spark values. |
| `milvus.read.columnar` | `true` | Set `false` to deliver rows individually instead of Arrow batches. |
| `milvus.read.batch.max.rows` | `8192` | Limit the rows requested per native batch. |
| `milvus.read.batch.max.bytes` | `33554432` (32 MiB) | Set the target byte limit per native batch. |
| `milvus.read.arrow.max.bytes` | `Long.MaxValue` | Cap the Arrow allocator for each read task; this does not cap all native memory. |

See [read option details](docs/reference.md#23-data-operation-parameters) for
accepted values, limits and filter syntax. For an ordinary Snapshot read, use
`.option("milvus.snapshot.path", ...).load()`; `.load(path)` is reserved for
reading a write receipt.

### Writing columns

Set `milvus.write.operation=replace_columns`, provide the original
`milvus.snapshot.path` and storage options, then use
`.mode("append").save(resultPath)`. The DataFrame columns identify what to
replace. The example below shows the full flow; the
[write options guide](docs/column-replacement.md#write-options) explains the
accepted settings and restrictions.

## Replacing columns

Read in physical mode when computing values to write back. This includes every
stored row, even rows marked deleted, and adds `_milvus_row_id` to preserve each
row's identity. The write must receive every stored row exactly once.

For example, inside a Spark application with `spark` and `snapshotOptions` from
the Scala example, normalize an existing, unindexed string field named
`description`:

```scala
import org.apache.spark.sql.functions.{col, trim}

val source = spark.read
  .format("milvus")
  .options(snapshotOptions)
  .option("milvus.read.mode", "physical")
  .option("milvus.read.include_row_id", "true")
  .load()

val prepared = source.select(
  col("_milvus_row_id"),
  trim(col("description")).as("description")
)

val resultPath = "s3://example-bucket/column-writes/normalize-description-001"
prepared.write
  .format("milvus")
  .options(snapshotOptions)
  .option("milvus.write.operation", "replace_columns")
  .mode("append")
  .save(resultPath)

val receipt = spark.read
  .format("milvus")
  .options(snapshotOptions)
  .option("milvus.read.mode", "write_result")
  .load(resultPath)
receipt.show(false)
```

Use a new result path for each write and grant the required storage write
permissions. Preserve `_milvus_row_id` unchanged and keep every row, including
rows whose values stay unchanged. Key fields, indexed fields and some
Milvus-managed fields cannot be replaced; see the
[column replacement guide](docs/column-replacement.md) for eligible fields,
joining external updates, permissions and failure recovery.

The receipt identifies the committed revisions. The original Snapshot keeps
reading its original values; your application owns submission to Milvus and
online visibility of the replacements.

## Documents

| Document | Contents |
|---|---|
| [Reference](docs/reference.md) | Supported options, defaults, storage credentials and data types |
| [Column replacement](docs/column-replacement.md) | Physical reads, external updates, write results and recovery |
| [Examples](demo/README.md) | More Python and Java snapshot reads and SQL |
| [Cluster setup](docs/cluster-setup.md) | Driver and executor permissions, networking and native runtime |
| [3.0 release notes](docs/release-notes/3.0.md) | Capabilities and migration from earlier versions |
| [Release artifacts](docs/releasing.md#using-release-artifacts) | Maven coordinates and platform choices |

## Building

To develop the connector itself or build an assembly from source, follow
[Contributing](docs/contributing.md). It covers submodules, the Java 21 build,
native libraries, formatting and tests. Maintainers can find the candidate
validation and publication procedure in [Releasing](docs/releasing.md).

## License

See [LICENSE](LICENSE).
