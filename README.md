# Spark-Milvus

**Read and write Milvus Collection data in Apache Spark.** Spark-Milvus lets
you use DataFrames to analyze Collection data and write back updated field
values, bringing Milvus data into your Spark workflows.

- **Read Collection data**: Load data into a DataFrame, then filter, join,
  analyze and process it with Spark SQL, Scala or PySpark.
- **Write Collection data**: Write Spark results to Milvus data files to replace
  existing field values, for data cleaning, field transformations and batch
  updates.

The connector currently accesses Collection data through a **Snapshot**, which
captures the Collection's data at a fixed point in time. Provide its JSON file
path and storage access settings; Spark-Milvus discovers the schema and reads
and writes data files directly in object storage.

To get started, [add the dependency](#installing), follow the
[read](#reading-collection-data) and [write](#writing-collection-data) examples,
then [run your application](docs/application-development.md#running-your-application).
For all supported options, defaults and limits, see the
[Reference](docs/reference.md#2-milvus-format-parameters).

This guide uses **Spark-Milvus 3.0.0** for Milvus 3.0, available from
[Maven Central](https://repo.maven.apache.org/maven2/com/zilliz/spark-milvus-4.0_2.13/3.0.0/).
Earlier releases do not provide this API. See the
[3.0.0 release notes](https://github.com/zilliztech/spark-milvus/releases/tag/v3.0.0)
for capabilities and migration guidance.

## Why Spark-Milvus

Milvus serves online vector search, while Spark provides distributed computing
for data preparation and analysis. Spark-Milvus connects these workflows around
the same Collection data, so applications can combine online retrieval with
offline processing.

For example, a product Collection can power similar-product search in Milvus.
In Spark, you can join its data with a product catalog, check data quality and
[update eligible fields in batches](docs/column-replacement.md). Your application
controls when the resulting updates are submitted to Milvus and become visible
online.

## Requirements and compatibility

Before writing your application, prepare:

- An existing Milvus 3.0 Snapshot path and access to its object storage. The
  connector reads that fixed data state; your application must keep the
  Snapshot and all referenced files available until the job finishes.
- A Spark distribution and a connector artifact with matching Spark and Scala
  versions. The examples below use Spark 4.0, Scala 2.13 and Java 21.
- Storage access on both the Spark driver and executors, plus a matching native
  platform library on their classpaths.

### Spark and Scala

| Spark | Scala binary version | Available dependency choices |
|---|---|---|
| 3.5 | 2.12, 2.13 | Thin modules plus a native platform |
| 4.0 | 2.13 | Universal, platform-specific or thin |
| 4.1 | 2.13 | Thin modules plus a native platform |
| 4.2 | 2.13 | Universal, platform-specific or thin |

### Operating systems and CPU architectures

Published native libraries support the following platforms:

| Operating system | CPU architecture | Artifact platform ID | Minimum system requirements |
|---|---|---|---|
| Linux | x86_64 (AMD64) | `linux-x86_64` | glibc 2.35+ and GCC 12 C++ runtime or newer |
| Linux | ARM64 (aarch64) | `linux-aarch64` | glibc 2.35+ and GCC 12 C++ runtime or newer |
| macOS | Apple silicon (ARM64) | `darwin-aarch64` | macOS 14 or later |

Linux binaries are tested on Ubuntu 22.04 and 24.04. Native artifacts are not
provided for Windows, musl-based Linux (including Alpine), or Intel Macs.
Both the Spark driver and every executor must meet these requirements and have
a matching native platform library on their classpaths.

See [artifact selection](docs/releasing.md#using-release-artifacts) for the full
runtime requirements and thin-module coordinates.

## Installing

### Maven

For a Spark 4.0 / Scala 2.13 application, add the connector dependency and mark
Spark as provided by your runtime. Set `spark.version` to the version installed
on your cluster.

```xml
<properties>
  <spark.version>4.0.1</spark.version>
  <spark-milvus.version>3.0.0</spark-milvus.version>
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

### Gradle

For a Spark 4.0 / Scala 2.13 application, use the following `build.gradle`
(Groovy DSL):

```groovy
plugins {
    id 'scala'
}

repositories {
    mavenCentral()
}

dependencies {
    implementation 'org.scala-lang:scala-library:2.13.16'
    implementation 'com.zilliz:spark-milvus-4.0_2.13:3.0.0'
    compileOnly 'org.apache.spark:spark-sql_2.13:4.0.1'
}
```

Set the Spark version to the one installed on your cluster. Gradle's
[`compileOnly`](https://docs.gradle.org/current/userguide/dependency_configurations.html)
keeps Spark available for compilation; your Spark runtime supplies it when the
application runs. For a Java-only application, use the `java` plugin and omit
the explicit `scala-library` dependency.

### sbt

```scala
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "com.zilliz" % "spark-milvus-4.0_2.13" % "3.0.0",
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

## Reading Collection data

The connector is a Spark DataSource named `milvus`. Provide a Snapshot of the
Collection to select a fixed data state to read. The connector discovers the
schema from that Snapshot and requires no session extension or plugin.

Choose either the Scala or PySpark example below. Both print the Collection's
schema and five rows from the selected Snapshot. Replace the S3 path, endpoint
and region with your own values; the bucket is part of the `s3://` path.

These examples use `fs.use_iam=true`: the native storage library obtains AWS
credentials from its credential chain, such as process environment credentials
or a workload IAM role. Make that identity available to both the driver and
executors. For access-key authentication and other providers, see
[storage authentication](docs/reference.md#22-storage-location-and-credentials).

### Scala

Save this as your application's `ReadMilvus.scala`, then package it using your
Maven, Gradle or sbt project:

```scala
import org.apache.spark.sql.SparkSession

object ReadMilvus {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Read Milvus Collection").getOrCreate()
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

spark = SparkSession.builder.appName("Read Milvus Collection").getOrCreate()
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
rows.createOrReplaceTempView("milvus_collection")
spark.sql("SELECT id, embedding FROM milvus_collection WHERE category = 1").show()
```

Vector fields are available as column values; the default read converts most
vector types into Spark arrays or maps. See
[supported types](docs/reference.md#supported-data-types).

## Writing Collection data

Use a DataFrame to update existing field values in a Collection. The write uses
a Snapshot to identify the stored rows and columns to update. The connector takes
the final column values, writes new Milvus column files, commits manifest
revisions and publishes a write receipt. The `replace_columns` operation
replaces field values while preserving row count and row identities.

Read in physical mode when computing values to write back. This includes every
stored row, even rows marked deleted. Set `milvus.read.include_row_id=true` to
add `_milvus_row_id` and preserve each row's identity. The write must receive
every stored row exactly once.

For example, trim an existing, unindexed string field named `description`.
Place this inside the Scala example's `try` block, using its `spark` and
`snapshotOptions`:

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

The required `.mode("append")` writes new result files and revisions; it does
not append rows to the collection.

Use a new result path for each write and grant the required storage write
permissions. Preserve `_milvus_row_id` unchanged and keep every row, including
rows whose values stay unchanged. Key fields, indexed fields and some
Milvus-managed fields cannot be replaced; see the
[column replacement guide](docs/column-replacement.md) for eligible fields,
joining external updates, permissions and failure recovery.

The receipt identifies the committed revisions. The connector does not create
a Snapshot, and the original Snapshot keeps reading its original values. Your
application owns submission to Milvus and online visibility of the replacements.

## Documents

| Document | Contents |
|---|---|
| [Application development](docs/application-development.md) | Running applications, resolving JARs and building the connector from source |
| [Reference](docs/reference.md) | Supported options, defaults, storage credentials and data types |
| [Examples](demo/README.md) | Collection reads, SQL and column replacement |
| [Cluster setup](docs/cluster-setup.md) | Driver and executor permissions, networking and native runtime |
| [3.0 release notes](docs/release-notes/3.0.md) | Capabilities and migration from earlier versions |
| [Release artifacts](docs/releasing.md#using-release-artifacts) | Maven coordinates and platform choices |

## License

See [LICENSE](LICENSE).
