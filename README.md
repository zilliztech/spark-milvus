# Milvus Spark Connector

Read fixed Milvus Storage V3 snapshots directly from object storage with Apache
Spark, and replace stored columns of such a snapshot from a DataFrame: the
connector writes new column files, commits new manifest revisions and publishes
a receipt.

Callers supply and protect the snapshot and compute the new values. The
connector does not create snapshots, call Milvus, register new revisions, or
confirm online visibility. Index construction, append, backup and legacy
storage compatibility are outside this repository. Vector fields are supported as
ordinary data columns.

Snapshot metadata must declare `format_version` 4 or 6. Missing and unsupported
versions fail, including for empty snapshots. The 1.x line ended at `v1.6.0`.
Connector versions follow Milvus releases; the 3.0 line targets Milvus 3.0.
Use a published 3.0 version, or a verified candidate produced from this source.
Earlier release lines do not provide this API.

## Project structure

| Layer | Module | Responsibility |
|---|---|---|
| 1 | `native-runtime` | Validate component requirements and extract one platform bundle |
| 1 | `native-storage` | Pinned milvus-storage JNI and upstream JVM APIs |
| 2 | `core` | V3 snapshots, schema, filters, reads and shared V3 file-writing primitives |
| 3 | `spark` | Read and column-replacement DataSource V2, neutral segment scan contract and Arrow conversion |
| — | `integration` | Explicit object-storage integration tests, outside the root aggregate |

Dependencies point downwards. The public native bundle contains Storage.
An embedding application can provide one complete native bundle satisfying the
bindings' component requirements; it must not overlay bundles for one platform.

| Spark selector | Compile version | Scala | Java bytecode |
|---|---|---|---|
| `3.5` | 3.5.5 | 2.12.20, 2.13.16 | 11 |
| `4.0` (default) | 4.0.0 | 2.13.16 | 17 |
| `4.1` | 4.1.1 | 2.13.16 | 17 |
| `4.2` | 4.2.0 | 2.13.16 | 17 |

Build with Java 21. Spark 3.5 Arrow tests use Java 17 through
`SPARK35_TEST_JAVA_HOME`. Reader thin
candidates cover the source matrix; fat distribution consumers cover 4.0/4.2.

## Installing

The examples below describe the 3.0 release artifacts. Replace
`<published-3.0-version>` with a version listed in
[GitHub Releases](https://github.com/zilliztech/spark-milvus/releases) after
publication. A candidate built by CI is not automatically available on Maven
Central or Docker Hub.

For a Spark 4.0 / Scala 2.13 application, add the Maven dependency:

```xml
<dependency>
  <groupId>com.zilliz</groupId>
  <artifactId>spark-milvus-4.0_2.13</artifactId>
  <version>&lt;published-3.0-version&gt;</version>
</dependency>
```

Or use sbt:

```scala
libraryDependencies += "com.zilliz" % "spark-milvus-4.0_2.13" % "<published-3.0-version>"
```

This universal entry resolves the connector JVM implementation and all released
native platforms. For a Linux x86_64 deployment, use
`spark-milvus-4.0-linux-x86_64_2.13` to resolve only that platform. Spark itself
is supplied by the application or cluster. See the
[artifact selection guide](docs/releasing.md#using-release-artifacts) for Spark
4.2, thin modules and platform requirements.

An existing Spark application can resolve the same dependency with `--packages`:

```bash
export RELEASE_VERSION='<published-3.0-version>'
export LD_PRELOAD="$JAVA_HOME/lib/libjsig.so"
"$SPARK_HOME/bin/spark-submit" \
  --packages "com.zilliz:spark-milvus-4.0-linux-x86_64_2.13:$RELEASE_VERSION" \
  --conf "spark.executorEnv.LD_PRELOAD=/path/on/workers/to/jdk/lib/libjsig.so" \
  your_application.py
```

Use the running JRE's signal library on each driver and executor. The command
above targets a Linux client-mode driver; see [cluster setup](docs/cluster-setup.md)
for remote drivers and storage access.

For direct `--jars` use, download a matching assembly and its checksums from the
[release assets](https://github.com/zilliztech/spark-milvus/releases), or build
one below. The small Maven entry JAR is not an assembly: downloading it alone
does not bring its dependencies. The [JAR download guide](docs/releasing.md#jar-downloads)
explains the available files. For containers, use the
[Docker image guide](docker/README.md#run-a-release-image).

## Building

```bash
git submodule update --init --recursive milvus-proto milvus-storage
sbt compile test
make native-bundle
sbt -Dmilvus.native.bundle=/absolute/path/to/milvus-native-linux-x86_64.jar spark/assembly
```

The two submodules provide storage common/schema protobufs and the native engine.
The root only aggregates modules. Native compilation is explicit; ordinary sbt
compilation does not run Conan. See [contributing](docs/contributing.md) for the
native toolchain, signal chaining, formatting and required checks.

## Using the connector

Install the connector as above, or supply an assembly with `spark-submit --jars`.
No connector session extension or plugin is required for reads.

```scala
val snapshotOptions = Map(
  "milvus.snapshot.path" -> "snapshots/fixed.json",
  "fs.storage_type" -> "remote",
  "fs.cloud_provider" -> "aws",
  "fs.bucket_name" -> "example-bucket",
  "fs.address" -> "s3.us-west-2.amazonaws.com",
  "fs.region" -> "us-west-2",
  "fs.use_iam" -> "true"
)
val rows = spark.read.format("milvus").options(snapshotOptions).load()
rows.where("category = 1").select("id", "vector").show()
```

Storage credentials are supplied through these options, independently of any
other source the application reads. Keep snapshot metadata, manifests, data and
deletes available for every action. See the [reference](docs/reference.md) for
canonical options.

## Replacing columns

A physical read returns every stored row of a snapshot with its row identity.
The caller computes final values with ordinary Spark code and writes them back
with `milvus.write.operation=replace_columns` and `mode("append")`; the
connector restores stored order, writes new column files and commits a new
manifest revision per segment, then publishes a receipt at the `.save()` path.
Registering the new revisions with Milvus stays with the caller. Read
[the column replacement guide](docs/column-replacement.md).

## Documents

| Document | Contents |
|---|---|
| [Reference](docs/reference.md) | Read options and supported data types |
| [Column replacement](docs/column-replacement.md) | Physical reads, row identities, the column write and its receipt |
| [Examples](demo/README.md) | Python/Java snapshot reads and SQL |
| [Container](docker/README.md) | Build and run the reader image |
| [Cluster setup](docs/cluster-setup.md) | Runtime, identities and networking |
| [Contributing](docs/contributing.md) | Build, formatting and test requirements |
| [Releasing](docs/releasing.md) | Candidate closure, CI gates and release procedure |
| [3.0 release notes](docs/release-notes/3.0.md) | Capabilities and migration from earlier versions |
| [Native build](native/build/README.md) | Native platform build and manifest |

## License

See [LICENSE](LICENSE).

## Automated PR review

AI review covers all changed files, including documentation, using fixed Git
objects without executing PR code. See the [operation guide](.github/ai-review/README.md)
for repository policy, coverage reports, deployment and incomplete reviews.
