# Milvus Spark Connector

Read and search fixed Storage V3 snapshots with Apache Spark, directly from
object storage. The caller supplies the snapshot path and its storage identity
and keeps the metadata, data, deletes and index files available for every action.
The read/search path does not call Milvus or create snapshots.

Vector search uses NEAREST BY: Spark 4.2's native syntax, and the connector's
`nearestByJoin` / `nearest_by_join` on Spark 3.5 to 4.1. EXACT scans vectors;
APPROX uses compatible persisted indexes. Deletions and scalar predicates apply
before ranking. Use the read-only session extension and `MilvusSparkPlugin`;
see [the search reference](docs/reference-en.md#vector-search).

The distribution contains only V3 reads and search. Writing, backfill, index
construction, snapshot output, old storage formats, backup and Cardinal are
outside its scope. Public APIs contain no Milvus service client or SDK
dependency.

The 1.x line stopped at `v1.6.0` and is no longer maintained. The 2.0 rewrite
continues on `refactor/v2`. Candidate versions and artifact coordinates are
explicitly recorded by the [release workflow](docs/releasing.md).

## Project structure

The public build has five production modules in three layers. Dependencies
point downward; `core` and `native-storage` reject Spark imports at compile time.
Everything native lives under `native/`: the three layer-1 modules and the
unified native build in `native/build/`.

| Layer | Module | Directory | What it holds |
|---|---|---|---|
| 1 | `native-runtime` | `native/runtime` | Verification and extraction of the unified native bundle |
| 1 | `native-storage` | `native/storage` | The pinned milvus-storage JNI and Java/Scala API |
| 1 | `native-vector` | `native/vector` | The upstream Knowhere Java API and persisted-index search adapter |
| 2 | `core` | `core` | V3 snapshots, manifests, deletes, schema, codecs, read planning and search; no Spark |
| 3 | `spark` | `spark` | Read-only DataSource, DataFrame and NEAREST BY interfaces |
| — | `integration` | `integration` | V3 reads from a real object store, outside root's aggregate |

The root only aggregates modules. It has no mixed assembly or publication.
Select a Spark line and Scala version when starting sbt; the `spark` module
compiles common sources with the required `scala-shims` directories.

| Spark selector | Compile version | Supported Scala | Java bytecode |
|---|---|---|---|
| `3.5` | 3.5.5 | 2.12.20, 2.13.16 | 11 |
| `4.0` (default) | 4.0.0 | 2.13.16 | 17 |
| `4.1` | 4.1.1 | 2.13.16 | 17 |
| `4.2` | 4.2.0 | 2.13.16 | 17 |

The build runs on Java 21. Spark 3.5 native Arrow tests fork Java 17 via
`SPARK35_TEST_JAVA_HOME`. The published release matrix remains Spark 4.0/4.2;
source compilation of another line does not expand that release commitment.

Four git submodules sit in the repository. `milvus-proto` supplies the
protobuf definitions: `common.proto` and `schema.proto` are generated into
`core` because the storage format itself is defined in protobuf. No service
protocols or gRPC clients are generated. `milvus-storage`
supplies the native storage library from `milvus-io/milvus-storage`, pinned at
official commit `5694520`, including PR #714's Tencent OIDC role chain while
retaining eager Parquet pre-buffering and the IO pool API from PR #703.
`knowhere` tracks upstream `zilliztech/knowhere` on `main`, including merged
PR #1829, and supplies the C API, JNI and Java sources used by
`native-vector` and the unified native build.

Initialize the three native source submodules at their recorded gitlinks before
compiling:

```bash
git submodule update --init milvus-proto milvus-storage knowhere
```

## Documents

| Document | What it answers |
|---|---|
| [demo/README.md](demo/README.md) | Run Python and Java examples for snapshot path lookup, reads, SQL, vector search and partition selection |
| [docs/reference-en.md](docs/reference-en.md) | V3 read options, vector search, data types and supported entry points |
| [docs/contributing.md](docs/contributing.md) | Building, formatting, testing and the native bundle, beyond this README |
| [docs/releasing.md](docs/releasing.md) | Maven coordinates, the supported release matrix, candidate validation and Central publication |
| [native/build/README.md](native/build/README.md) | How the per-platform native bundle is built and published |

[AGENTS.md](AGENTS.md) is the entry point for agents and people working on the
code: what the project is, how it is layered, and the rules any change has to
satisfy. `CLAUDE.md` is a symlink to it.

## Environment

The quickest way to a complete build, package and test environment is the
development container: it is the `dev` stage of the root `Dockerfile`, built
locally on top of its toolchain stage, with the dependency caches in named
volumes and an optional
local object-storage services for integration tests.

```bash
scripts/devcontainer.sh up        # build the dev image once, start the container
scripts/devcontainer.sh init      # submodules, Conan profile and remote
scripts/devcontainer.sh shell     # make, sbt and the native build run in here
```

VS Code and other IDEs that read `.devcontainer/devcontainer.json` offer the
same container as "Reopen in Container". The rest of this section is for a
toolchain installed directly on the host.

Minimum 2 CPU cores and 8 GB of RAM. Version mismatches between the tools below
cause build failures that are hard to read, so pin them.

| Tool | Version |
|---|---|
| Java | 21 |
| Scala | 2.13.16 |
| Spark | 4.0.1, built for Scala 2.13 |
| sbt | 1.11.1 |

[SDKMAN](https://sdkman.io/) is the easiest way to manage them:

```bash
sdk install java 21.0.5-zulu
sdk install scala 2.13.16
sdk install sbt 1.11.1
```

SDKMAN's Spark is built for Scala 2.12, so install the 2.13 build by hand from
the [Spark download page](https://www.apache.org/dyn/closer.lua/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3-scala2.13.tgz).

Java 26 cannot run the test suites that start a SparkSession: `Subject.getSubject`
was removed and Spark still calls it. Use Java 21.

### spark-submit wrapper

Point `SPARK_HOME` at the installation you just unpacked:

```bash
#!/bin/bash
export SPARK_HOME=/xxx/spark-4.0.1-bin-hadoop3-scala2.13

if [ ! -d "$SPARK_HOME" ]; then
  echo "Error: SPARK_HOME directory does not exist: $SPARK_HOME"
  exit 1
fi

SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
if [ ! -f "$SPARK_SUBMIT" ]; then
  echo "Error: spark-submit not found at: $SPARK_SUBMIT"
  exit 1
fi

exec "$SPARK_SUBMIT" "$@"
```

```bash
chmod +x /xxx/spark-submit-wrapper.sh
alias spark-submit-wrapper="/xxx/spark-submit-wrapper.sh"
```

## Building

The native libraries, the toolchain versions and the two ways to build the
native bundle are described in [native/build/README.md](native/build/README.md).

Knowhere library loading uses the Java API and JNI from pinned upstream `main`,
which includes merged PR #1829.
The API builds automatically; the native platform JAR is selected explicitly.
See [Knowhere library loading](docs/contributing.md#knowhere-library-loading)
for the real JNI smoke command and the `libjsig` preload requirement.

```bash
sbt compile test                           # default Spark 4.0, Scala 2.13.16
sbt -DsparkVersion=4.2 -DscalaVersion=2.13.16 \
  'clean; integration/clean; compile; test'
sbt -DsparkVersion=3.5 -DscalaVersion=2.12.20 \
  'clean; integration/clean; compile; test'
# Select a matching native bundle before assembly or native integration tests.
sbt -DsparkVersion=4.0 -Dmilvus.native.bundle=/absolute/path/native.jar spark/assembly
sbt -DsparkVersion=4.0 -Dmilvus.native.bundle=/absolute/path/native.jar integration/test
```

The Maven release workflow exports JVM and native artifacts independently and
tests both universal and platform-specific entries. See the
[release guide](docs/releasing.md) for coordinates, native prerequisites and the
build, consumer and publication commands.

The fat jar only loads Milvus segments when it carries the native
`milvus-storage` libraries for the platform it runs on. `make package` builds
them into the unified Storage/Knowhere bundle from the platform's profile under
`native/build/profiles/`, on Linux and macOS, x86_64 and aarch64 alike. The macOS
toolchain and its Conan configuration are in
[contributing.md](docs/contributing.md#macos). Released bundles cover Linux
x86_64 and aarch64 and macOS on Apple silicon (`darwin-aarch64`, macOS 14 or
later), so a Mac developer resolves the connector from Maven Central without
building anything native; see [releasing.md](docs/releasing.md).

`sbt compile` builds the five production modules for the selected combination.
Use project IDs such as `core/test`, `spark/compile` and `integration/Test/compile`
for individual work. Restart sbt and clean when changing the Spark or Scala
selection. `spark/assembly` produces `spark/target/scala-<binary>/spark-milvus-<line>-bundle-*.jar`.

### Docker

The Docker build handles the native dependencies on an architecture-native
worker. Linux x86_64 and Linux aarch64 workers both build the unified
Storage/Knowhere bundle; either accepts a matching prebuilt unified JAR and its
`.properties` sidecar through `NATIVE_BUNDLE` instead.

```bash
docker build -t spark-milvus .  # current architecture, Spark 4.0 local read bundle
```

| Build argument | Default | Meaning |
|---|---|---|
| `GIT_BRANCH` | `unknown` | Goes into the version string |
| `NATIVE_BUNDLE` | empty | Prebuilt unified Linux JAR inside the build context, with its checksum sidecar |
| `NATIVE_JOBS` | `50` | Native build concurrency, from 1 to 50 |
| `NATIVE_BUILD_OPTIONS` | empty | Unified OSS source build options, including `--conan-lock` |

Docker does not publish Maven artifacts; use the verified [release workflow](docs/releasing.md).
The [integration chart](deploy/helm/spark-milvus-integration/README.md) only runs
V3 reads with an existing Secret; it does not deploy or set up a Milvus service.

The version is derived as `2.0.0-{branch}-{arch}-SNAPSHOT`, for example
`2.0.0-refactor-v2-amd64-SNAPSHOT`.

Pull the jar back out of the image:

```bash
docker create --name temp spark-milvus
docker cp temp:/opt/spark-milvus/. ./
docker rm temp
```

## Using the connector

Use the public candidate's JVM implementation and matching OSS native JAR, or
its universal/platform-specific Maven entry, for the selected Spark line. See
[release coordinates](docs/releasing.md). For a local build, use the selected
`spark/assembly` bundle.

### Run the demos

The [demo directory](demo/README.md) contains Python and Java examples for
snapshot reads, filters and projections, SQL analytics, exact and indexed vector
search, vector functions, and partition selection. These use a committed 14-row
snapshot by default and need no Milvus service or object-store credentials. A
separate REST example looks up an existing snapshot's path on deployments that
expose the snapshot API and can write connector read options for it.

Use Linux x86_64 or aarch64, Java 21, Spark 4.0.1 for Scala 2.13, and a local
`spark/assembly` JAR containing the native libraries for your platform. From
the checkout root:

```bash
export SPARK_HOME=/absolute/path/to/spark-4.0.1-bin-hadoop3-scala2.13
export JAVA_HOME=/absolute/path/to/jdk-21
export CONNECTOR_JAR="/absolute/path/to/spark-milvus-4.0-bundle-<version>.jar"
./demo/run.sh python 01_read_snapshot.py
./demo/run.sh java ReadSnapshot
./demo/run.sh python 04_exact_search.py
./demo/run.sh java ExactSearch
./demo/run.sh python 05_filtered_search.py --explain
./demo/run.sh java IndexSearch
```

Replace the paths and version with your actual installation and assembly.
The [walkthrough](demo/README.md) covers all nine example pairs, assembly setup,
expected results, a verified release candidate, REST path lookup, and your own
snapshot. Java sources compile on launch using the existing JDK and JARs;
no Maven build or dependency download is required.
`./demo/run.sh --help` works before Spark is configured.

### Query with Scala DataFrames

With a Spark session configured as above, this example reads an existing
collection snapshot and finds three matches per query. Replace the snapshot, storage settings,
field names and query vectors with your own; here `embedding` is a
three-dimensional float vector and `id` and `title` are collection fields.

```scala
import com.zilliz.spark.connector.implicits._
import org.apache.spark.sql.functions.call_function
import spark.implicits._

val docs = spark.read.format("milvus")
  .option("milvus.snapshot.path", "files/snapshots/10/metadata/1.json")
  .option("fs.storage_type", "local")
  .option("fs.root_path", "/data/milvus")
  .load()
val queries = Seq(
  (1L, Seq(0.1f, 0.2f, 0.3f)),
  (2L, Seq(0.4f, 0.5f, 0.6f))
).toDF("qid", "vec")

val hits = queries.nearestByJoin(
  docs,
  call_function("vector_l2_distance", queries("vec"), docs("embedding")),
  3, "exact", "distance"
).select(queries("qid"), docs("id"), docs("title"))

hits.explain(true) // Look for MilvusNearestByJoin with input=Milvus table.
hits.show(false)
```

The import supplies `nearestByJoin` on Spark 3.5–4.1; Spark 4.2 has the method
built in. Reading uses a fixed snapshot. `approx` can use compatible persisted
indexes; inspect `explain` for indexed segments and exact-scan fallbacks. See
the [search reference](docs/reference-en.md#vector-search) for these semantics
and the [read examples](docs/reference-en.md#31-reading-data) for direct
`spark.read.format("milvus")` options.

## License

See [LICENSE](LICENSE).

## Automated PR review

AI review covers all changed files, including documentation, using fixed Git
objects without executing PR code. See the [operation guide](.github/ai-review/README.md)
for repository policy, coverage reports, deployment and incomplete reviews.
