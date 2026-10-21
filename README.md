# Milvus Spark Connector

Read and write Milvus collections from Apache Spark by going straight at the
Milvus storage format on object storage, rather than through the Milvus query
path. A read lists a snapshot, plans one Spark partition per segment, and pulls
Arrow column batches out of the segment files. A write stages segment files in
the same format on object storage; ordinary DataFrame append does not register
new segments with Milvus and does not make them queryable there. See the
[write contract](docs/reference-en.md#24-write-parameters) before using it.

Storage V2 requires Milvus 2.6 or later. Service operations also require the
corresponding Milvus APIs; the end-to-end demos below use Milvus 3.0+ for
snapshot creation. For Milvus 2.5 and earlier use the `legacy` branch, which is
no longer maintained.

The 1.x line stopped at tag `v1.6.0` and is no longer maintained. The 2.0 line
is a rewrite on branch `refactor/v2`, versioned
`2.0.0-{branch}-{arch}-SNAPSHOT`.

Vector search is written as a NEAREST BY join: Spark 4.2's own, and the
connector's `nearestByJoin` and `nearest_by_join` on 3.5 to 4.1. Over a Milvus
table the connector runs it and returns each query's global TopK, Spark's
result. APPROX searches the index files the snapshot pinned (the HNSW and IVF
families and FLAT) and EXACT scans the vectors; deletions and scalar
predicates apply before the search, and only the rows selected are read for
their columns. See [NEAREST BY](docs/reference-en.md#nearest-by-on-spark-42).
Cardinal index files require a Cardinal-enabled build of the pinned Knowhere
revision; the plain upstream CI artifact does not contain that engine.

Read a prepared snapshot with `milvus.snapshot.path`, or a backup with
`milvus.backup.dir`, plus its storage access configuration. These paths do not
contact Milvus and need no Milvus Java SDK. The caller prepares the input and
keeps its metadata, data and indexes available for the entire job. See
[Reading with DataFrames](docs/reference-en.md#1-reading-with-dataframes).

The existing service-connected client, Catalog and CALL interfaces remain
during migration; the final public reader will use offline inputs only.
`MilvusCatalog` exposes databases and collections as Spark namespaces and
tables, loads latest or time-travel snapshots, and implements collection
`CREATE TABLE` / `DROP TABLE` with validated Milvus field and index properties.
Creating a collection does not create a connector snapshot; it becomes readable
after Milvus produces one. The options are in the
[API reference](docs/reference-en.md).

## Project structure

The build has twelve sbt modules in four layers. Dependencies only point
downward, and the boundary between layer 2 and layer 3 is enforced at compile
time: a source file in `core`, `compat` or `client` that mentions
`org.apache.spark` fails the build.

| Layer | Module | What it holds |
|---|---|---|
| 1 | `native-runtime` | Verifies and extracts one unified native dependency bundle shared by both upstream bindings |
| 1 | `native-storage` | Compiles and packages the pinned milvus-storage JNI and Java/Scala API |
| 1 | `native-vector` | Compiles the pinned Knowhere PR #1829 Java API and delegates vector operations through its upstream JNI |
| 2 | `core` | The storage format itself: snapshots, manifests, delete files, schema, codecs, statistics, planning, segment read and write, indexes, object-storage access. No Spark. |
| 2 | `compat` | Adapters for the two non-standard read entry points that need format code: Storage V2 packed segments and a milvus-backup export directory. The option-string segment list is resolved in `spark-base`'s `options` package. |
| 2 | `client` | The gRPC client for the online Milvus service |
| 3 | `spark-base` | Connector sources shared by every Spark line. Not an sbt project, just a source directory. |
| 3 | `spark-3.5`, `spark-4.0`, `spark-4.1`, `spark-4.2` | One project per maintained Spark line. Each pins its own Spark, Arrow, antlr and Java version and compiles the shared sources. |
| 4 | `apps-4.0` | The backfill job users run; vector search is a NEAREST BY join that `spark-base` takes over |
| — | `integration-4.0` | Integration tests. Needs a real Milvus and MinIO; never published. |

Why the core layer carries no Spark dependency: one artifact serves all four
Spark lines, its tests run without a SparkSession, and the boundary is checked
by the compiler instead of by review.

Only the Spark layer has to be split per line, because the `TableCatalog` and
`ParserInterface` method sets differ, and Arrow, antlr and the Java target
version are pinned per line. The fat jar is an `assembly` task on
`spark-<line>`, not a module of its own. During the migration two fat jars are
usable, packed by the same rules: root's `sbt assembly`
(`spark-connector-assembly-*.jar`) for Spark 4.0, and `sbt spark42/assembly`
(`spark-milvus-4.2-bundle-*.jar`) for Spark 4.2. The 3.5, 4.0 and 4.1 tasks
have no merge or shading rules yet.

Four git submodules sit in the repository. `milvus-proto` supplies the
protobuf definitions: `common.proto` and `schema.proto` are generated into
`core` because the storage format itself is defined in protobuf, and the five
files carrying gRPC services are generated into `client`. `milvus-storage`
supplies the native storage library from `milvus-io/milvus-storage`, pinned at
official commit `5694520`, including PR #714's Tencent OIDC role chain while
retaining eager Parquet pre-buffering and the IO pool API from PR #703.
`knowhere` tracks the PR #1829 branch (`LawrenceTL92/knowhere-contrib`, branch
`codex/knowhere-jni-pr`) and supplies the C API, JNI and Java sources used by
`native-vector` and the unified native build. `docs/design` is the private
knowledge repository `zilliztech/vector-batch-knowledge`, with the connector's
designs under `projects/spark-milvus`; the build and CI do not read it.

Initialize the three native source submodules at their recorded gitlinks before
compiling, and `docs/design` as well if you have access to it:

```bash
git submodule update --init milvus-proto milvus-storage knowhere
git submodule update --init docs/design
```

## Documents

| Document | What it answers |
|---|---|
| [docs/reference-en.md](docs/reference-en.md) | Every option and entry point: vector search, the catalog, reads, writes, backup directories, SQL procedures, data types |
| [docs/user-guide-snapshot-backfill.md](docs/user-guide-snapshot-backfill.md) | How to add a field to a collection and fill it offline with the backfill job |
| [docs/contributing.md](docs/contributing.md) | Building, formatting, testing and the native bundle, beyond this README |
| [docs/releasing.md](docs/releasing.md) | Maven coordinates, the supported release matrix, candidate validation and Central publication |
| [native-build/README.md](native-build/README.md) | How the per-platform native bundle is built and published |

[AGENTS.md](AGENTS.md) is the entry point for agents and people working on the
code: what the project is, how it is layered, and the rules any change has to
satisfy. `CLAUDE.md` is a symlink to it. The design documents are kept in
`projects/spark-milvus` in the private repository
`zilliztech/vector-batch-knowledge`, mounted as the submodule `docs/design`;
nothing in this repository's build needs it.

## Environment

The quickest way to a complete build, package and test environment is the
development container: it is the `dev` stage of the root `Dockerfile`, built
locally on top of its toolchain stage, with the dependency caches in named
volumes and an optional
local Milvus and MinIO for the integration suite.

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
native bundle are described in [native-build/README.md](native-build/README.md).

Knowhere library loading uses the Java API and JNI from pinned PR #1829.
The API builds automatically; the native platform JAR is selected explicitly.
See [Knowhere library loading](docs/contributing.md#knowhere-library-loading)
for the real JNI smoke command and the `libjsig` preload requirement.

```bash
sbt clean compile package publishLocal   # compile and publish to the local repository
sbt assembly                             # fat jar with every dependency, Spark 4.0
sbt spark42/assembly                     # the same for Spark 4.2
sbt test                                 # unit tests, all modules
sbt integration40/test                   # integration tests, needs Milvus and MinIO
```

The Maven release workflow exports JVM and native artifacts independently and
tests both universal and platform-specific entries. See the
[release guide](docs/releasing.md) for coordinates, native prerequisites and the
build, consumer and publication commands.

The fat jar only loads Milvus segments when it carries the native
`milvus-storage` libraries for the platform it runs on. `make package` builds
them into the unified Storage/Knowhere bundle from the platform's profile under
`native-build/profiles/`, on Linux and macOS, x86_64 and aarch64 alike. The macOS
toolchain and its Conan configuration are in
[contributing.md](docs/contributing.md#macos).

`sbt compile` builds every module except `integration-4.0`, which sits outside
root's aggregate. To work on one, prefix the command with
its project id: `core/test`, `spark40/compile`, `apps40/test`. The ids drop the
dot, so the project for `spark-4.0` is `spark40`.

### Docker

The Docker build handles the native dependencies on an architecture-native
worker. Linux x86_64 and Linux aarch64 workers both build the unified
Storage/Knowhere bundle; either accepts a matching prebuilt unified JAR and its
`.properties` sidecar through `NATIVE_BUNDLE` instead.

```bash
docker build --build-arg PUBLISH_MAVEN=false -t spark-milvus .  # current architecture

# Trusted publication: BuildKit exposes the credential only to the publish RUN.
docker build \
  --secret id=maven_credentials,src=/path/to/sbt-credentials \
  --build-arg PUBLISH_MAVEN=true \
  -t spark-milvus .
```

| Build argument | Default | Meaning |
|---|---|---|
| `GIT_BRANCH` | `unknown` | Goes into the version string |
| `PUBLISH_TO_CENTRAL` | `true` | Whether to publish to Maven Central Snapshots |
| `PUBLISH_MAVEN` | unset | Repository-neutral publication override used by trusted CI |
| `MAVEN_CREDENTIALS_FILE` | `/run/secrets/maven_credentials` | In-build path of the BuildKit `maven_credentials` secret |
| `NATIVE_BUNDLE` | empty | Prebuilt unified Linux JAR inside the build context, with its checksum sidecar |
| `NATIVE_JOBS` | `50` | Native build concurrency, from 1 to 50 |
| `NATIVE_BUILD_OPTIONS` | empty | Unified source build options, including `--conan-lock` and `--with-cardinal` |

The version is derived as `2.0.0-{branch}-{arch}-SNAPSHOT`, for example
`2.0.0-refactor-v2-amd64-SNAPSHOT`.

Pull the jar back out of the image:

```bash
docker create --name temp spark-milvus
docker cp temp:/opt/spark-milvus/. ./
docker rm temp
```

## Using the connector

Use an assembly built from this 2.0 checkout for your Spark line and native
platform: root's assembly for Spark 4.0, or `spark42/assembly` for Spark 4.2.
The historical 1.x examples and artifacts do not implement the APIs below.
The root Maven coordinate remains `com.zilliz:spark-connector_2.13`; 2.0
snapshots use the snapshot repository:

```scala
ThisBuild / resolvers +=
  "Sonatype Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"
```

### Run the read and search demo

[scripts/spark_submit_demo.sh](scripts/spark_submit_demo.sh) runs the maintained
[search demo](scripts/search_demo.py). It creates a temporary collection,
inserts 2,000 vectors through the Milvus SDK, flushes and snapshots it, then
uses Spark to search that snapshot exactly. It checks that the connector
executes the join and compares the results with NumPy. It does not exercise
persisted-index APPROX search.

Prerequisites: Milvus 3.0+ with its object storage reachable from Spark, Spark
4.0 for Scala 2.13, Java 21, Python with `pymilvus` 3.0+ and `numpy`, and root's
assembly containing the matching native bundle (see [Building](#building)).
For a local Milvus and MinIO using the script's defaults:

```bash
export SPARK_HOME=/path/to/spark-4.0-bin-hadoop3
export JAVA_HOME=/path/to/jdk-21
export JAR=/path/to/spark-connector-assembly-2.0.0-refactor-v2-amd64-SNAPSHOT.jar
./scripts/spark_submit_demo.sh
```

The script defaults to `http://localhost:19530`, MinIO at `localhost:9000`,
bucket `a-bucket` and root `files`, with local-development credentials.
Override `MILVUS_URI`, `MILVUS_TOKEN` and the `S3_*` variables documented in the
script for your installation. The Milvus identity and object-storage identity
are separate. Driver and executors both need access to the storage. The demo
drops only its newly created collection and snapshot on exit; `KEEP=1` keeps
them for inspection. A successful run prints `Search demo passed`.

The script configures the session extension, `MilvusSparkPlugin`, catalog,
credential redaction and `libjsig` preload before Spark starts. Use the same
startup settings in your jobs; a cluster's executor preload path must point
to the JDK installed on its workers. The plugin is required for connector
search execution, not just faster library initialization.

### Query with Scala DataFrames

With a Spark session configured as above, this example reads an existing
collection snapshot and finds three matches per query. Replace the collection,
field names and query vectors with your own; here `embedding` is a
three-dimensional float vector and `id` and `title` are collection fields.

```scala
import com.zilliz.spark.connector.implicits._
import org.apache.spark.sql.functions.call_function
import spark.implicits._

val docs = spark.table("milvus.default.docs")
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

For the separate field-backfill workflow, start with the
[backfill guide](docs/user-guide-snapshot-backfill.md) and its runnable
[demo](scripts/run_backfill_demo.sh). Ordinary DataFrame append is staging
only; it is not an online insert or a full-refresh workflow.

## License

See [LICENSE](LICENSE).

## Automated PR review

AI review covers all changed files, including documentation, using fixed Git
objects without executing PR code. See the [operation guide](.github/ai-review/README.md)
for repository policy, coverage reports, deployment and incomplete reviews.
