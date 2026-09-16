# Milvus Spark Connector

Read and write Milvus collections from Apache Spark by going straight at the
Milvus storage format on object storage, rather than through the Milvus query
path. A read lists a snapshot, plans one Spark partition per segment, and pulls
Arrow column batches out of the segment files. A write produces segment files in
the same format and registers them back with Milvus.

**Requires Milvus 2.6 or later** (Storage V2). For Milvus 2.5 and earlier use
the `legacy` branch, which is no longer maintained.

Two lines exist right now. The 1.x line is frozen at tag `v1.6.0` and only takes
fixes. The 2.0 line is a rewrite on branch `refactor/v2`, versioned
`2.0.0-{branch}-{arch}-SNAPSHOT`.

Persisted vector-index queries use `MilvusSearch.search`, which loads the
snapshot's HNSW index files, applies deletions and scalar predicates before
search, retrieves projected hit rows, and returns global TopK. See the
[query contract](docs/reference-en.md#persisted-index-search-refactorv2).
Cardinal index files require a Cardinal-enabled build of the pinned Knowhere
revision; the plain upstream CI artifact does not contain that engine.

## Project structure

The build has eleven sbt modules in four layers. Dependencies only point
downward, and the boundary between layer 2 and layer 3 is enforced at compile
time: a source file in `core`, `compat` or `client` that mentions
`org.apache.spark` fails the build.

| Layer | Module | What it holds |
|---|---|---|
| 1 | `native-storage` | JNI over the `loon_*` C interface of milvus-storage |
| 1 | `native-vector` | Loads pinned Knowhere PR #1829 artifacts and delegates BruteForce through its upstream Java API and JNI |
| 2 | `core` | The storage format itself: snapshots, manifests, delete files, schema, codecs, statistics, planning, segment read and write, indexes, object-storage access. No Spark. |
| 2 | `compat` | Adapters for three non-standard read entry points: Storage V2 packed segments, an offline segment list passed through options, and a milvus-backup export directory |
| 2 | `client` | The gRPC client for the online Milvus service |
| 3 | `spark-base` | Connector sources shared by every Spark line. Not an sbt project, just a source directory. |
| 3 | `spark-3.5`, `spark-4.0`, `spark-4.1`, `spark-4.2` | One project per maintained Spark line. Each pins its own Spark, Arrow, antlr and Java version and compiles the shared sources. |
| 4 | `apps-4.0` | The jobs users run: backfill, brute-force search, diagnostic tools, and the legacy gRPC insert path |
| — | `integration-4.0` | Integration tests. Needs a real Milvus and MinIO; never published. |

Why the core layer carries no Spark dependency: one artifact serves all four
Spark lines, its tests run without a SparkSession, and the boundary is checked
by the compiler instead of by review. Ray cannot reuse it — Ray is Python and
cannot depend on a JVM jar. What it can share is the C ABI in layer 1.

Only the Spark layer has to be split per line, because the interfaces differ:
`ProcedureCatalog` exists only in Spark 4.0 and later, and Arrow, antlr and the
Java target version are pinned per line. The fat jar is an `assembly` task on
`spark-<line>`, not a module of its own.

Two git submodules sit at the repository root. `milvus-proto` supplies the
protobuf definitions: `common.proto` and `schema.proto` are generated into
`core` because the storage format itself is defined in protobuf, and the five
files carrying gRPC services are generated into `client`. `milvus-storage`
supplies the native storage library.

## Documents

[AGENTS.md](AGENTS.md) is the entry point for working on this repository: it
routes to whichever file answers a question and lists the rules any change has
to satisfy. `CLAUDE.md` is a symlink to it.

The 2.0 design lives in `docs/design`. Its entry groups documents by the question
you need to answer: architecture or engineering conventions.

| Document | What it answers |
|---|---|
| [docs/design/README.md](docs/design/README.md) | The layering, the read and write paths, priorities, open decisions, and the decision log |
| [docs/design/capabilities.md](docs/design/capabilities.md) | The 51 capabilities the connector commits to, by id |
| [docs/design/architecture/modules.md](docs/design/architecture/modules.md) | Modules, packages, directories, the twelve build constraints, and the 1.x to 2.0 migration table |
| [docs/design/architecture/overview.html](docs/design/architecture/overview.html) | The illustrated version of the design |
| [docs/design/engineering/sbt.html](docs/design/engineering/sbt.html) | Principles and practices for maintaining the sbt build |

`docs/reference-en.md` is the user-facing API reference for the connector
options and entry points. [docs/contributing.md](docs/contributing.md) covers
the build mechanics beyond this README, [docs/context.md](docs/context.md) what
the design depends on outside this repository, and
[docs/writing.md](docs/writing.md) the standard for documents here.

## Environment

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

Knowhere library loading uses the Java API and JNI from pinned PR #1829.
The API builds automatically; the native platform JAR is selected explicitly.
See [Knowhere library loading](docs/contributing.md#knowhere-library-loading)
for the native build/import script, packaging and real JNI smoke command.

```bash
sbt clean compile package publishLocal   # compile and publish to the local repository
sbt assembly                             # fat jar with every dependency
sbt test                                 # unit tests, all modules
sbt integration40/test                   # integration tests, needs Milvus and MinIO
```

`sbt compile` builds all eleven modules. To work on one, prefix the command with
its project id: `core/test`, `spark40/compile`, `apps40/test`. The ids drop the
dot, so the project for `spark-4.0` is `spark40`.

### Docker

The Docker build handles every dependency, including the native milvus-storage
library.

```bash
docker build -t spark-milvus .                                  # current architecture
docker build --build-arg PUBLISH_TO_CENTRAL=false -t spark-milvus .
```

| Build argument | Default | Meaning |
|---|---|---|
| `GIT_BRANCH` | `unknown` | Goes into the version string |
| `PUBLISH_TO_CENTRAL` | `true` | Whether to publish to Maven Central Snapshots |

The version is derived as `2.0.0-{branch}-{arch}-SNAPSHOT`, for example
`2.0.0-refactor-v2-amd64-SNAPSHOT`.

Pull the jar back out of the image:

```bash
docker create --name temp spark-milvus
docker cp temp:/workspace/target/scala-2.13/spark-connector-assembly-*.jar ./
docker rm temp
```

## Using the connector

The published coordinate is `com.zilliz:spark-connector_2.13`
([Maven](https://mvnrepository.com/artifact/com.zilliz/spark-connector_2.13)).
Released versions currently exist to exercise the release process; active work
lands in the snapshots, which need the snapshot repository:

```scala
ThisBuild / resolvers +=
  "Sonatype Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"
```

A runnable example lives in
[milvus-spark-connector-example](https://github.com/SimFG/milvus-spark-connector-example).
Build it with `sbt clean compile package`, then:

```bash
spark-submit-wrapper \
  --jars /xxx/spark-connector-assembly-x.x.x-SNAPSHOT.jar \
  --class "example.HelloDemo" \
  /xxx/milvus-spark-connector-example_2.13-0.1.0-SNAPSHOT.jar
```

`HelloDemo` reads a collection called `hello_spark_milvus`, so create it first
and make sure the local Milvus is reachable. Pre-built assembly jars are also
attached to the
[GitHub releases](https://github.com/SimFG/milvus-spark-connector/releases).

For the option names and entry points, see the
[API reference](docs/reference-en.md).

## License

See [LICENSE](LICENSE).
