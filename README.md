# Milvus Spark Connector

Read and write Milvus collections from Apache Spark by going straight at the
Milvus storage format on object storage, rather than through the Milvus query
path. A read lists a snapshot, plans one Spark partition per segment, and pulls
Arrow column batches out of the segment files. A write produces segment files in
the same format and registers them back with Milvus.

**Requires Milvus 2.6 or later** (Storage V2). For Milvus 2.5 and earlier use
the `legacy` branch, which is no longer maintained.

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
`spark-<line>`, not a module of its own. During the migration the usable fat
jar is still root's `sbt assembly` (`spark-connector-assembly-*.jar`); the
per-line tasks have no merge or shading rules yet.

Four git submodules sit in the repository. `milvus-proto` supplies the
protobuf definitions: `common.proto` and `schema.proto` are generated into
`core` because the storage format itself is defined in protobuf, and the five
files carrying gRPC services are generated into `client`. `milvus-storage`
supplies the native storage library; until upstream merges the change it
depends on, `.gitmodules` fetches it from the fork `Thor-ChenBiao/milvus-storage`.
`knowhere` tracks the PR #1829 branch (`LawrenceTL92/knowhere-contrib`, branch
`codex/knowhere-jni-pr`) and supplies the C API, JNI and Java sources used by
`native-vector` and the unified native build. `docs/design` is the private
design repository `zilliztech/spark-milvus-design`; the build and CI do not
read it.

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
| [native-build/README.md](native-build/README.md) | How the per-platform native bundle is built and published |

[AGENTS.md](AGENTS.md) is the entry point for agents and people working on the
code: what the project is, how it is layered, and the rules any change has to
satisfy. `CLAUDE.md` is a symlink to it. The design documents are kept in the
private repository `zilliztech/spark-milvus-design`, mounted as the submodule
`docs/design`; nothing in this repository's build needs it.

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
sbt assembly                             # fat jar with every dependency
sbt test                                 # unit tests, all modules
sbt integration40/test                   # integration tests, needs Milvus and MinIO
```

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

Search jobs set
`spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin` so that
every executor loads the native bundle at start; the
[API reference](docs/reference-en.md) explains the setting. For the option
names and entry points, see the same reference.

## License

See [LICENSE](LICENSE).
