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

During the repository migration, the source tree and transitional artifacts
still contain the old Catalog, client, management CALL, write and backup
implementations. New callers use the V3 file entry below. Writing, backfill,
index construction, snapshot output, old storage formats, backup and Cardinal
belong to the private `milvus-offline-compute` repository. Existing APIs remain
until the T15 source cleanup; their presence is not a public support commitment.

The 1.x line stopped at `v1.6.0` and is no longer maintained. The 2.0 rewrite
continues on `refactor/v2`. Candidate versions and artifact coordinates are
explicitly recorded by the [release workflow](docs/releasing.md).

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
`knowhere` tracks upstream `zilliztech/knowhere` on `main`, including merged
PR #1829, and supplies the C API, JNI and Java sources used by
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
| [docs/user-guide-snapshot-backfill.md](docs/user-guide-snapshot-backfill.md) | Retained pre-migration guide; maintained backfill callers now live in the private offline repository |
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

Use the public candidate's JVM implementation and matching OSS native JAR, or
its universal/platform-specific Maven entry, for the selected Spark line. See
[release coordinates](docs/releasing.md). The old root assembly remains a
transitional build target; new callers do not discover it by glob or assume it
contains the private jobs.

### Run the read and search demo

[scripts/spark_submit_demo.sh](scripts/spark_submit_demo.sh) verifies a supplied
public candidate and submits the [file-only example](scripts/search_demo.py).
It uses Spark 4.0.1 / Scala 2.13, Java 21 and PySpark from that distribution.
No Milvus endpoint, SDK, Catalog or fixture writer participates.

Create a private options JSON containing `milvus.snapshot.path` and the
corresponding `fs.*` / `extfs.*` storage settings. For example, a local immutable
fixture can use:

```json
{"milvus.snapshot.path":"snapshot.json","fs.storage_type":"local","fs.root_path":"/data/nearest"}
```

The separate queries JSON contains one or more `{ "qid": 1, "vector": [0.1, 0.2] }`
objects in an array. Keep credential-bearing options files outside the checkout.
For a small snapshot with `id` and `vector` columns:

```bash
export SPARK_HOME=/path/to/spark-4.0.1-bin-hadoop3
export JAVA_HOME=/path/to/jdk-21
export CANDIDATE=/verified/public-candidate
./scripts/spark_submit_demo.sh --options /private/read-options.json \
  --queries /data/queries.json --mode exact
./scripts/spark_submit_demo.sh --options /private/read-options.json \
  --queries /data/queries.json --mode approx
```

The example bounds its oracle input to 10,000 rows. EXACT must match an
independent L2 calculation. APPROX must load and search a persisted index and
reports recall against that oracle; an arbitrary approximate index is not
required to return the exact ranking. Both modes require actual connector native
search. `--id-column`, `--vector-column` and `--k` select the example fields and
result size. Driver and workers need access to the same fixed input and the JDK's
`libjsig` path. The script configures only the read extension and plugin and never
creates or deletes the input. A successful run prints `Search demo passed`.

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

Backfill and other write jobs run from the private offline repository. The
[retained backfill guide](docs/user-guide-snapshot-backfill.md) identifies the
migrated entry; its old public implementation is transitional until T15.

## License

See [LICENSE](LICENSE).

## Automated PR review

AI review covers all changed files, including documentation, using fixed Git
objects without executing PR code. See the [operation guide](.github/ai-review/README.md)
for repository policy, coverage reports, deployment and incomplete reviews.
