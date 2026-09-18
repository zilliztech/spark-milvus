# Working in this repository

[README.md](../README.md) covers installing the toolchain, building the
jar and running the example. This file covers what a contributor needs beyond
that.

## Project ids

sbt project ids cannot contain a dot, so the id and the directory differ for the
Spark lines. The directory is `spark-4.0`, the id is `spark40`, and the publish
coordinate is `spark-milvus-4.0`. The same split applies to `apps-4.0` /
`apps40` and `integration-4.0` / `integration40`.

Per-module commands take the id as a prefix:

```bash
sbt core/test spark40/compile apps40/test
sbt spark35/compile spark41/compile spark42/compile   # cross-line check
```

The root project has no sources of its own. It depends on `spark40` and
`apps40` and exists to assemble the fat jar under the 1.x artifact name, so the
Dockerfile and the cloud consumers do not have to change.

Only the root assembly is published during the migration. Its POM omits the
embedded `spark40` and `apps40` module dependencies, which are not published
separately yet; external dependencies remain in the POM.

Use root's `assembly` for the runnable fat jar. Per-line assembly tasks do not
yet share its merge and shading rules: `spark40/assembly` currently fails on
module descriptors, Netty version metadata and FastDoubleParser notices. A
successful source-classpath test does not validate those unfinished bundles.

## Imports

Import types and objects at the top of Scala and Java source files, then use
their short names in code. Do not write fully qualified names inline in method
calls, constructors, type annotations or method signatures. Resolve name
conflicts with descriptive Scala import aliases or imported enclosing types.
Follow the import grouping and ordering in `.scalafmt.conf`.

For example:

```scala
import com.zilliz.milvus.storage.expr.PlanParser

filter.foreach(PlanParser.parse)
```

Do not write `filter.foreach(com.zilliz.milvus.storage.expr.PlanParser.parse)`.
Package declarations, imports and class-name strings required by reflection or
configuration still use the complete package name.

## Build files

Follow [sbt principles and practices](design/engineering/sbt.html) when reviewing
or changing the build, using the repository's
[sbt skill](../.agents/skills/spark-milvus-sbt/SKILL.md). `build.sbt` holds module
wiring and publication decisions; root run, assembly and publication details
are named settings in the same file.

| File | What to change there |
|---|---|
| `project/Versions.scala` | Library versions and the Spark line matrix |
| `project/Dependencies.scala` | Dependency coordinates, scopes and dependency groups |
| `project/Modules.scala` | Shared compile/test settings, checks and Scala cross-version settings |
| `project/KnowhereBuild.scala` | Pinned upstream API compilation, native artifact verification and packaged JNI smoke |
| `project/NativeBundle.scala` | Unified native resource selection, source pins, manifest and ELF validation |
| `project/plugins.sbt` | Build plugins and their meta-build dependencies |

## Tests that need the native library

Suites that call into C check for upstream `libmilvus-storage-jni` first and cancel
themselves when it is absent, so a machine without it still gets a green run
with those suites reported as canceled rather than failed:

- `StorageNativeTest`, `WriterRoundTripTest`, `SegmentWriterTest` and `SegmentReaderTakeTest` in core
- `MilvusV3PartitionWriterLifecycleTest` and `HadoopEndpointSnapshotTest` in spark-4.0

The UAT suites — `StorageNativeUatTest` and `SegmentReaderUatTest` in core,
`StorageFullChainUatTest`, `SnapshotReadUatTest`, `ConnectorWriteReadUatTest`
and the scenario suite `uat.DataFrameScenariosUatTest` in spark-4.0 — cancel
on their own environment variables as well, so they stay canceled even with
the library present. The scenario suite is also compiled into the 3.5, 4.1 and
4.2 lines, so `spark35/testOnly ...DataFrameScenariosUatTest` runs the same
scenarios there. The 3.5 line needs a JDK 17 for that run: Arrow 12, which
Spark 3.5 ships, cannot allocate on JDK 21. Name it in
`SPARK35_TEST_JAVA_HOME` and the line's test JVM forks from it; unit tests do
not allocate through the C Data Interface and run on the build's JDK 21.

Use the current run's summary to report successful, failed, canceled, ignored
and pending tests, and completed or aborted suites. Name the native or UAT
suites that canceled and state the missing library or environment prerequisite;
canceled tests did not pass and provide no coverage for that run.

`integration-4.0` needs a real Milvus on 19530 and MinIO on 9000. It is outside
the root aggregate, compiles in CI and is not run there.

## Building the native library

For a bundle containing both storage and Knowhere, use the
[unified native build](#unified-native-bundle) below. The standalone migration
targets `make build-milvus-storage && make copy-native-libs` build and copy
upstream `libmilvus-storage`, `libmilvus-storage-jni` and their dependencies
under `native-storage/src/main/resources/native/<platform>/`. The JNI methods
and `NativeLibraryLoader` belong to milvus-storage. The former Connector
`libnative-storage-jni`, its C++ sources and `build-native-jni` target have been
removed. Building requires Conan, CMake and a Rust toolchain.

Initialize the pinned native source submodules before compiling:

```bash
git submodule update --init milvus-storage knowhere
```

The `native-storage` sbt module compiles `milvus-storage/java/src/main` for
Scala 2.12 and 2.13 instead of importing upstream's sbt project or a Scala 2.13
binary JAR. `Compile / sourceGenerators` copies the upstream Java/Scala sources
to `sourceManaged` with `Sync`; Connector formatting operates on its own
sources and does not rewrite the submodule. Arrow is `provided`, with the Spark
4.0 line as the API compilation baseline and each Spark line supplying runtime
Arrow. Source compilation does not start a native build.

The upstream JNI migration is implemented and has fresh cross-version,
unit, native load-order and real UAT snapshot/index results. Current results
and remaining JNI diagnostics and packaging limitations are recorded in
[the storage I/O validation state](design/architecture/storage-io.html#state).

Upstream milvus-storage supports local macOS builds: `cpp-mac-ci.yml` builds
on `macos-26` with conan 2.25.1, CMake 3.31.10 and LLVM 18 from brew.
This JNI migration has been tested on Linux x86_64 only; the upstream CI job
does not establish that this modified dependency combination works on macOS.

### Two things that will bite

**Pin a CMake 3.x somewhere durable.** Several packages in the dependency chain
cap their CMake policy range below 4, so the conan profile has to set
`tools.cmake:cmake_program`. A path under `/tmp` disappears on cleanup and the
build then fails with `cmake: No such file or directory` before compiling
anything — the error names the missing binary, not the real problem.

**The artifact is large.** An earlier macOS build measured
`libmilvus-storage.dylib` at 481 MB, including 162 MB of symbols, and its
upstream JNI bridge at 177 KB. Those measurements are not the size of the
current candidate. Native builds include Arrow, Parquet, cloud SDKs and the
Rust bridge; the dependency closure also contains shared libraries. Validate
the complete packaged dependency set rather than assuming fully static linkage.
`native-storage/src/main/resources/native/` is gitignored, so none of this is
committed. Section 4.2 of
[storage-access.html](design/architecture/storage-access.html) covers what the
size costs.

### What the Makefile does per platform

The Makefile derives the library suffix and resource platform from `uname`.
Both platforms copy the upstream engines from `Release` and Conan dependencies
from `Release/libs`, preserving the `ossl-modules` and `engines-3` subdirectories.

| | macOS | Linux |
|---|---|---|
| Suffix | `.dylib` — `add_library(... SHARED)` sets no `SUFFIX` | `.so` |
| Build output | `cpp/build/Release` and `cpp/build/Release/libs` | `cpp/build/Release` and `cpp/build/Release/libs` |
| Resource path | `native/darwin-aarch64/` | `native/linux-<arch>/` |

The upstream `NativeLibraryLoader` selects resources under
`native/<platform>/`. A library copied directly into `native/` is outside that
platform directory and cannot satisfy the packaged-library load.

## Unified native bundle

The unified Linux platform JAR contains both upstream JNI entry libraries and
one dynamically linked dependency set. The implementation and acceptance status
are documented in [the native build design](design/engineering/native-libraries.html).
The native build is explicit; ordinary sbt compilation does not start Conan or
CMake. The independent project in `native-build/` defines both engines, their
upstream JNI implementations and the optional Cardinal plugins without executing
their upstream CMake files. One Conan host graph supplies shared dependencies
to one CMake/Ninja build tree.

Build both engines, package their shared dependencies, and select the result:

```bash
make native-bundle NATIVE_JOBS=50 NATIVE_BUILD_OPTIONS=--with-cardinal
make package NATIVE_BUNDLE="$PWD/target/native-build/linux-x86_64/milvus-native-linux-x86_64.jar"
```

The source build currently uses the Linux x86_64 GCC 12 profile in
`native-build/profiles/`. It needs Conan 2, CMake 3.27.5, Ninja, the profile's
compilers, Rust, libclang, a JDK, `patchelf` and access to the pinned source
repositories and Conan recipes. Rust bindgen loads libclang when building the
storage bridge's `custom-labels` dependency; install `libclang-dev` on Ubuntu.
`--with-cardinal` also needs access to both pinned Cardinal
revisions. `dependencies.json` selects the newer version when the two source
recipes conflict. Every dependency uses the exact upstream recipe revision in
`native-build/dependencies.json`; the repository does not export replacement
recipes or rewrite cached recipes. Integration-specific link relationships live
in `native-build/cmake/Storage.cmake`, `Knowhere.cmake` and `Cardinal.cmake`.
Conan can reuse a cached package only when the recipe revision, configuration
and package ID match.

`NATIVE_JOBS` accepts 1 through 50. The driver builds the Rust bridge first with
one Ninja job and up to `NATIVE_JOBS` Cargo jobs. It then builds the C++ targets
with up to `NATIVE_JOBS` Ninja jobs and limits any Cargo recheck to one job.
This sequencing prevents the Rust and C++ schedulers from each starting a full
parallel build at once. Engine and JNI targets are installed under
`NATIVE_WORK_DIR/install/lib/` before their dependency closure is staged.

Native acceptance requires both JNI libraries to load in two fresh JVMs, using
storage-first and Knowhere-first order. Staging, packaging and sbt use the same
checker in `native-build/jvm_load.py`; a matching JDK with `libjsig` is required.
Per-library ELF and relocation checks remain diagnostic and do not block this
acceptance. JNI functional tests and real Spark queries must still run against
the resulting assembly; successful `System.load` alone does not prove them.

The build records source changes, recipe identities, dependency locks, the
resolved graph, compiler commands and library hashes under `NATIVE_WORK_DIR`.
The resource JAR's provenance embeds only normalized source and package
identities and digests of the lock, graph, build inputs, libraries, audit
results and external evidence. Collected licenses are packaged alongside it.
The JAR does not embed build-machine absolute paths. Full source snapshots, the
complete Conan graph, `compile_commands.json`, native build commands, the
build-input backup and the expanded CMake trace remain under `NATIVE_WORK_DIR`.
Reusing a work directory requires identical source and build inputs. Repeated
builds validate a new candidate before replacing
`bundle/`; previous successful bundles and failed candidates remain available
for diagnosis. Pass `--conan-lock` through `NATIVE_BUILD_OPTIONS` to reuse a
reviewed complete dependency lock in another build directory. See
[native-build/README.md](../native-build/README.md) for the directory layout and
cache options.

On Linux x86_64, `make all`, `package` and `quick-build` use the unified bundle.
`NATIVE_BUNDLE` selects an existing Linux platform JAR and skips native
compilation; otherwise `NATIVE_WORK_DIR` holds the build and Conan reuses
compatible cached packages. Linux aarch64 and macOS retain the existing
storage-only build when no bundle is selected. Linux aarch64 can consume a
matching prebuilt unified bundle; the source profile does not cross-compile it.
Docker uses the same `native-resources` target as Make. Both native build paths
limit concurrency to `NATIVE_JOBS` (1..50) and preserve initialized submodule
checkouts. The storage-only resource target always invokes the incremental
build before copying, including when previous libraries already exist.
Docker keeps Conan packages, Cargo registry/Git downloads and ccache in locked
BuildKit cache mounts, so a later build failure does not discard completed
dependencies. Conan configuration is initialized inside the mount, including
when the cache is empty. Native work directories and Cargo compilation outputs
remain local to each build; cache reuse does not skip source or provenance checks.
This does not establish joint Storage/Knowhere support on platforms
without a validated unified bundle.

Select the resulting resource-only JAR and its `.properties` checksum sidecar:

```bash
sbt -Dmilvus.native.bundle=/absolute/path/to/milvus-native-linux-x86_64.jar \
  native-runtime/verifyNativeBundle native-vector/knowhereSmoke
sbt -Dmilvus.native.bundle=/absolute/path/to/milvus-native-linux-x86_64.jar test assembly
```

Before native tests, preload the selected JRE's `lib/libjsig.so` as described
below. The resulting root assembly contains the bundle resources. At runtime,
one class loader extracts and verifies them in one private directory; both
upstream JNI loaders use that directory. The external `.properties` checksum
sidecar is a build input and is not needed next to the deployed assembly.

Bundle-selected functional test JVMs set `LD_BIND_NOW=1` and an empty external
library path. A JVM may still request lazy binding when loading a native
library; this setting does not prove every function symbol has been resolved.
The shared loading checker clears `LD_BIND_NOW` and verifies completed
`System.load` calls. No external library directory or runtime mutation of
`java.library.path` is needed.

The system provides glibc, libstdc++, libgcc_s and `libz.so.1`. Zulu JDKs load
system zlib before connector initialization; a second bundled copy cannot
override those existing symbol bindings. Native checks verify the required
zlib symbol versions against the system provider. Other non-system dependencies
are packaged together. Per-library diagnostics check Cardinal's two plugins
with their declared Knowhere parent callbacks and other libraries individually.
Those diagnostic results do not replace the JVM loading and functional tests.

`milvus.native.bundle` and `knowhere.native.jar` are mutually exclusive. When a
unified bundle is selected, the old storage resources do not enter the build
classpath. A malformed bundle fails; it never falls back to those old resources.
An explicit `knowhere.native.path` pointing elsewhere is rejected at runtime.
The connector sets this property only during Knowhere initialization under a
JVM-shared lock, then restores its prior value on success or failure. Multiple
isolated copies of the Connector's JNI bindings in one JVM remain unsupported.
Legacy input options and records below remain available during migration; their
previous validation results do not validate a newly built bundle.

Storage uses the same explicit-path handoff. `NativeStorageLibrary` obtains the
verified `libmilvus-storage-jni.so` path from `native-runtime`, temporarily sets
`milvus.storage.native.path`, calls the upstream `NativeLibraryLoader`, and
restores the prior property on success or failure. The upstream loader owns
`System.load`. A different configured path, or an upstream loader initialized
before this handoff, fails visibly instead of selecting another library. When
no unified bundle is present, the adapter leaves selection to the upstream
loader, which keeps its packaged-resource and system-library-path behavior.

### Checking native resource merging

Assembly passes through a single native resource without buffering it. When
multiple JARs supply the same path, `NativeBundle.nativeMergeStrategy` compares
SHA-256 and byte length using a fixed-size buffer and rejects differing content.
The plugin's general `deduplicate` strategy buffers whole entries and exhausted
a 4 GB heap with the current 478 MB storage library. The streaming strategy
passes the same assembly command at 4 GB. With sbt-assembly 2.1.1, a custom merge
strategy disables assembly output caching, so repeated assembly commands repack
the JAR; compilation, native validation and native build caches remain enabled.

After changing that strategy, run its bounded-memory regression probe from the
repository root. First run an sbt command to compile the build definition, then
use Java 21:

```bash
native_probe_cp="$(cat project/target/streams/compile/dependencyClasspath/_global/streams/export):$PWD/project/target/scala-2.12/sbt-1.0/classes"
mkdir -p target/native-merge-probe
"$JAVA_HOME/bin/java" -Xmx256m -cp "$native_probe_cp" scala.tools.nsc.Main \
  -classpath "$native_probe_cp" -d target/native-merge-probe \
  scripts/tests/NativeMergeProbe.scala
"$JAVA_HOME/bin/java" -Xmx64m \
  -cp "target/native-merge-probe:$native_probe_cp" NativeMergeProbe
```

The probe compares two streams larger than 512 MB with a 64 MB heap, verifies
single-resource pass-through, rejects content and length differences, and checks
stream closure after an I/O failure. Also run assembly with the real bundle;
the probe does not validate the final artifact or its runtime classpath.

## Knowhere library loading

The root `knowhere` submodule follows the Knowhere PR #1829 branch; the
superproject gitlink fixes the exact commit used by every build. `native-vector`
compiles `knowhere/java/src/main/java` with `javac --release 11`; it does not
implement another C API or JNI bridge. The API JAR is cached under
`native-vector/target/knowhere`. An ordinary compile or unit-test run needs a
JDK and an initialized submodule, but does not download source or compile or
load the native engine.

Build the pinned upstream native engine explicitly on the target Linux
architecture, using the prerequisites listed by the script:

```bash
JAVA_HOME=/path/to/jdk21 scripts/build-knowhere.sh build --jobs 2
```

For persisted Cardinal indexes, build that engine from the same pinned Knowhere
revision and its pinned Cardinal tags:

```bash
JAVA_HOME=/path/to/jdk21 scripts/build-knowhere.sh build --jobs 16 \
  --with-cardinal --cardinal-repository /path/to/authorized/cardinal-clone
```

On success this writes a separate `target/knowhere-native/<revision>/cardinal/<platform>`
artifact and records `build.with_cardinal=true`, the Cardinal revisions and the
actual CMake configuration. It does not replace the existing OSS artifact.
The upstream Cardinal recipe uses `-march=native`; treat this build as a local
verification artifact until CPU portability has been separately established.
The connector derives `META-INF/milvus/knowhere-runtime.properties` from the
selected, checksum-verified artifact's `build.with_cardinal` provenance field.
`NativeVectorLibrary.RuntimeInfo.cardinalSupported()` exposes that feature;
an arbitrary `knowhere.native.path` override does not establish Cardinal support.
Validate the storage/Knowhere dependency combination before registering its hashes.

The earlier `9dc2b8ad` checkout stopped at an upstream DiskANN exact-distance
assertion when Cardinal used quantized refinement. That result remains
historical: the current PR branch has changed the DiskANN tests and must be
rebuilt and validated from its new gitlink before any native or real-data result
is claimed. See the measured results and limitations in the
[vector design](design/architecture/vector-search.html#interop).

The gitlink now points at 29210a33, where the upstream DiskANN tests keep the
exact check for OSS DiskANN and, in Cardinal builds, check ordering, recall and
a 0.05 + 0.02 x distance tolerance instead (the C test through
`KNOWHERE_WITH_CARDINAL`; `DiskAnnIT` through
`-Dknowhere.test.approximateDistances=true`, which `scripts/build-knowhere.sh`
passes only with `--with-cardinal`). Both `build-knowhere.sh` variants at that
revision passed ctest, the JNI suite and `native-vector/knowhereSmoke` on Linux
x86-64; the unified `native-build` bundle has not been rebuilt at this gitlink
yet, so its validation is still due.

`scripts/build-knowhere.sh import-ci` only accepts a CI artifact explicitly
registered for the current gitlink. No artifact is registered for the current
PR branch head, so use `build`. A successful path leaves the platform JAR and a
`.jar.properties` provenance sidecar under
`target/knowhere-native/<revision>/<platform>/`. The sidecar binds the JAR
checksum to the submodule revision. Keep these files together.

Select that absolute JAR path for a native smoke or an assembly:

```bash
sbt -Dknowhere.native.jar=/absolute/path/to/knowhere-jni-1.0.0-SNAPSHOT-linux-x86_64.jar \
  native-vector/knowhereSmoke
sbt -Dknowhere.native.jar=/absolute/path/to/knowhere-jni-1.0.0-SNAPSHOT-linux-x86_64.jar \
  assembly
```

The selection is validated against the source pin, JAR checksum, platform,
C ABI, manifest and library checksums. It is optional for ordinary reads;
without it an assembly carries the Java API only, and requesting vector loading
fails with a missing-platform-JAR error. Assembly preserves `io.knowhere` class
names, upstream licenses and `native/knowhere/1/<platform>/` resources, and
rejects conflicting Knowhere resources.

When storage and Knowhere are packaged together, the build scans
`native-vector/storage-compatibility*.properties`. Exactly one record must match
the platform, storage engine SHA-256, full storage-native resource fingerprint
and Knowhere platform JAR SHA-256; zero or
multiple matches fail the build. Each record lists dependency hashes and audited
SONAME aliases. The build generates storage's
shared dependency resources from the original Knowhere dependency bytes, including
their aliases and license records. This prevents the loaders from selecting
different Folly binaries depending on load order. Original storage resources and
the upstream Knowhere JAR remain unchanged. An unverified pair fails the combined
build; validate both search/load orders and the full storage suite before
registering another pair. Without Knowhere, ordinary storage resources are used.

The loader reads
Milvus binlog/Parquet payloads and optional `SLICE_META`, or a Cardinal raw
`_mem.index.bin` stream. Payload markers select the matching Faiss or Cardinal
engine; a shared HNSW name alone does not establish format compatibility.
Each task owns and closes its index. There is no cross-task index cache.
The historical Knowhere `9dc2b8ad` JNI migration and corrected native dependency
build passed real UAT Cardinal HNSW queries against direct JNI results and an
independent 100,000-row reference. Actual storage entry-library relocation and
both native load orders also passed for that artifact. The current `1fff20db`
gitlink requires a rebuilt native bundle and a fresh validation run. The complete
validation record, remaining third-party JNI diagnostics and packaging limitations are in
[the storage I/O validation state](design/architecture/storage-io.html#state).
Local OSS fixtures do not replace real-instance-data validation. Tests live in
the external `milvus-spark-demo` validation project.

`MilvusSearch.search` takes a query set and returns each query's global TopK,
running `core.index.SegmentSearch` over the segment sets `SearchPlan` cut.
Missing native libraries fail the query. Planning, packing and merge tests run
in the ordinary suite. The explicit real-native check needs the selected
platform JAR and the JRE's `libjsig`:

```bash
sbt -java-home "$JAVA_HOME" -Dknowhere.native.jar=/absolute/path/to/platform.jar \
  "set core / Test / envVars += \"LD_PRELOAD\" -> \"$JAVA_HOME/lib/libjsig.so\"" \
  "set spark40 / Test / envVars += \"LD_PRELOAD\" -> \"$JAVA_HOME/lib/libjsig.so\"" \
  'spark40/Test/runMain com.zilliz.spark.connector.read.SegmentIndexSearchSmoke'
```

`NativeVectorLibrary.load()` explicitly initializes the upstream binding and
reports the native C ABI and index format versions. The upstream loader owns
extraction and `System.load`, including the optional development override
`-Dknowhere.native.path=/absolute/path/to/libknowhere_jni.so`. The override does
not verify build provenance or package checksums and requires its dependencies
to be available. A library load does not validate persisted Milvus index files.

For HotSpot, preload the **running JRE's** `lib/libjsig.so` before JVM startup.
Apply this to each driver or executor JVM that will load Knowhere; Java code
cannot establish signal chaining after startup. Do not package another JDK's
`libjsig` into the connector. The explicit smoke task supplies it automatically,
uses only packaged adapter/API/native JARs plus its test entry point, clears
build-library paths, and checks `-Xcheck:jni` diagnostics. Missing libraries or
failed native calls fail the smoke; it never cancels itself. Its log is
`native-vector/target/knowhere-smoke/jni.log`.

The pinned upstream native package reports missing `milvus-common` license
material. Preserve `missing-licenses.txt` and the bundled license records;
complete the upstream material before distributing native artifacts.

## JVM version

Use Java 21. On Java 26 every test that starts a SparkSession aborts with
`UnsupportedOperationException: getSubject is not supported`, because
`Subject.getSubject` was removed and Spark still calls it. The failure looks
like a code problem and is not one.

## How tests find their fixtures

Several suites read fixture files by a path relative to the repository root, for
example `core/src/test/data/seg_manifest.avro`. Forked tests would otherwise run
with the subproject as their working directory, so `Modules.nativeTest` pins
`baseDirectory` back to the build root. If a fixture path suddenly resolves
wrong, that setting is why.

The same settings block supplies the `--add-opens` flags Arrow needs. Without
them the first column batch throws `Failed to initialize MemoryUtil`.

## Formatting

Before every commit, including documentation-only commits, run the formatter
and its check with Java 21 from the repository root:

```bash
sbt 'set Global / concurrentRestrictions += Tags.limitAll(1)' scalafmtAll scalafmtCheckAll
git diff --check
```

The restriction serializes tasks for this sbt session while preserving existing
restrictions. All four Spark projects include `spark-base`, so an unrestricted
aggregated `scalafmtAll` can write the same file concurrently. One such run
truncated `Utf8FromBinaryColumn.scala` to a blank line. Do not run multiple
formatter processes against the same checkout, and review the diff for missing
code as well as formatting changes before staging. An empty Scala file can pass
`scalafmtCheckAll`.

The root commands cover the aggregated projects. When changing
`integration-4.0`, which is outside that aggregate, also run:

```bash
sbt integration40/scalafmtAll integration40/scalafmtCheckAll
```

When changing `build.sbt` or Scala/sbt files under `project/`, also run
`sbt scalafmtSbt scalafmtSbtCheck`. Use the pinned `.scalafmt.conf`; do not
change its rules to make a check pass. Repeat the relevant check after further
source edits and resolve failures before committing. Keep unrelated formatting
repairs in a separate commit from functional changes, preserving other
contributors' work. `git diff --check` checks whitespace errors and is not a
substitute for Scalafmt. After formatting and checks pass, run the full unit
test suite described below.

## Unit tests

Before every commit, including documentation-only commits, run the full root
unit test suite with Java 21 from the repository root, after completing the
[formatting and checks](#formatting):

```bash
sbt test
```

This runs the root aggregate and compiles the source and test code needed by
its tests. The full run must pass before committing. A focused `testOnly` run
is useful while fixing a test but does not replace this full run. Any test
failure, aborted suite or incomplete run blocks
the commit: fix the cause and rerun the full root suite. Do not add exclusions,
filters, ignored tests or cancellation conditions to make the run pass.

Existing native-library and UAT cancellation conditions remain part of the test
setup; report them and the actual counts as described in
[Tests that need the native library](#tests-that-need-the-native-library).
The root run does not include `integration40/test`, which requires live Milvus
and MinIO. Additional Scala cross-version checks, integration tests, native
builds and publication follow the task's scope and authorization.

## The protobuf split

`common.proto` and `schema.proto` are generated into `core` with `grpc = false`,
because the storage format itself is defined in protobuf: a snapshot embeds a
`CollectionSchema`. The five files that carry gRPC services are generated into
`client` with `grpc = true`, using an include path so they reference the message
classes core already produced. Neither module generates the same `.proto` twice.

Adding a proto file means deciding which side it belongs on, and adjusting the
`includeFilter` in core or the `excludeFilter` in client to match.

## Jackson

Layer 2 pins jackson-databind, jackson-core and jackson-annotations to one
version, because parquet-hadoop drags in a databind newer than
jackson-module-scala and mixing them throws `JsonMappingException` at runtime.

Do not apply that pin to the Spark lines. Spark ships a self-consistent jackson
set, and pinning there drags databind *below* Spark's own jackson-module-scala,
which throws the same way from the other direction.

## Commits

Write the message in English, explain why rather than what, and name the
verification you actually ran. When a change contradicts a design document, say
so in the message and fix the document in the same commit.
