# Working in this repository

[README.md](../README.md) covers installing the toolchain, building the
jar and running the example. This file covers what a contributor needs beyond
that.

## Development container

`scripts/devcontainer.sh` runs the toolchain of the root `Dockerfile` as a
container with the checkout bind-mounted at `/workspace` and the Conan, ccache,
Cargo, Coursier, Ivy and sbt caches in named volumes. Build outputs stay in the
checkout's `target/`. The container runs as your uid, so files it writes belong
to you; `DEV_UID` and `DEV_GID` override the value taken from `id`.

```bash
scripts/devcontainer.sh up                 # build the dev image if needed, start the container
scripts/devcontainer.sh init               # submodules, Conan profile and remote (idempotent)
scripts/devcontainer.sh shell              # login shell; NATIVE_JOBS defaults to the container's CPUs
scripts/devcontainer.sh run make package   # one command, then return
scripts/devcontainer.sh up --services      # also start Milvus, etcd and MinIO for integration40/test
scripts/devcontainer.sh run --env-file ~/.spark-milvus-uat.env sbt spark40/test   # UAT variables from a file
scripts/devcontainer.sh down               # stop everything; caches survive
scripts/devcontainer.sh clean              # stop and delete the cache and service volumes
```

Milvus, etcd and MinIO share the dev container's network namespace, so the
`localhost:19530` and `localhost:9000` addresses in `integration-4.0` work
unchanged. The container's architecture is the host's: on Apple Silicon it is
linux-aarch64, whose unified native build profile exists since 2026-09-21 and
was validated on a Graviton pod; a build inside the container itself has not
been run yet, so there the container may also consume a bundle built elsewhere. Rationale
and file layout: [devcontainer.html](design/engineering/devcontainer.html).

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

Follow the build constraints in [section 4 of modules.md](design/architecture/modules.md) when reviewing
or changing the build, using the repository's
[sbt skill](../.agents/skills/spark-milvus-sbt/SKILL.md). `build.sbt` holds module
wiring and publication decisions; root run, assembly and publication details
are named settings in the same file.

| File | What to change there |
|---|---|
| `project/Versions.scala` | Library versions and the Spark line matrix |
| `project/Dependencies.scala` | Dependency coordinates, scopes and dependency groups |
| `project/Modules.scala` | Shared compile/test settings, checks and Scala cross-version settings |
| `project/KnowhereBuild.scala` | Pinned upstream API compilation, the Cardinal flag taken from the bundle, and the packaged JNI smoke |
| `project/NativeBundle.scala` | Unified native resource selection, source pins, manifest and ELF validation |
| `project/plugins.sbt` | Build plugins and their meta-build dependencies |

## Tests that need the native library

Suites that call into C check for upstream `libmilvus-storage-jni` first and cancel
themselves when it is absent, so a machine without it still gets a green run
with those suites reported as canceled rather than failed:

- `StorageNativeTest`, `WriterRoundTripTest`, `SegmentWriterTest` and `SegmentReaderTakeTest` in core
- `MilvusV3PartitionWriterLifecycleTest` and `HadoopEndpointSnapshotTest` in spark-4.0

The UAT suites — `StorageNativeUatTest` and `SegmentReaderUatTest` in core,
`StorageFullChainUatTest`, `SnapshotReadUatTest`, `ConnectorWriteReadUatTest`,
the scenario suite `uat.DataFrameScenariosUatTest` and the vector search suite
`uat.VectorSearchUatTest` in spark-4.0 — cancel on their own environment
variables as well, so they stay canceled even with the library present.
`VectorSearchUatTest` additionally needs Knowhere's native libraries, which the
unified bundle carries on all four platforms, and its index cases need a snapshot whose vector field
carries a persisted index (`MILVUS_UAT_INDEXED_SNAPSHOT`). The scenario suite is also compiled into the 3.5, 4.1 and
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

The Helm execution contract for moving that suite into CI is documented in
[integration-tests.html](design/engineering/integration-tests.html). The chart
is under `deploy/helm/spark-milvus-integration`; it runs one Spark local-mode
Job against external services and references existing Secrets. The release
image and the currently hard-coded integration suites do not yet satisfy the
runner contract, so chart linting is not evidence that the live suite ran.

## Building the native library

The native libraries come from one build: the
[unified native bundle](#unified-native-bundle) below compiles milvus-storage
and Knowhere, their upstream JNI libraries and one shared dependency set into a
platform JAR. The JNI methods and `NativeLibraryLoader` belong to milvus-storage
and Knowhere; this repository compiles their Java APIs and packages their
libraries. Building requires Conan, CMake and a Rust toolchain.

Initialize the pinned native source submodules before compiling:

```bash
git submodule update --init milvus-storage knowhere
```

While the milvus-storage gitlink points at a commit that only the fork
`Thor-ChenBiao/milvus-storage` carries (decision 31, until upstream takes it),
an existing checkout needs `git submodule sync milvus-storage` after pulling a
change to `.gitmodules`.

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

### macOS

Apple silicon and Intel Macs take the same steps. The compiler has to be Apple
clang 17 or later, from Xcode or the Command Line Tools 16.3 onward: Knowhere
uses `std::atomic_ref`, which libc++ provides from LLVM 19. On an Intel Mac,
`darwin-x86_64` builds folly against the macOS 14.5 SDK that the Command Line
Tools install, so the Command Line Tools must be present even next to Xcode.

The pinned CMake 3 lives in a virtual environment of its own because a current CMake 4 rejects the
`cmake_minimum_required` of several pinned recipes, and because
`[platform_tool_requires]` in each profile names the version that must be on
`PATH`:

```bash
xcode-select --install                       # clang, otool, install_name_tool, codesign
brew install libomp ninja ccache openjdk@21 sbt   # libomp: Apple Clang ships no OpenMP runtime
curl https://sh.rustup.rs -sSf | sh          # cargo, for the storage Rust bridge
python3 -m venv ~/toolchain/cmake3venv       # not under /tmp, which the system empties
~/toolchain/cmake3venv/bin/pip install cmake==3.31.10 ninja conan==2.25.1
export PATH=~/toolchain/cmake3venv/bin:$PATH
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib   # bindgen
conan profile detect --force
conan remote add default-conan-local2 \
  https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local2
printf 'compiler:\n    apple-clang:\n        version: ["18", "19", "20", "21"]\n' \
  > ~/.conan2/settings_user.yml        # conan 2.25.1's own list stops at 17.0
make native-bundle NATIVE_JOBS=6 && make package   # storage and Knowhere
```

`NATIVE_JOBS` is the parallel compile width; the whole dependency graph is
built from source here, because the Conan remote has no macOS binary packages.
`settings_user.yml` is the only Conan configuration the build needs: the
profile names the Apple clang version it was validated with, and Conan's stock
`settings.yml` stops a few releases behind Xcode. The pinned recipes are used
unmodified, and the build's provenance records that: the boost and avro recipes
each list a working mirror after their dead first URL, and thrift 0.17 and arrow
compile unpatched against the macOS SDK's libc++.

The JVM's signal-chaining library is preloaded with `DYLD_INSERT_LIBRARIES`
(`libjsig.dylib`) where Linux uses `LD_PRELOAD` (`libjsig.so`); both the
Makefile and the root `run` settings pick the right one.

If Conan Center or crates.io are slow from your network, `[platform_tool_requires]
cmake/3.31.12` in the Conan profile (with Homebrew's CMake on the PATH and
`CMAKE_POLICY_VERSION_MINIMUM=3.5` in `[buildenv]`) skips the CMake tool
packages, and a crates.io mirror in `~/.cargo/config.toml` speeds up the Rust
bridge. The Makefile exports `CARGO_NET_GIT_FETCH_WITH_CLI=true`, so cargo's
git fetches of `lance` and `vortex` honor your `git` configuration (SSH keys,
`insteadOf` rewrites, proxies).

### The artifact is large

An earlier macOS build measured `libmilvus-storage.dylib` at 481 MB, including
162 MB of symbols, and its upstream JNI bridge at 177 KB. Those measurements are
not the size of the current candidate. Native builds include Arrow, Parquet,
cloud SDKs and the Rust bridge; the dependency closure also contains shared
libraries. Validate the complete packaged dependency set rather than assuming
fully static linkage. Section 4.2 of
[storage-access.html](design/architecture/storage-access.html) covers what the
size costs.

## Unified native bundle

The unified platform JAR contains both upstream JNI entry libraries and
one dynamically linked dependency set. The implementation and acceptance status
are documented in [native-build/README.md](../native-build/README.md); the library layers and the ways to obtain a bundle are in [build.html](design/engineering/build.html).
The native build is explicit; ordinary sbt compilation does not start Conan or
CMake. The independent project in `native-build/` defines both engines, their
upstream JNI implementations and the optional Cardinal plugins without executing
their upstream CMake files. One Conan host graph supplies shared dependencies
to one CMake/Ninja build tree.

Build both engines, package their shared dependencies, and select the result:

```bash
make native-bundle NATIVE_JOBS=50 NATIVE_BUILD_OPTIONS=--with-cardinal
make package NATIVE_BUNDLE="$PWD/target/native-build/$platform/milvus-native-$platform.jar"
```

A platform is built from source when `native-build/profiles/` holds its Conan
profile and `native-build/platforms.py` holds its adapter; all four platforms
have both. The build needs Conan 2, the CMake version the
profile's `[platform_tool_requires]` names, Ninja, ccache, the profile's
compilers, Rust, libclang, a JDK, and access to the pinned source repositories
and Conan recipes. Rust bindgen loads libclang when building the storage
bridge's `custom-labels` dependency; install `libclang-dev` on Ubuntu, and on
macOS point `LIBCLANG_PATH` at the Command Line Tools copy. Linux adds
`patchelf` and the libaio development package; macOS adds `otool`,
`install_name_tool`, `codesign` and Homebrew's `libomp`, since Apple Clang
ships no OpenMP runtime.
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

`make all`, `package` and `quick-build` build the unified bundle and select it;
adding a platform is adding its profile and its adapter, since `build.py` names
whichever of the two is missing. `NATIVE_BUNDLE` selects an existing platform
JAR and skips native compilation; otherwise `NATIVE_WORK_DIR` holds the build
and Conan reuses compatible cached packages. Any platform can consume a matching
prebuilt unified bundle, which no profile cross-compiles. A prebuilt bundle must
match the host, and `verifyNativeBundle` rejects one whose manifest names
another platform.
Docker calls the `native-resources` target, which is `native-bundle`. The build
limits concurrency to `NATIVE_JOBS` (1..50) and preserves initialized submodule
checkouts.
Docker keeps Conan packages, Cargo registry/Git downloads and ccache in locked
BuildKit cache mounts, so a later build failure does not discard completed
dependencies. Conan configuration is initialized inside the mount, including
when the cache is empty. Native work directories and Cargo compilation outputs
remain local to each build; cache reuse does not skip source or provenance checks.
Maven publication credentials enter the final build step through the optional
BuildKit secret named `maven_credentials`; they are not build arguments, copied
files, image layers or cache-mount contents. A publishing build must set
`MAVEN_CREDENTIALS_FILE=/run/secrets/maven_credentials` (the Docker default)
and provide that secret. A non-publishing build provides no secret.
This does not establish joint Storage/Knowhere support on platforms
without a validated unified bundle.

Select the resulting resource-only JAR and its `.properties` checksum sidecar:

```bash
sbt -Dmilvus.native.bundle=/absolute/path/to/milvus-native-$platform.jar \
  native-runtime/verifyNativeBundle native-vector/knowhereSmoke
sbt -Dmilvus.native.bundle=/absolute/path/to/milvus-native-$platform.jar test assembly
```

Before native tests, preload the selected JRE's `lib/libjsig.so`
(`lib/libjsig.dylib` through `DYLD_INSERT_LIBRARIES` on macOS) as described
below. The resulting root assembly contains the bundle resources. At runtime,
one class loader extracts and verifies them in one private directory; both
upstream JNI loaders use that directory. The external `.properties` checksum
sidecar is a build input and is not needed next to the deployed assembly.

Bundle-selected functional test JVMs set `LD_BIND_NOW=1` and an empty external
library path (`LD_LIBRARY_PATH`, and `DYLD_LIBRARY_PATH` with
`DYLD_FALLBACK_LIBRARY_PATH` on macOS, where dyld has no equivalent of
`LD_BIND_NOW`). A JVM may still request lazy binding when loading a native
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

The unified bundle is the only source of native libraries in the build. A
malformed bundle fails the build. A `native/` directory under
`native-storage/src/main/resources`, left by the storage-only build this
repository no longer has, fails the build until it is deleted, so no second copy
of the storage libraries reaches the classpath. An explicit
`knowhere.native.path` pointing elsewhere is rejected at runtime. The connector
sets this property only during Knowhere initialization under a JVM-shared lock,
then restores its prior value on success or failure. Multiple isolated copies of
the Connector's JNI bindings in one JVM remain unsupported.

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
superproject gitlink fixes the exact commit used by every build. After pulling
a change of the submodule URL, run `git submodule sync knowhere` before
`git submodule update`. `native-vector`
compiles `knowhere/java/src/main/java` with `javac --release 11`; it does not
implement another C API or JNI bridge. The API JAR is cached under
`native-vector/target/knowhere`. An ordinary compile or unit-test run needs a
JDK and an initialized submodule, but does not download source or compile or
load the native engine.

Cardinal comes from the same build: `make native-bundle
NATIVE_BUILD_OPTIONS=--with-cardinal` compiles both pinned Cardinal revisions
into the bundle, which records `with_cardinal=true`. The Cardinal sources use
`-march=native`, so a Cardinal bundle needs a host with the build machine's
instruction set. The connector derives
`META-INF/milvus/knowhere-runtime.properties` from the selected bundle's
`with_cardinal`; `NativeVectorLibrary.RuntimeInfo.cardinalSupported()` exposes
that feature, and an arbitrary `knowhere.native.path` override does not
establish Cardinal support.

The loader reads
Milvus binlog/Parquet payloads and optional `SLICE_META`, or a Cardinal raw
`_mem.index.bin` stream. Payload markers select the matching Faiss or Cardinal
engine; a shared HNSW name alone does not establish format compatibility.
Each task owns and closes its index. There is no cross-task index cache.
The historical Knowhere `9dc2b8ad` JNI migration and corrected native dependency
build passed real UAT Cardinal HNSW queries against direct JNI results and an
independent 100,000-row reference. Actual storage entry-library relocation and
both native load orders also passed for that artifact. The complete
validation record, remaining third-party JNI diagnostics and packaging limitations are in
[the storage I/O validation state](design/architecture/storage-io.html#state).
Local OSS fixtures do not replace real-instance-data validation. Tests live in
the external `milvus-spark-demo` validation project.

`MilvusSearch.search` takes a query set and returns each query's global TopK,
running `core.index.SegmentSearch` over the segment sets `SearchPlan` cut.
Missing native libraries fail the query. Planning, packing and merge tests run
in the ordinary suite. The explicit real-native check needs the selected
unified bundle and the JRE's `libjsig`:

```bash
sbt -java-home "$JAVA_HOME" -Dmilvus.native.bundle=/absolute/path/to/milvus-native-$platform.jar \
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

For HotSpot, preload the **running JRE's** `lib/libjsig.so` before JVM startup,
through `LD_PRELOAD` on Linux and `DYLD_INSERT_LIBRARIES` with `lib/libjsig.dylib`
on macOS.
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
