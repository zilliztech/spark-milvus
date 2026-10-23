# Working in this repository

[README.md](../README.md) covers the V3 read/search entry and environment.
This guide describes contributor builds, tests and native artifacts. Service
preparation, writing, backfill, backup and legacy-format compatibility are out
of scope; callers supply existing V3 inputs.

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
scripts/devcontainer.sh up --services      # optional local services; public reads only need object storage
scripts/devcontainer.sh run --env-file my.env sbt spark/test   # environment variables from a file
scripts/devcontainer.sh down               # stop everything; caches survive
scripts/devcontainer.sh clean              # stop and delete the cache and service volumes
```

The optional services share the dev container's network namespace. Public
integration uses fixed V3 data in an isolated object store, as described below.
The container uses the host architecture and can consume a matching native
bundle built elsewhere.

## Project ids and version selection

The five production project IDs are `native-runtime`, `native-storage`,
`native-vector`, `core` and `spark`. The layer-1 projects live under `native/`
as `native/runtime`, `native/storage` and `native/vector`; `core` and `spark`
match their directories. The root only aggregates these projects. `integration` is separate because it needs a real object store.

`-DsparkVersion` selects a supported line or its pinned compile version;
`-DscalaVersion` selects the full Scala version. Defaults are 4.0 / 2.13.16;
3.5 also supports 2.12.20. Unsupported combinations fail while loading the
build. `Versions` is the authoritative matrix.

```bash
sbt core/test spark/compile
sbt -DsparkVersion=4.2 -DscalaVersion=2.13.16 'clean; integration/clean; compile; test'
sbt -DsparkVersion=3.5 -DscalaVersion=2.12.20 'clean; integration/clean; compile; test'
```

Restart sbt and clean when switching combinations; the ordinary `target` and
`crossTarget` directories are retained. Do not change the selected line with
interactive `++`. CI uses independent invocations and cleans all module outputs,
including integration; its evidence is kept outside those module targets.

`spark/src/main/scala` and `spark/src/test/scala` contain shared sources.
`scala-shims` contains only explicit version-dependent roots; `Modules` selects
the matching roots for compilation and source JARs. Formatting and package-path
checks cover all retained main/test roots, including unselected shims.

Use `spark/assembly` with a selected native bundle for a local fat JAR. All
lines use the same merge/shading rules and leave Spark-supplied Arrow/Jackson
modules out. Published candidates use `spark/releaseJvmArtifacts` and, for 4.0,
`spark/releaseThinArtifacts`; root `assembly` and direct root publication have
been retired. See [the release guide](releasing.md) for supported release lines
and final artifact validation.

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

Follow the build constraints when reviewing or changing the build, using the
repository's
[sbt skill](../.agents/skills/spark-milvus-sbt/SKILL.md). `build.sbt` holds module
wiring, source selection and candidate exports; assembly settings are named
blocks in the same file.

| File | What to change there |
|---|---|
| `project/Versions.scala` | Library versions and the Spark line matrix |
| `project/Dependencies.scala` | Dependency coordinates, scopes and dependency groups |
| `project/Modules.scala` | Shared compile/test settings, checks and Scala cross-version settings |
| `project/KnowhereBuild.scala` | Pinned upstream API compilation and the packaged JNI smoke |
| `project/NativeBundle.scala` | Unified native resource selection, source pins, manifest, provenance and JVM load validation |
| `project/ReleaseArtifacts.scala` | JVM release classpath, sources, API documentation and artifact metadata |
| `project/plugins.sbt` | Build plugins and their meta-build dependencies |

## Fixed inputs for read tests

Public read tests consume SHA256-checked files under
`core/src/test/resources/read-fixtures` and the native-vector test resources.
Existing files in `core/src/test/data` also have a checksum manifest; their
original locations stay stable.
`ReadFixtures.copyTo` verifies and copies a named group; it never regenerates a
missing or corrupt sample. Run `python3 scripts/ci/check_read_fixtures.py` to
verify files and all source package paths, including every Spark shim
directory.
The same check runs in public CI, without the private knowledge submodule.

Each sample group records its origin and expected results. Changes to binary
samples require deliberate regeneration and review of expected results and
hashes. V3 and Knowhere groups record their producer revisions and native
bundle. Ordinary tests and CI only verify and copy committed inputs.

## Tests that need the native library

Tests that require upstream Storage or Knowhere JNI cancel when the selected
bundle is unavailable. This does not count as successful native validation.
Core segment/Arrow read suites and Spark `nearestby`, `V3ReadContractTest` and
`ConnectReadSessionTest` use frozen V3 inputs. Connect runs on the 4.x lines,
with the client isolated from the classic Spark classpath. Environment-driven
UAT reader suites additionally require their named input options.

Spark 3.5 uses Arrow 12, whose C Data allocation needs a Java 17 test JVM.
Set `SPARK35_TEST_JAVA_HOME` to that JDK while running sbt itself on Java 21.
`ArrowReadOwnershipTest` requires Arrow's C Data JNI but no Milvus native bundle.
The release/native CI explicitly requires each named suite to execute and
rejects canceled, ignored, pending, failed or missing cases.

Report each run's actual successful, failed and canceled counts and completed
or aborted suites. Name missing native/UAT prerequisites; a green ordinary
unit run with cancellations does not establish native compatibility.

`integration` is outside the root aggregate. Its `MilvusDataSourceTest` reads
committed `collections/nearest` V3 files from a real object store and checks
filtering, projection, deletes, repeated actions, task retries and two explicit
identities in the same bucket. Each identity must fail to read the other
identity's snapshot or segment files. No Milvus server is needed.

```bash
python3 scripts/ci/read_storage.py start --directory /tmp/milvus-read-fixture
export MILVUS_READ_IT_OPTIONS=/tmp/milvus-read-fixture/a.json
export MILVUS_READ_IT_OPTIONS_B=/tmp/milvus-read-fixture/b.json
sbt -DsparkVersion=4.0 -Dmilvus.native.bundle=/absolute/path/to/milvus-native-linux-x86_64.jar \
  'integration/testOnly *MilvusDataSourceTest'
python3 scripts/ci/read_storage.py stop --directory /tmp/milvus-read-fixture
```

Always run `stop`, including after failures. The helper removes only its owned
container and credential files. A containerized workspace can pass
`--network-container <workspace-id>` to bind the store to its loopback.
Existing object stores can supply equivalent fixed files and two read policies.
Remote keys are bucket-relative; `fs.root_path` does not prepend a prefix.
Keep credentials out of Git and test artifacts.

The [integration chart](../deploy/helm/spark-milvus-integration/README.md)
accepts `readOptions.existingSecret` with `a.json` and `b.json`. The test image
must supply the runner; chart linting alone does not prove that a job ran.
The earlier service-management chart is maintained under private `deploy/helm`;
public values reject service setup and write settings.

### Public read CI

`exportBuildMatrix` derives five Spark/Scala source combinations from `Versions`.
Each CI invocation selects a line and Scala version, validates source JAR/POM
contents and runs source/spill checks. Shared modules run with their own
configuration. The default root unit run tests Spark 4.0; other lines receive
the matrix checks. Push/manual/full-validation CI adds mandatory native
read/search/Arrow/Connect suites and real object-storage reads on Linux x86_64.
The eight independent release consumer jobs cover Spark 4.0/4.2 on the two Linux
architectures and two supported OS versions.

Candidate validation rejects the Milvus SDK and non-release dependencies.
Native engine bindings retain upstream APIs needed by downstream libraries;
the final boundary applies to connector capabilities. Source matrix coverage
does not expand the published release matrix.

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

The milvus-storage gitlink points at official commit `5694520`, including
PR #714's Tencent OIDC role chain. After pulling a repository URL or pin
change, update an existing checkout with:

```bash
git submodule sync milvus-storage
git submodule update --init milvus-storage
```

Rebuild the unified native bundle for the new gitlink. Its manifest and
provenance must record the new storage revision; a bundle from the former pin
is rejected. The unified build includes Talon's C++ sources, Rust feature and
CXX bridge as required by this upstream version. That does not enable Talon
read routing at runtime: `fs.talon.mode` still defaults to `0` (disabled).

The `native-storage` sbt module compiles `milvus-storage/java/src/main` for
Scala 2.12 and 2.13 instead of importing upstream's sbt project or a Scala 2.13
binary JAR. `Compile / sourceGenerators` copies the upstream Java/Scala sources
to `sourceManaged` with `Sync`; Connector formatting operates on its own
sources and does not rewrite the submodule. Arrow is `provided`, with the Spark
4.0 line as the API compilation baseline and each Spark line supplying runtime
Arrow. Source compilation does not start a native build.

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
profiles name Apple clang 17, which Xcode 16.4 and the Xcode 26 releases report
and which the `macos-15` release runner uses, and Conan's stock `settings.yml`
stops a few releases behind Xcode, so the file matters once a newer Xcode
reports a later version. The release build (`scripts/ci/prepare_native.sh`)
also exports `MACOSX_DEPLOYMENT_TARGET=14.0`; a local build takes the host's
default target unless that variable is set. The pinned recipes are used
unmodified, and the build's provenance records that: the boost and avro recipes
each list a working mirror after their dead first URL, and thrift 0.17 and arrow
compile unpatched against the macOS SDK's libc++.

The JVM's signal-chaining library is preloaded with `DYLD_INSERT_LIBRARIES`
(`libjsig.dylib`) where Linux uses `LD_PRELOAD` (`libjsig.so`); both the
Makefile and the forked test settings pick the right one.

If Conan Center or crates.io are slow from your network, `[platform_tool_requires]
cmake/3.31.12` in the Conan profile (with Homebrew's CMake on the PATH and
`CMAKE_POLICY_VERSION_MINIMUM=3.5` in `[buildenv]`) skips the CMake tool
packages, and a crates.io mirror in `~/.cargo/config.toml` speeds up the Rust
bridge. The Makefile exports `CARGO_NET_GIT_FETCH_WITH_CLI=true`, so cargo's
git fetches of `lance` and `vortex` honor your `git` configuration (SSH keys,
`insteadOf` rewrites, proxies).

### The artifact is large

Native builds include Arrow, Parquet, cloud SDKs and the Rust bridge, and the
dependency closure also contains shared libraries; the storage library alone is
several hundred megabytes. Validate the complete packaged dependency set rather
than assuming fully static linkage.

## Unified native bundle

The unified platform JAR contains both upstream JNI entry libraries and
one dynamically linked dependency set. The implementation and acceptance status
are documented in [native/build/README.md](../native/build/README.md).
The native build is explicit; ordinary sbt compilation does not start Conan or
CMake. The independent project in `native/build/` defines both engines, their
upstream JNI implementations and the optional Cardinal plugins without executing
their upstream CMake files. One Conan host graph supplies shared dependencies
to one CMake/Ninja build tree.

Build both engines, package their shared dependencies, and select the result:

```bash
make native-bundle NATIVE_JOBS=50
make package NATIVE_BUNDLE="$PWD/target/native-build/$platform/milvus-native-$platform.jar"
```

A platform is built from source when `native/build/profiles/` holds its Conan
profile and `native/build/platforms.py` holds its adapter; all four platforms
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
`native/build/dependencies.json`; the repository does not export replacement
recipes or rewrite cached recipes. Integration-specific link relationships live
in `native/build/cmake/Storage.cmake`, `Knowhere.cmake` and `Cardinal.cmake`.
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
checker in `native/build/jvm_load.py`; a matching JDK with `libjsig` is required.
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
[native/build/README.md](../native/build/README.md) for the directory layout and
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
sbt -Dmilvus.native.bundle=/absolute/path/to/milvus-native-$platform.jar test spark/assembly
```

Before native tests, preload the selected JRE's `lib/libjsig.so`
(`lib/libjsig.dylib` through `DYLD_INSERT_LIBRARIES` on macOS) as described
below. The selected `spark/assembly` contains the bundle resources. At runtime,
one class loader extracts and verifies them in one private directory; both
upstream JNI loaders use that directory. The external `.properties` checksum
sidecar is a build input and is not needed next to the deployed assembly.
Setting `spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin`
moves that extraction (about 3 s per executor) to executor start; a failed
preload is logged and the first task loads as before.

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
`native/storage/src/main/resources`, left by the storage-only build this
repository no longer has, fails the build until it is deleted, so no second copy
of the storage libraries reaches the classpath. An explicit
`knowhere.native.path` pointing elsewhere is rejected at runtime. The connector
sets this property only during Knowhere initialization under a JVM-shared lock,
then restores its prior value on success or failure. Multiple isolated copies of
the Connector in one JVM, its JNI bindings among them, remain unsupported. Local
mode with `spark.executor.userClassPathFirst=true` makes a second one: the
executor's class loader takes the jar again beside the driver's, the native
loader finds two manifests and stops with an error that names them and the
setting. Leave that setting unset in local mode.

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
The plugin's general `deduplicate` strategy buffers whole entries, which a
storage library of several hundred megabytes does not fit in the assembly heap;
the streaming strategy does. With sbt-assembly 2.1.1, a custom merge
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

The root `knowhere` submodule follows upstream `main`, including merged PR #1829; the
superproject gitlink fixes the exact commit used by every build. After pulling
a change of the submodule URL, run `git submodule sync knowhere` before
`git submodule update`. `native-vector`
compiles `knowhere/java/src/main/java` with `javac --release 11`; it does not
implement another C API or JNI bridge. The API JAR is cached under
`native/vector/target/knowhere`. An ordinary compile or unit-test run needs a
JDK and an initialized submodule, but does not download source or compile or
load the native engine.

Public release bundles use open-source Knowhere and disable Cardinal. Private
Cardinal builds use their pinned historical native build tools, pin the plugin revisions and record
`with_cardinal=true`; they are outside public release acceptance. The JVM
compatibility resource records Storage/Knowhere pins and rejects mismatches
before JNI initialization.

Persisted-index readers consume Milvus index payloads and `SLICE_META`; each
task owns and closes its index. Native loading and format compatibility are
separate checks. NEAREST BY executes `SegmentSearch` over the segment sets from
`SearchPlan`, with global TopK merging. Missing libraries fail the query.

The [release consumer](releasing.md#consumer-gates-and-ci-triggers) compiles
`ReadOnlyConsumerSmoke` and `ReadFixtures` independently, embeds checked V3 and
index samples, and loads production classes from the candidate through real
`spark-submit --packages`. `milvus.smoke.master=local-cluster` requests an
independent executor JVM. It verifies native reads/search, not Milvus service
registration. Write/index-build/snapshot-output workloads are private.

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
`native/vector/target/knowhere-smoke/jni.log`.

`stage.py` fails staging when a required source license document is missing;
collected licenses land in `bundle/licenses/` and
`META-INF/milvus-native/licenses/`.

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
restrictions. The Spark formatter includes all common and shim roots even when
only one combination is compiled. Do not run multiple formatter processes
against one checkout. Review the diff for missing code; an empty Scala file
can pass `scalafmtCheckAll`.

`integration` is outside root's aggregate. When changing it, also run:

```bash
sbt integration/scalafmtAll integration/scalafmtCheckAll
```

When changing `build.sbt` or Scala/sbt files under `project/`, also run
`sbt scalafmtSbt scalafmtSbtCheck`. CI runs the root `scalafmtCheckAll` and
`scalafmtSbtCheck`; `integration/scalafmtCheckAll` remains a local check.
Use the pinned `.scalafmt.conf`; do not
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
The root run does not include `integration/test`; its V3 read suite requires a
real object store. Additional Scala
cross-version checks, integration tests, native
builds and publication follow the task's scope and authorization.

CI runs `python3 -m unittest discover -s native/build -p 'test_*.py'` plus the
same discovery over `scripts/tests`; run them when touching `native/build/`.
Python examples live in `demo/python`, Java examples in
`demo/java/src/main/java/com/zilliz/spark/connector/demo`, and helper/launcher
tests in `demo/tests`. After changing a demo, run
`python3 -m unittest discover -s demo/tests -p 'test_*.py'` and the affected
examples in both languages with the runtime described in
[demo/README.md](../demo/README.md), such as
`./demo/run.sh python 04_exact_search.py` and `./demo/run.sh java ExactSearch`.
The launcher compiles Java sources using the existing Spark and connector JARs.
Snapshot-path helpers use the describe REST API independently of Spark sessions;
exercise their request/response checks without requiring a production service.

## Storage protobufs

Only `common.proto` and `schema.proto` are generated into `core`, with
`grpc = false`, because snapshots embed the protobuf `CollectionSchema`.
Public builds generate no Milvus service stubs.

## Jackson

Layer 2 pins jackson-databind, jackson-core and jackson-annotations to one
version, because parquet-hadoop drags in a databind newer than
jackson-module-scala and mixing them throws `JsonMappingException` at runtime.

Do not apply that pin to the Spark lines. Spark ships a self-consistent jackson
set, and pinning there drags databind *below* Spark's own jackson-module-scala,
which throws the same way from the other direction.

## Commits

Write the message in English, explain why rather than what, and name the
verification you actually ran. When a change contradicts a document in this
repository, say so in the message and fix the document in the same commit.

## Documents

Public documents are English Markdown files in `docs/`, including this
guide, [reference-en.md](reference-en.md), and the
[release guide](releasing.md). They describe the connector, its use, build and
release process. Contributions may include design proposals, decisions and the
rationale needed to review a change in the same pull request.

### Repository-local document references

Repository documents must not cite documents hosted only by a developer's local
web server. When such a document is needed, copy its content and required
images, data and other assets into the repository that owns the documentation,
then use relative links to that copy. Reuse an existing repository copy when
available. Record the source title, date and version without retaining the
local web URL as a reference.

The copied document and its links must remain usable from a checkout without
the original machine or web server. Paths outside the repository are not a
substitute for copying the document. Keep public and private material in their
respective repositories. Links to official documentation and other shared
external sources remain valid references.
