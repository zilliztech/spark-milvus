# Unified native build

`scripts/build-native.sh` builds the pinned milvus-storage and Knowhere sources,
including their upstream JNI implementations, with this directory's CMake
project. `cmake/Storage.cmake`, `cmake/Knowhere.cmake` and `cmake/Cardinal.cmake`
define the targets; the build does not execute the engines' or Cardinal's
upstream CMake files. A configuration trace checks that boundary.

Both engines use one Conan host dependency graph. `dependencies.json` pins
upstream provenance and selects the newer version when their requirements
conflict. Every direct package reference includes its exact upstream recipe
revision. The build consumes those recipes without patching or exporting a
project-owned replacement. Integration-specific link relationships are declared
on the consuming targets in this project's CMake files. The build does not
select an unpinned latest release.
`profiles/linux-x86_64` selects GCC 12, C++20 and shared host dependencies.
OpenBLAS uses `dynamic_arch=True`. Header-only dependencies and the internal
Rust bridge do not expose a shared-library option.

A platform is built when `native-build/profiles/` holds its Conan profile and
`platforms.py` holds its adapter. `build.py` names the missing one rather than
refusing an operating system.

Every platform requires Conan 2, a CMake 3 whose version the profile's
`[platform_tool_requires]` declares, Ninja, Python 3.9+, Git, a JDK selected by
`JAVA_HOME`, Rust/Cargo, libclang and ccache. Rust bindgen loads libclang while
building the storage bridge's `custom-labels` dependency. Conan remotes must
provide each pinned upstream recipe that is absent from the local cache. A
CMake 4 rejects the `cmake_minimum_required` of several pinned upstream
recipes, so the declared version is the one that must be on `PATH`.

No package manager ships that CMake next to a current one, so put it in a
virtual environment of its own and prepend that to `PATH` for the build. It is
the machine's, not the checkout's: a directory under `/tmp` is emptied by the
system and takes the toolchain with it.

```bash
python3 -m venv ~/toolchain/cmake3venv
~/toolchain/cmake3venv/bin/pip install cmake==3.31.10 ninja
PATH=~/toolchain/cmake3venv/bin:$PATH scripts/build-native.sh --work-dir ...
```

| Platform | Compilers | Binary tools | Also |
| --- | --- | --- | --- |
| `linux-x86_64`, `linux-aarch64` | GCC/G++/gfortran 12 | patchelf, readelf, ldd, binutils | the libaio development package, which Folly's async I/O needs; Ubuntu supplies libclang in `libclang-dev` |
| `darwin-aarch64` | Apple Clang, a separate OpenMP runtime, gfortran | otool, install_name_tool, codesign | no libaio, liburing or OpenBLAS: `dependencies.json` scopes those to Linux, as the engines' own conanfiles do |

The build records tool versions and never reads a native library from an older
connector JAR.

```bash
scripts/build-native.sh \
  --work-dir /absolute/path/to/new-build \
  --cardinal-repository /absolute/path/to/authorized/cardinal-checkout \
  --with-cardinal --jobs 50
```

Omit `--with-cardinal` for the open-source variant. The Knowhere revision comes
from the root `knowhere` submodule gitlink; Cardinal revisions come from
`dependencies.json`. Cardinal sources require authorization. The build uses the
initialized Knowhere submodule as a Git object source and creates an isolated
checkout at the gitlink revision. `--knowhere-source` may name another Git
object source only when its HEAD is the same revision. Storage is copied from
the selected submodule working tree and its complete source manifest and diff
are recorded. An exported storage tree must
specify `--storage-revision`; `--storage-patch` preserves its Git diff.

`--cargo-cache` copies an existing Cargo target directory; Cargo determines
which artifacts remain valid. `--corrosion-source` selects a clean checkout of
the exact Corrosion revision in `dependencies.json`, used to generate and build
the Rust/CXX bridge. The driver snapshots and verifies it before CMake uses it;
the recorded tool versions include Rust and Cargo.
`--no-remote` resolves Conan recipes and binary packages only from the cache;
recipe source downloads still run. These options do not select prebuilt engine libraries.

The driver records source identities, locks the dependency graph before
installing packages, and rejects a resumed build if its source tree or
dependency inputs changed. Use a new work directory for changed inputs. Conan
can still reuse packages from the previous build. `--conan-lock /path/conan.lock`
reuses a reviewed full dependency lock in a new work directory. Its direct
recipes must match the current selected revisions, and every resolved host and
build dependency must remain in that lock. Without this argument, the first run
resolves and writes `provenance/conan.lock`; use that lock for later builds to
preserve transitive versions as well as direct requirements. Jobs must be between 1 and
50. Both engines share one `cmake-build/` Ninja tree. The driver first builds
`milvus-storage-rust` with one Ninja job and the requested Cargo job count, then
builds the C++ targets with the requested Ninja job count. Any Cargo recheck in
that second phase uses one Cargo job, so independent Rust and C++ schedulers
cannot each start 50 compilation jobs at the same time.

Conan reuses compatible cached binaries when the recipe revision, configuration
and package ID all match. `build.py` copies `conanfile.py` and
`dependencies.json` into the work directory, checks the dependency declarations
from both pinned engine revisions, and rejects a selected conflict version unless
it is the newer upstream requirement. Conan then resolves only the exact recipe
references in `dependencies.json`; the full lock covers every host and build
dependency. The driver writes the host and build profiles it passes to Conan
into the work directory: the checked-in `profiles/linux-x86_64` plus a
`[replace_requires]` section naming every reference in `dependencies.json`,
without the `[options]` section for the build profile. The consumer's
`force=True` requirements pin the host context only; without the replacements,
build-context tools such as protoc resolve their zlib and openssl ranges to the
newest remote revision, and the lock's context-free overrides then rewrite those
ranges to host revisions that its `build_requires` does not contain, so
`conan install --lockfile` fails on a fresh cache.

The independent CMake targets state the additional integration link edges that
the unified consumers need. For example, storage links its public dependency
set and keeps `aio` in the runtime closure explicitly, while Knowhere, DiskANN
and Cardinal name their direct shared-library providers. These declarations do
not modify Conan cache entries or dependency source trees. Provenance records
the direct references, the two upstream dependency declarations, the selected
versions, the complete lock and the resolved package graph.

The general and concurrency C API tests compile the pinned Knowhere test
sources unchanged. The project-owned DiskANN acceptance fixture under
`tests/knowhere/` requests unquantized refinement so its exact-distance
assertions test that contract in both engine variants. The build never modifies
the pinned Knowhere checkout, and the fixture changes no production default.

The Rust bridge is linked privately into the storage engine. Public C++ headers
retain their generated bridge include directories without propagating the
static archives into JNI consumers. On Linux JNI builds, a linker map hides
vendored LZ4, XXHash, Zstandard and prefixed AWS-LC symbols while preserving the
public `loon_*` C API and C++/CXX runtime symbols. These rules live in our CMake
targets and `cmake/storage-private-symbols.map`; they do not require changes to
storage's upstream build files. This build does not produce Python bindings.
Rust `openssl-sys` uses the same Conan OpenSSL headers and shared libraries
through `OPENSSL_DIR`, `OPENSSL_STATIC=0` and `OPENSSL_NO_VENDOR=1`. Cargo's pinned
internal compression implementations are not replaced with a different version.

Upstream Cardinal uses host-native CPU compiler flags. This local validation
build can require the builder's instruction set; the `linux-x86_64` classifier
does not promise compatibility with every x86 CPU. The provenance records the
host CPU, the compiler's native target and actual compile/link flags.

The resulting files are:

| Path below the work directory | Contents |
| --- | --- |
| `status.json` | Current phase and failure details |
| `sources/` | Isolated engine source snapshots |
| `dependency-input/`, `dependencies/` | Pinned direct requirements and Conan-generated CMake dependencies |
| `provenance/` | Source identities, dependency selection, tool versions, lock, graph, compiler commands and full configuration trace |
| `provenance/build-input/` | Full backup of this build implementation and its inputs |
| `cmake-build/` | One CMake/Ninja tree for the Rust bridge, both engines, JNI and C API tests |
| `install/lib/` | Installed engine, JNI and optional Cardinal libraries before dependency staging |
| `bundle/lib/` | Runtime dependency closure with SONAME aliases and relative RPATH |
| `bundle-candidates/` | Unpromoted staging attempts, including failed audit evidence |
| `bundle-history/` | Previous successful bundles retained when a new candidate is promoted |
| `bundle/provenance.json` | Normalized source and package identities, features and library hashes |
| `bundle/provenance/` | Digests and normalized summaries that associate the JAR with external build evidence |
| `bundle/licenses/` | Collected upstream licenses |
| `bundle/audit/` | Per-library relocation and fresh loader diagnostics |

The resource JAR's provenance includes only normalized source and package
identities, including the exact direct recipe references, and digests of the
native-build inputs, dependency lock and graph, libraries, audit results and
external evidence. Collected licenses are packaged alongside it. The JAR does
not include build-machine absolute paths. Full source snapshots, the complete
Conan graph, `compile_commands.json`, native build commands, the build-input
backup and the expanded `cmake-trace.jsonl` remain in the external work
directory (`NATIVE_WORK_DIR` when Make invokes the build). Their recorded hashes
associate that external evidence with the delivered libraries.

Staging rejects conflicting SONAMEs and any dependency outside the selected
graph, apart from an explicit list of compiler runtimes and libaio. The latter
are copied with their source hashes and package origin recorded. glibc, the
platform C++ runtime and `libz.so.1` stay system dependencies. Some JDKs load
system zlib before JNI initialization, so packaging another implementation
cannot determine which zlib symbols the process uses. The bundle records its
consumers' required ZLIB symbol versions and the validation host's provider
path, package version and SHA; target machines must satisfy that ABI.
Only staged copies receive
`$ORIGIN` RPATH; inputs and cached Conan packages remain unchanged.

Native acceptance uses two fresh JVMs that call `System.load` on both JNI
entries, in storage-first and Knowhere-first order. Staging, packaging and sbt
share `jvm_load.py` and `NativeLoadCheck.java`. The checker uses the selected
JRE's `libjsig`, clears additional library paths and JVM injection options, and
rejects either failed load order. Provenance records `auditPolicy: jvm-load` and
both `jvmLoadTests`; an older standalone audit is not JVM loading evidence.

Per-library GNU stack, relocation, private-symbol and `RTLD_NOW` results remain
diagnostics. Their failures are recorded unchanged and do not block packaging.
JNI loading does not establish that JNI calls or Spark queries work; functional
storage and vector tests and real UAT queries remain separate acceptance steps.
Every run stages in a fresh candidate directory. Only a candidate whose C API
tests and JVM loading passed replaces `bundle/`; the old bundle is retained in
`bundle-history/`. A failed retry preserves the last successful bundle. A
build-directory lock prevents two builders from changing the same work tree.
The engine and storage JNI diagnostics also record exported private LZ4, XXHash,
Zstandard, OpenSSL and AWS-LC symbols from the Rust archive.

Package the validated directory with `scripts/package-native.py`. Packaging
requires exact agreement with the audited provenance's files, hashes, aliases,
ELF dependencies and Cardinal features, and successful required C API tests.
This build
does not install resources into the connector or modify compatibility records.
Run its focused regression tests with:

```bash
python3 -m unittest discover -s native-build -p 'test_*.py' -v
```
