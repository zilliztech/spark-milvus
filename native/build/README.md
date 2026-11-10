# Storage native build

`scripts/build-native.sh` builds the pinned milvus-storage source and its
upstream JNI implementation with this directory's CMake project. The public
bundle contains the Storage component only. `cmake/Storage.cmake` defines its
targets; a configuration trace rejects executing the engine's upstream CMake
files. Optional native components can extend this build through the descriptor
contract below, so consumers reuse the Storage implementation and resolve one
shared dependency graph. The driver owns Storage source, licenses, build settings
and dependency options. Extensions may add component-owned inputs and reviewed
shared dependency versions; they cannot replace Storage-owned inputs. Native
acceptance requires dependency relocations (or declared parent-plugin resolution)
and fresh JVM loads; diagnostic failures block bundle promotion.

The Storage pin includes Tencent's OIDC assume-role chain. Its source graph
requires Talon's C++ filesystem, Rust feature and generated CXX bridge; all
three are compiled and provenance records `storage.talon=true`. Talon routing
keeps the upstream default `fs.talon.mode=0`. Storage's C++ CRT transport remains
disabled (`storage.crt=false`), excluding its optional `aws-c-s3` requirement.
Changing the Storage gitlink requires a fresh native bundle.

## Toolchain and dependency graph

`dependencies.json` pins exact upstream recipe revisions. Its
`transitive_references` records Folly's reviewed double-conversion, fast_float
and libevent recipes and the AWS CRT recipe family validated with the
pinned Milvus AWS SDK recipe. Conan Center can publish a different recipe for the
same version with incompatible dependency requirements; an empty cache must
resolve the same reviewed family as an existing cache. The driver supplies these
revisions through a partial seed lock, without adding direct dependencies or
forcing different upstream versions. The resulting complete lock must retain
every selected recipe revision. The build consumes
those recipes without patching them or exporting project-owned replacements.
`profiles/` and `platforms.py` supply the Conan profile and platform adapter;
a platform needs both. Linux profiles select GCC 12 and C++20. Darwin profiles
select Apple Clang and C++20, with `llvm-openmp` from the same dependency graph.
The Darwin x86 profile uses the macOS 14.5 SDK for Folly's affected math sources.
The release matrix determines which of these platforms are distributed and
which consumer operating systems validate their artifacts.

Every platform needs Conan 2, the CMake 3 version declared by its profile,
Ninja, Python 3.9+, Git, a JDK selected by `JAVA_HOME`, Rust/Cargo, libclang and
ccache. [toolchain.json](toolchain.json) pins the Rust and Java versions used by
CI. Conan remotes must supply pinned recipes missing from the local cache.
CMake 4 rejects several pinned upstream recipes, so put the profile's CMake 3
on `PATH`. Conan's settings must also accept the compiler version in the
selected profile.

| Platform | Additional tools and dependencies |
| --- | --- |
| Linux x86_64 and aarch64 | GCC/G++/gfortran 12, patchelf, readelf, ldd, binutils and the libaio development package |
| Darwin x86_64 and aarch64 | Apple Clang from the selected profile, otool, install_name_tool and codesign; no Fortran or Linux I/O libraries |

Storage's Linux dependency graph includes OpenBLAS through milvus-common.
OpenBLAS uses `dynamic_arch=True`, with common-code targets `NEHALEM` on x86
and `ARMV8` on ARM to prevent build-host AVX512 flags entering common kernels.
Its compiled thread capacity is 64; the package-scoped CMake configuration
participates in Conan's package ID. Runtime thread settings still control
concurrency. Folly's recipe removes its `FindLibAIO.cmake`; Linux profiles provide
`LIBAIO_LIBRARIES=aio` to Folly's own CMake build and include that setting in
its package ID. Folly must carry its own `libaio` dependency instead of relying
on another library to make its unresolved AIO calls loadable.
Linux milvus-common also links its own atomic operations through the upstream
`COMMON_LINKER_LIBS` input. On Linux, for the exact pinned gRPC and OpenTelemetry recipes,
`cmake/DependencyLinkage.cmake` supplies missing target dependencies after their
CMake targets exist: OpenTelemetry's gRPC proto library links the Conan gRPC
wrapper, and the generated upb descriptor table belongs to the shared lower
provider used by both JSON/textformat and gRPC. The hook rejects unreviewed
recipe/source shapes. Both Conan contexts include its content hash in package
identity; incidental build paths do not decide cached binary identity.
Header-only dependencies and the internal Rust bridge do not
expose shared-library options.

## Build and resume

Run from the repository root:

```bash
scripts/build-native.sh \
  --work-dir /absolute/path/to/storage-build \
  --jobs 8
```

Storage is copied from the selected submodule working tree. Its complete source
manifest and Git diff are recorded. An exported source tree requires
`--storage-source`, `--storage-revision` and, when applicable, `--storage-patch`.
The driver never reads native libraries from an older connector JAR.

`--cargo-cache` copies a Cargo target directory; Cargo decides which artifacts
remain valid. `--corrosion-source` must identify a clean checkout of the exact
Corrosion revision in `dependencies.json`. Without it, the driver fetches that
revision. Both sources are snapshotted and verified before CMake uses them.
`--no-remote` restricts Conan recipe and package resolution to its cache;
recipe source downloads can still run.

Use a new work directory when sources or dependency inputs change. Resuming a
work directory checks its source manifests, build implementation and dependency
inputs before using prior outputs. Conan and ccache can reuse matching packages
and object files across work directories. A build lock prevents concurrent
writers to the same work directory.

The driver checks upstream dependency declarations, selects pinned references,
and writes a full Conan lock before installing packages. `--conan-lock` can
reuse a reviewed lock in a new work directory; its direct and reviewed transitive recipes must match the
current selection and it must cover every resolved host and build dependency.
The generated host and build profiles replace dependency ranges with the exact
selected references, including tool dependencies in the build context.

All components share one CMake/Ninja tree. The driver builds the Storage Rust
bridge first with one Ninja job and the requested Cargo concurrency, then
builds C++ with the requested Ninja concurrency. A Cargo recheck during C++
compilation uses one Cargo job. `--jobs` accepts values from 1 through 50.

The Rust bridge links privately into Storage. Linux linker rules hide vendored
compression and prefixed AWS-LC symbols while preserving public Storage APIs.
Rust `openssl-sys` uses the Conan OpenSSL headers and shared libraries through
`OPENSSL_DIR`, `OPENSSL_STATIC=0` and `OPENSSL_NO_VENDOR=1`. Cargo's internal
compression implementations retain their own pins. This build produces no
Python bindings.

## Component extension contract

A consumer that owns another native engine prepares immutable source snapshots
and passes `--components /absolute/path/to/components.json`. The consumer owns
its engine source selection, CMake targets, acceptance tests and JVM binding.
The public driver owns Storage, Conan resolution, staging, platform handling
and provenance. An extension includes its targets into the same CMake tree and
installs its libraries into the common install directory. It must not replace
or rename the `storage` component.

The descriptor's `components` array has these fields for each component:

| Field | Meaning |
| --- | --- |
| `name`, `revision` | Distinct lowercase component name and exact 40-character Git revision |
| `sourceDirectory`, `sourceObjectDirectory` | Immutable source snapshot and original Git object source |
| `recipe`, `recipeOptions` | Upstream Conan recipe path and options used when validating its requirements |
| `cmake`, `cmakeVariables` | Component CMake file and cache variables; driver namespaces (`CMAKE_`, `MILVUS_`, `STORAGE_`, `FETCHCONTENT_`) are reserved |
| `loadEntries` | Platform-specific JNI filenames; JVM acceptance loads the combined entries in forward and reverse order, omitting duplicate orders |
| `dlopenEntries`, `parentProviders` | Engine/plugin entry libraries and explicit plugin-to-provider relationships for staging diagnostics |
| `testNames` | Exact CTest names required before a bundle can be promoted |
| `sourceLicenseDirectories` | Map from license owner to `{ "directory": "/source/snapshot", "files": ["LICENSE"] }`; owners must be the component name or begin with that name plus `-`; documents must be nonempty and remain inside the component snapshot |

The driver computes source manifests and source identities itself, hashes the
extension CMake directory, and rejects changed inputs on resume. Supplied source
hashes do not replace these checks. Component source and extension manifests
are included with the normalized evidence delivered in the bundle.

Top-level `references` adds or selects exact Conan recipe revisions.
`transitiveReferences` can extend the reviewed recipe seed without adding direct
requirements; platform scopes apply to both kinds of pin.
`versionPolicy` supplies version ordering and excluded requirement rules;
its nested maps extend the Storage policy. `platformScope` and `staticOn`
apply platform restrictions. `conanOptions` and `conanConf` become explicit
Conan host options and configuration, including package-ID inputs where needed.
Existing Storage dependency options and configuration cannot be overridden;
shared OpenBLAS configuration, for example, remains in the public profile.
Extensions cannot replace a recipe revision at the same version or alter an
existing platform/version policy. These inputs participate in the dependency
identity. Dependency conflicts must
satisfy the same newer-upstream-version policy as the public graph.

`featureOptions` contains boolean features. `metadata` records consumer-owned
provenance, such as the exact public native-tools revision; it cannot override
driver-owned identities. The resulting bundle contains every component in one
resource JAR. Consumers must not place a Storage-only bundle beside an extended
bundle on the same classpath.

## Outputs and acceptance

| Path below the work directory | Contents |
| --- | --- |
| `status.json` | Current phase and failure details |
| `sources/` | Isolated Storage and Corrosion source snapshots |
| `dependency-input/`, `dependencies/` | Exact requirements and Conan-generated CMake dependencies |
| `provenance/` | Source identities, dependency selection, tool versions, lock, graph, commands and configuration trace |
| `provenance/build-input/` | Snapshot of the native build implementation |
| `cmake-build/`, `install/lib/` | CMake/Ninja outputs and installed libraries before staging |
| `bundle/lib/` | Runtime closure with SONAME aliases and relative RPATH |
| `bundle/provenance.json`, `bundle/provenance/` | Normalized identities and evidence digests |
| `bundle/licenses/`, `bundle/audit/` | Upstream licenses and loader diagnostics |
| `bundle-candidates/`, `bundle-history/` | Unpromoted attempts and previous successful bundles |

Staging rejects conflicting SONAMEs and dependencies outside the selected graph,
except explicitly audited compiler runtimes and libaio. Only staged copies
receive relative RPATHs; source libraries and Conan packages are unchanged.
glibc, the platform C++ runtime and system zlib remain system dependencies. The
bundle records required zlib symbol versions and the validation host's provider.

Native acceptance runs fresh JVMs using `jvm_load.py` and `NativeLoadCheck.java`.
A public bundle loads the Storage JNI entry; an extended bundle tests each
unique forward/reverse order. The checker uses the selected JRE's `libjsig`,
clears additional library paths and JVM injection options, and rejects a failed
load. Provenance records `auditPolicy: jvm-load` and the exact `jvmLoadTests`.
Per-library relocation, private-symbol and `RTLD_NOW` checks must also pass.
Plugins may resolve declared parent-engine symbols only when their recorded
parent provides every unresolved symbol and the parent-context check succeeds.
A missing diagnostic tool or failed dependency check blocks acceptance even
when both JVM loads succeed. The packager repeats these checks on its input bytes. JNI load success does not establish query or
write correctness; native fixtures and object-storage integration remain
separate acceptance steps.

Only a candidate whose declared component tests, relocations and JVM loading pass replaces
`bundle/`. A failed retry preserves the previous successful bundle. Package a
validated directory with:

```bash
python3 scripts/package-native.py \
  --lib-dir /absolute/path/to/storage-build/bundle/lib \
  --provenance /absolute/path/to/storage-build/bundle/provenance.json \
  --licenses /absolute/path/to/storage-build/bundle/licenses \
  --evidence /absolute/path/to/storage-build/bundle/provenance \
  --output /absolute/path/to/milvus-native-linux-x86_64.jar
```

Packaging verifies hashes, aliases, dependencies, architectures, component test
records and fresh JVM loads. The format-2 manifest lives under
`native/milvus/2/<platform>/`, names its components and records their revisions.
JVM bindings declare independent requirements under
`META-INF/milvus-native/requirements/`, indexed by `requirements.list`. The
shared runtime checks all binding requirements against the single bundle.

The resource JAR contains normalized evidence and licenses, without build-host
absolute paths. Full source snapshots, the Conan graph, compiler commands and
expanded CMake trace remain in the work directory; recorded hashes associate
them with the delivered libraries. CI cache identities include component
revisions, platform ABI requirements, toolchain and functional build inputs.
Markdown and top-level Python driver tests do not change the bundle cache key;
strict local resume still checks the complete build snapshot. ABI and consumer
checks run on cache hits too.

Run focused driver regression tests with:

```bash
python3 -m unittest discover -s native/build -p 'test_*.py' -v
```
