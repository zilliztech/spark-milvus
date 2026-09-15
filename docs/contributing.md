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
| `project/Modules.scala` | Shared compile/test settings, checks and the temporary JNI dependency |
| `project/plugins.sbt` | Build plugins and their meta-build dependencies |

## Tests that need the native library

Suites that call into C check for `libnative-storage-jni` first and cancel
themselves when it is absent, so a machine without it still gets a green run
with those suites reported as canceled rather than failed:

- `StorageNativeTest` and `WriterRoundTripTest` in core
- `MilvusV3PartitionWriterLifecycleTest` in spark-4.0

The UAT suites — `StorageNativeUatTest` and `SegmentReaderUatTest` in core,
`StorageFullChainUatTest` and `SnapshotReadUatTest` in spark-4.0 — cancel on their own environment
variables as well, so they stay canceled even with the library present.

Everything else passes. A green run looks like this:

| Module | Tests |
|---|---|
| core | 82, plus 3 canceled |
| compat | 40 |
| client | 20 |
| spark-4.0 | 307, plus 1 canceled |
| apps-4.0 | 212 |

`integration-4.0` needs a real Milvus on 19530 and MinIO on 9000; it compiles in
CI but is not run there.

## Building the native library

`make build-milvus-storage && make copy-native-libs` builds
`libmilvus-storage`, `libmilvus-storage-jni` and this repository's own
`libnative-storage-jni`, and puts all three where `NativeLibraryLoader` can find
them. `copy-native-libs` builds the last one itself when it is missing; `make
build-native-jni` builds only it. With them in place the suites above run
instead of cancelling. It needs conan, CMake and a Rust toolchain.

This works on macOS. An earlier version of this page said the library is built
inside the Docker image and not on a developer machine; that was wrong, and it
described a broken local setup as if it were a property of the platform.
milvus-storage supports macOS and has a CI job for it — `cpp-mac-ci.yml` builds
on `macos-26` with conan 2.25.1, CMake 3.31.10 and LLVM 18 from brew.

### Two things that will bite

**Pin a CMake 3.x somewhere durable.** Several packages in the dependency chain
cap their CMake policy range below 4, so the conan profile has to set
`tools.cmake:cmake_program`. A path under `/tmp` disappears on cleanup and the
build then fails with `cmake: No such file or directory` before compiling
anything — the error names the missing binary, not the real problem.

**The artifact is large.** `libmilvus-storage.dylib` is 481 MB, of which 162 MB
is an unstripped symbol table (`strip -x` takes it to 315 MB); the two JNI
bridges are 177 KB and 85 KB. Everything — arrow, parquet, the AWS/Azure/GCP SDKs, the Rust
bridge — is linked statically so the library loads without help from the host.
`native-storage/src/main/resources/native/` is gitignored, so none of this is
committed. Section 4.2 of
[storage-access.html](design/architecture/storage-access.html) covers what the
size costs.

### What the Makefile does per platform

Three things differ between macOS and Linux, and the Makefile now derives all
three from `uname`:

| | macOS | Linux |
|---|---|---|
| Suffix | `.dylib` — `add_library(... SHARED)` sets no `SUFFIX` | `.so` |
| Build output | `cpp/build/Release` — the `POST_BUILD` step that fills `build/Release/lib` sits inside `if(NOT APPLE)` in `cpp/CMakeLists.txt`, so that directory never exists here | `cpp/build/Release/lib` |
| Resource path | `native/darwin-aarch64/` | `native/linux-<arch>/` |

The resource path matters: `NativeLibraryLoader.stripPlatformPrefix` skips any
JAR entry that is not under `native/<platform>/`, so a library copied flat into
`native/` is silently never extracted.

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

`sbt scalafmtAll` formats every module. Parts of the 1.x tree were never
formatted, so a blanket run touches files unrelated to your change — revert
those before committing rather than mixing them in.

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
