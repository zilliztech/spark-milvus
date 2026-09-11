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

`build.sbt` starts with shared defaults and explicit module declarations, then
groups root packaging and publishing at the end. Module-specific dependencies
stay beside the module that uses them.

| File | What to change there |
|---|---|
| `project/Versions.scala` | Library versions and the Spark line matrix |
| `project/Dependencies.scala` | Dependency coordinates, scopes and dependency groups |
| `project/Modules.scala` | Shared compile/test settings, checks and the temporary JNI dependency |
| `project/plugins.sbt` | Build plugins and their meta-build dependencies |

Reuse `Modules.jacksonPin` only in core, compat and client. Root, Spark and apps
share `Modules.legacyJni` until the native-storage module replaces the upstream
Scala binding. `Dependencies.legacyRootDeps` preserves the root artifact's
existing dependency declarations, including its separate `legacyRootArrow`
version; it is not the version matrix for the Spark modules.

Adding a Spark line means adding its version row, an explicit `sparkProject`
declaration and an aggregate entry. Keep this wiring visible, and extract shared
settings when they remove real duplication. The reasons for migration choices
belong in the design decision log.

## Tests that cannot pass locally

Two tests need `libmilvus-storage-jni`, which is built inside the Docker image
and not on a developer machine. They fail with `UnsatisfiedLinkError` and that
is expected:

- `MilvusStorageFFITest`
- `MilvusStorageMultiFileGroupTest`

Everything else passes. A green run looks like this:

| Module | Tests |
|---|---|
| core | 32 |
| compat | 43 |
| client | 20 |
| spark-4.0 | 261, of which the two above fail locally |
| apps-4.0 | 212 |

`integration-4.0` needs a real Milvus on 19530 and MinIO on 9000; it compiles in
CI but is not run there.

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
