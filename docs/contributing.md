# Working in this repository

The connector reads fixed Storage V3 snapshots and replaces their stored columns
from a DataFrame. Callers own snapshot preparation and service submission. See [README](../README.md).

## Development container

`scripts/devcontainer.sh` runs the root Dockerfile toolchain with this checkout
mounted at `/workspace` and isolated Conan, ccache, Cargo, Coursier and sbt caches.
Use `up`, `init`, `shell`, `run <command>` and `down`. Build outputs stay under
module `target` directories. Native compilation uses the host architecture.

## Project ids and version selection

Production projects are `native-runtime`, `native-storage`, `core` and `spark`.
The first two live under `native/`. `integration` stays outside the root
aggregate. Profiles support 3.5 (Scala 2.12/2.13), 4.0, 4.1 and 4.2.

```bash
sbt core/test spark/compile
sbt -DsparkVersion=4.2 -DscalaVersion=2.13.16 'clean; integration/clean; spark/compile; spark/test'
sbt -DsparkVersion=3.5 -DscalaVersion=2.12.20 'clean; integration/clean; spark/compile; spark/test'
```

Restart sbt and clean module outputs between selections. Do not use interactive
`++` to switch the full profile. All shim roots are formatted and checked;
compilation and source JARs include only selected roots. The root publishes no
artifact. Use `spark/assembly` for a local reader bundle.

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

Follow the [sbt skill](../.agents/skills/spark-milvus-sbt/SKILL.md).
`build.sbt` owns module wiring and exports; `project/Versions.scala` owns profiles;
`Dependencies.scala` owns dependency declarations; `Modules.scala` enforces
layering and tests. `NativeBundle.scala` validates component requirements and
platform bundles. `ThinArtifacts.scala` exports the reader closure;
`ReleaseArtifacts.scala` exports the reader JVM distribution.

## Fixed inputs for read tests

Fixed files in `core/src/test/resources/read-fixtures` and `core/src/test/data`
have checksums and provenance. Ordinary tests verify and copy them, never
regenerate them.

```bash
python3 scripts/ci/check_read_fixtures.py
```

## Tests that need the native library

Run on JDK 21. Spark 3.5 Arrow tests fork Java 17 supplied by
`SPARK35_TEST_JAVA_HOME`. Native-dependent ordinary read tests may cancel when no
bundle is available; report successful, failed, canceled and aborted counts.
A green ordinary test run is not native acceptance: the column replacement's
object-storage cases also cancel without their fixture, so the explicit
validation below requires both and fails on any cancellation.

```bash
python3 scripts/ci/read_storage.py start --directory /tmp/milvus-read-fixture
export MILVUS_READ_IT_OPTIONS=/tmp/milvus-read-fixture/a.json
export MILVUS_READ_IT_OPTIONS_B=/tmp/milvus-read-fixture/b.json
sbt -Dmilvus.native.bundle=/absolute/path/to/bundle.jar integration/test
python3 scripts/ci/read_storage.py stop --directory /tmp/milvus-read-fixture

python3 scripts/ci/input_storage.py start --env-file /tmp/milvus-input.env
eval "$(cat /tmp/milvus-input.env)"
python3 scripts/ci/read_validation.py column-write --matrix build-matrix.json \
  --line 4.0 --scala 2.13.16 --native-bundle /absolute/path/to/bundle.jar
python3 scripts/ci/input_storage.py stop --env-file /tmp/milvus-input.env
```

`build-matrix.json` is the source matrix that `sbt 'exportBuildMatrix
build-matrix.json'` writes. Always stop owned fixtures after failures. Column
replacement acceptance exercises real V3 writes and readback, locally and on
object storage under an identity scoped to one key prefix. No Milvus service is
needed. Never mutate shared Hadoop settings to switch users.

### Public read CI

The source matrix covers every Spark/Scala combination. Full validation requires
native Storage reads, Arrow ownership, Connect, independent consumer JVMs,
object-store identity isolation and, on every combination, column replacement
writes and readback under a prefix-scoped identity.
Linux x86/ARM and macOS Storage coverage is retained.

## Building the native library

Initialize `milvus-proto` and `milvus-storage`, then use `make native-bundle`.
The pinned toolchain and per-platform requirements are maintained in
[native/build/README.md](../native/build/README.md). Native builds are explicit;
ordinary sbt compilation does not start Conan. Use Java 21 and the running JRE's
`libjsig` on driver/executors. Do not preload a Linux library on macOS.

## Unified native bundle

Manifest version 2 records the platform, component set, source revisions,
load entries, library checksums and build provenance. Public bundles contain
only `storage`. Binding JARs declare component requirements; the shared runtime
extracts one complete dependency set and the upstream loader performs loading.
An embedding distribution can use a complete superset bundle, with every
component checked against its owning binding. Two bundles for the same platform
are rejected. A cache hit still requires current ABI and loading acceptance.

```bash
sbt -Dmilvus.native.bundle=/absolute/path/to/bundle.jar test spark/assembly
python3 -m unittest discover -s native/build -p 'test_*.py'
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 -m unittest discover -s scripts/ci -p 'test_*.py'
```

### Checking native resource merging

`native/milvus/` resources must agree byte for byte. Component descriptors are
unique; only `requirements.list` combines distinct component names. An assembly
must never silently choose one of two different native library copies.

## JVM version

Use Java 21. On Java 26 every test that starts a SparkSession aborts with
`UnsupportedOperationException: getSubject is not supported`, because
`Subject.getSubject` was removed and Spark still calls it. The failure looks
like a code problem and is not one.

## How tests find their fixtures

Several suites read fixture files by a path relative to the repository root, for
example `core/src/test/data/sample_snapshot.json`. Forked tests would otherwise run
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

Existing native-library cancellation conditions remain part of the test setup;
report them and the actual counts as described in
[Tests that need the native library](#tests-that-need-the-native-library).
The root run does not include `integration/test`; its V3 read suite requires a
real object store. Additional Scala
cross-version checks, integration tests, native
builds and publication follow the task's scope and authorization.

CI runs `python3 -m unittest discover -s native/build -p 'test_*.py'` plus the
same discovery over `scripts/tests`; run them when touching `native/build/`
or `docker/`, whose image build test lives there.
Python examples live in `demo/python`, Java examples in
`demo/java/src/main/java/com/zilliz/spark/connector/demo`, and helper/launcher
tests in `demo/tests`. After changing a demo, run
`python3 -m unittest discover -s demo/tests -p 'test_*.py'` and the affected
examples in both languages with the runtime described in
[demo/README.md](../demo/README.md), such as
`./demo/run.sh python 01_read_snapshot.py` and `./demo/run.sh java ReadSnapshot`.
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

## Pull requests

Changes reach `main` and the release branches only through pull requests, and
pull requests come from a fork: a repository ruleset reserves branch creation on
`zilliztech/spark-milvus` for maintainers, who use it for the `{major}.{minor}`
release branches. Push your branch to your fork and open the pull request
against `main`, or against a release branch for a fix that ships on that line.
The ordinary CI checks run for a fork pull request without repository secrets;
the native builds and consumers run once the change is pushed to the protected
branch. A pull request needs one approving review and the required checks, and
merges by squash; `main` merges through its merge queue.

## Commits

Write the message in English, explain why rather than what, and name the
verification you actually ran. When a change contradicts a document in this
repository, say so in the message and fix the document in the same commit.

## Documents

Public documents are English Markdown files in `docs/`, including this
guide, [reference.md](reference.md), and the
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
