# Working on the Milvus Spark Connector

Orientation for anyone opening this repository, human or agent. `CLAUDE.md` is
a symlink to this file, so every tool reads the same page; edit this one.

This page and [docs/contributing.md](docs/contributing.md) are what you need
to build, test and change the code.

## What this project is

Spark reads and searches fixed Milvus Storage V3 snapshots directly from object
storage. The caller provides an existing snapshot and storage configuration;
the public connector does not call Milvus or prepare snapshots. Reads plan
segment partitions and consume Arrow batches. NEAREST BY returns top-k matches
using exact scans or compatible persisted indexes. Spark 4.2 supplies the join
syntax; the connector supplies `nearestByJoin` and `nearest_by_join` on 3.5–4.1.
Writing, backfill, index construction, snapshot output, service clients, backup
and legacy storage compatibility are outside this repository's scope.

The 1.x line stopped at tag `v1.6.0` and is no longer maintained: it takes no
fixes. The 2.0 line is a rewrite developed on `main` and released from branch
`2.0`, where the `v2.0.x` tags are cut; snapshots are versioned
`2.0.0-{branch}-{arch}-SNAPSHOT`, and `2.0.0-{arch}-SNAPSHOT` on the release
branch. Everything below describes 2.0.

## The three public layers

Five production modules, with dependencies only pointing downward.

| Layer | Modules | What lives there |
|---|---|---|
| 1 | `native-runtime`, `native-storage`, `native-vector` | Unified platform loading and the upstream Storage/Knowhere APIs; nothing above this layer loads a `.so`. |
| 2 | `core` | V3 snapshot formats, read/search computation and storage access; no Spark dependencies. |
| 3 | `spark` | Read-only DataSource V2 and vector search, with shared sources and selected Spark shims. |

The layer-1 modules live in `native/runtime`, `native/storage` and
`native/vector`, and the unified native build that produces the platform bundle
is `native/build/`; the project IDs keep their `native-` names. The three
submodules stay at the repository root.

The root aggregates those modules and publishes no artifact. `integration`
is outside the aggregate and tests fixed V3 inputs in a real object store;
it needs no Milvus service. Use `spark/assembly` for the selected local bundle.

`-DsparkVersion` selects 3.5, 4.0 (default), 4.1 or 4.2;
`-DscalaVersion` selects a supported full Scala version (default 2.13.16).
Only Spark 3.5 also supports 2.12.20. Restart sbt and clean module outputs when
switching combinations. `spark/src/main/scala` is shared; explicit
`scala-shims` roots contain the remaining Dataset and NEAREST BY differences.
Formatting and source/package checks cover every root, while compilation and
source JARs contain only the selected roots. See [README.md](README.md).

Three git submodules: `milvus-proto`, `milvus-storage` and `knowhere` supply
the protobuf definitions and the two native engines and must be initialized to
build.

## Rules any change has to satisfy

1. **No Spark below layer 3.** A source file in `core` or `native-storage`
   that mentions `org.apache.spark` fails the build. The check strips comments
   first. Use `com.zilliz.milvus.storage.Logging` instead of Spark's.
2. **Directories mirror packages.** Scala allows a mismatch; this repository
   does not.
3. **Dependencies point downward only.** If a lower layer needs something from a
   higher one, the thing is in the wrong layer. Move it; do not add a back edge.
4. **Every decision is written down** before the code that depends on it is
   written: each `package.scala` states what its package is responsible for,
   and a change that alters a responsibility updates it in the same commit.

Two habits follow from the same idea. **No compatibility shims**: when code
moves, update the call sites, because a forwarding stub hides the move and
outlives the migration. **Fix causes, never patch the edge**: swallowing an
exception, turning a failure into an empty result or adding a flag that routes
around a defect makes a visible problem invisible while the system keeps running
wrong.

Code, comments, build scripts, `README.md` and the documents under `docs/` are
English, and the public documents are Markdown. Use imports and short type or
object names in code; follow the [import rules](docs/contributing.md#imports).

Repository documents must not cite workstation-hosted web documents. Copy any
needed document and its required assets into the appropriate repository first,
then cite that copy with repository-relative links; see
[document references](docs/contributing.md#repository-local-document-references).

## Before every commit

Apply [.agents/skills/spark-milvus-sbt/SKILL.md](.agents/skills/spark-milvus-sbt/SKILL.md).
The [formatting](docs/contributing.md#formatting) and
[unit tests](docs/contributing.md#unit-tests) sections of the contributing guide
hold the required commands: the formatter and its check with Java 21, `git diff
--check`, then the full root unit test suite. A change to a `package.scala`
keeps its responsibility statement current
([Documents](docs/contributing.md#documents)).
