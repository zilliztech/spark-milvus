# Working on the Milvus Spark Connector

Orientation for anyone opening this repository, human or agent. `CLAUDE.md` is
a symlink to this file, so every tool reads the same page; edit this one.

The full guide — where the work stands, which document governs which
subsystem, the design principles and the index-maintenance rules — lives in
the private design repository `zilliztech/spark-milvus-design`, mounted here as
the submodule `docs/design`. If that directory is checked out, read
`docs/design/AGENTS.md` before anything else; it continues this page. If it is
not, this page and [docs/contributing.md](docs/contributing.md) are what you
have, and they are enough to build, test and change the code.

## What this project is

Spark reads and writes Milvus collections by going straight at the Milvus
storage format on object storage, not through the Milvus query path. A read
lists a snapshot, plans one Spark partition per segment, and pulls Arrow column
batches out of the segment files. A write produces segment files in the same
format and registers them back with Milvus. Vector search enters through
`MilvusSearch.search`, which takes a query set and returns each query's top-k
over the snapshot's persisted indexes or by exact scan.

Two lines exist. The 1.x line is frozen at tag `v1.6.0` and takes fixes only.
The 2.0 line is a rewrite on branch `refactor/v2`, versioned
`2.0.0-{branch}-{arch}-SNAPSHOT`. Everything below describes 2.0.

## The four layers

Twelve sbt modules, and dependencies only point downward.

| Layer | Modules | What lives there |
|---|---|---|
| 1 | `native-runtime`, `native-storage`, `native-vector` | Storage uses milvus-storage's upstream JNI and Java/Scala API; vector library loading through Knowhere's upstream C/JNI and Java API. `native-runtime` verifies and extracts the unified platform bundle for both bindings. Nothing above this layer loads a `.so`. |
| 2 | `core`, `compat`, `client` | The Milvus storage format and the client for the online service. All computation happens here. No Spark: a source file mentioning `org.apache.spark` fails the build. |
| 3 | `spark-base`, `spark-3.5`, `spark-4.0`, `spark-4.1`, `spark-4.2` | The DataSource V2 surface. `spark-base` is a shared source directory, not a project; each line project compiles it against its own Spark, Arrow, antlr and Java version. |
| 4 | `apps-4.0` | The backfill job users run, and the SQL vector distance functions. |

`integration-4.0` sits outside the layering and outside root's aggregate: its
suites need a live Milvus and MinIO.

Only layer 3 splits per Spark line, because the `TableCatalog` and
`ParserInterface` method sets differ there, while Arrow, antlr and the Java
target are also pinned per line. During migration, the usable fat jar is root's
`assembly`. [README.md](README.md) has the full module table and the build
commands.

Four git submodules: `milvus-proto`, `milvus-storage` and `knowhere` supply
the protobuf definitions and the two native engines and must be initialized to
build; `docs/design` is the private design repository and is not needed by the
build or by CI.

## Rules any change has to satisfy

1. **No Spark below layer 3.** A source file in `core`, `compat` or `client`
   that mentions `org.apache.spark` fails the build. The check strips comments
   first. Use `com.zilliz.milvus.storage.Logging` instead of Spark's.
2. **Directories mirror packages.** Scala allows a mismatch; this repository
   does not.
3. **Dependencies point downward only.** If a lower layer needs something from a
   higher one, the thing is in the wrong layer. Move it; do not add a back edge.
4. **Every decision is written down** in the design repository before the code
   that depends on it is written, and every capability the connector commits
   to has a row in its `capabilities.md`; each `package.scala` names the
   capability ids its package carries, and `sbt checkCapabilityIndex` checks
   the two against each other.

Two habits follow from the same idea. **No compatibility shims**: when code
moves, update the call sites, because a forwarding stub hides the move and
outlives the migration. **Fix causes, never patch the edge**: swallowing an
exception, turning a failure into an empty result or adding a flag that routes
around a defect makes a visible problem invisible while the system keeps running
wrong.

Code, comments, build scripts, `README.md` and the documents under `docs/` are
English, and the public documents are Markdown. Use imports and short type or
object names in code; follow the [import rules](docs/contributing.md#imports).

## Before every commit

Apply [.agents/skills/spark-milvus-sbt/SKILL.md](.agents/skills/spark-milvus-sbt/SKILL.md).
The [formatting](docs/contributing.md#formatting) and
[unit tests](docs/contributing.md#unit-tests) sections of the contributing guide
hold the required commands: the formatter and its check with Java 21, `git diff
--check`, then the full root unit test suite. A change to a `package.scala` or
to `docs/design/capabilities.md` also runs `sbt checkCapabilityIndex`, which
needs the `docs/design` submodule checked out. A document change under
`docs/design` is committed and pushed in that repository first; the gitlink
here moves in the same commit as the code it describes
([Documents](docs/contributing.md#documents)).
