# Working on the Milvus Spark Connector

Orientation for anyone opening this repository, human or agent. It carries the
model, the current state and the invariants, and points at the file that holds
the detail. `CLAUDE.md` is a symlink to this file, so every tool reads the same
page; edit this one.

## What this project is

Spark reads and writes Milvus collections by going straight at the Milvus
storage format on object storage, not through the Milvus query path. A read
lists a snapshot, plans one Spark partition per segment, and pulls Arrow column
batches out of the segment files. A write produces segment files in the same
format and registers them back with Milvus.

Two lines exist. The 1.x line is frozen at tag `v1.6.0` and takes fixes only.
The 2.0 line is a rewrite on branch `refactor/v2`, versioned
`2.0.0-{branch}-{arch}-SNAPSHOT`. Everything below describes 2.0.

## The four layers

Eleven sbt modules, and dependencies only point downward. This is the model
every rule below refers to.

| Layer | Modules | What lives there |
|---|---|---|
| 1 | `native-storage`, `native-vector` | JNI over two C libraries: milvus-storage's `loon_*` and a `mv_*` shim around knowhere. Nothing above this layer loads a `.so`. |
| 2 | `core`, `compat`, `client` | The Milvus storage format and the client for the online service. All computation happens here. No Spark: a source file mentioning `org.apache.spark` fails the build. |
| 3 | `spark-base`, `spark-3.5`, `spark-4.0`, `spark-4.1`, `spark-4.2` | The DataSource V2 surface. `spark-base` is a shared source directory, not a project; each line project compiles it against its own Spark, Arrow, antlr and Java version. |
| 4 | `apps-4.0` | The jobs users run: backfill, brute-force search, diagnostic tools, the legacy gRPC insert path. |

`integration-4.0` sits outside the layering and outside root's aggregate: its
suites need a live Milvus and MinIO.

Only layer 3 splits per Spark line, because the interfaces differ there.
`ProcedureCatalog` exists only in Spark 4.0 and later, and Arrow, antlr and the
Java target are pinned per line. The fat jar is an `assembly` task on
`spark-<line>`, not a module of its own. [README.md](README.md) has the full
module table.

## Where the work stands

The design documents describe the finished 2.0. Most of layer 2's computation is
not written yet. Read them as a target, not as a description of the code.

Done: the 1.x sources are all in their modules and `src/` no longer exists. 568
unit tests pass. `core` has schema, codec, snapshot, manifest, delete, path and
a Hadoop-backed reader; `compat` has the V2 packed and backup entry points;
`client` is complete.

Not written: the `ObjectStore` interface itself, plus `credential`, `expr`,
`index`, `stats`, `read.plan`, `read.exec`, `write.commit` and `write.exec` in
core. Their `package.scala` files exist and state what belongs there. The same
is true of `compat.offline` and `apps.legacy`, whose code is still sitting in
`spark-base`.

Layer 3 holds the 1.x connector code as it was. Splitting it into catalog,
table, scan, write, options, types and expr is the next refactor, not a move.

Layer 1 is placeholders. All four Spark lines still consume the upstream
milvus-storage Java binding as an unmanaged jar, which is also why the 3.5 line
cannot produce Scala 2.12 artifacts yet.

Nine design questions are still open: 5, 6, 10, 11, 12, 13, 14, 16 and 19 in
section 4 of [docs/design/README.md](docs/design/README.md). Several of them
block specific packages, so check that list before starting on one.

## How the documents index the code

The capability id is the spine. Every feature the connector commits to has an id
in [docs/design/capabilities.md](docs/design/capabilities.md) — `R*` read, `W*`
write, `C*` catalog, `A*` procedures, `V*` vector, `K*` compatibility entry
points, `O*` apps, `G*` configuration — along with the user-facing entry point,
the package that implements it, its prerequisites and its priority. Every
`package.scala` names the ids its package carries. Follow either direction to
get from a feature to its code.

The package names in the 实现位置 column are design names, not paths. They
resolve mechanically: `core.X` is `com.zilliz.milvus.storage.X`, `compat.X` is
`com.zilliz.milvus.storage.compat.X`, `client.X` is
`com.zilliz.milvus.client.X`, `apps.X` is
`com.zilliz.spark.connector.apps.X`, and `spark.X` is
`com.zilliz.spark.connector.X`, which exists once per Spark line.

Section 10 of capabilities.md lists what 2.0 deliberately will not do: TopN and
aggregate pushdown, UPDATE and MERGE, the `text_match` family, GIS expressions,
struct-array expressions, `random_sample`, DataSource V1 filters, and reading or
writing Vortex column groups. Check it before designing around a gap.

| Question | File |
|---|---|
| What does the connector commit to doing? | [docs/design/capabilities.md](docs/design/capabilities.md) |
| How is it layered, and what is still undecided? | [docs/design/README.md](docs/design/README.md) — read path, write path, priorities, open decisions in section 4, decision log in section 6 |
| Which module and package does a thing belong to? | [docs/design/modules.md](docs/design/modules.md) — module table, package design, directory tree, build constraints in section 4, migration state in section 5 |
| How do we compare with the Lance Spark connector? | [docs/design/lance-spark.md](docs/design/lance-spark.md) |
| Illustrated versions of the above | [docs/design/overview.html](docs/design/overview.html), [docs/design/lance-spark.html](docs/design/lance-spark.html) |
| What options does a user pass? | [docs/reference-en.md](docs/reference-en.md), [docs/reference-cn.md](docs/reference-cn.md) |
| What is a given package responsible for? | The `package.scala` or `package-info.java` in that package |
| How do I build, test and run it? | [README.md](README.md), then [docs/contributing.md](docs/contributing.md) for the mechanics on top |
| What sits outside this repository? | [docs/context.md](docs/context.md) |
| How should a document here be written? | [docs/writing.md](docs/writing.md) |

## Rules any change has to satisfy

The full list is section 4 of [modules.md](docs/design/modules.md); these four
get violated most often.

1. **No Spark below layer 3.** A source file in `core`, `compat` or `client`
   that mentions `org.apache.spark` fails the build. The check strips comments
   first, so explaining that a class used to extend Spark's `Logging` is fine.
   Use `com.zilliz.milvus.storage.Logging` instead.
2. **Directories mirror packages.** Scala allows a mismatch; this repository
   does not.
3. **Dependencies point downward only.** If a lower layer needs something from a
   higher one, the thing is in the wrong layer. Move it; do not add a back edge.
   Six such edges were found during the 2.0 migration and every one turned out
   to be a misfiled file, not a real cycle.
4. **Every decision goes in the log.** A choice that shaped the code belongs in
   section 6 of `docs/design/README.md` with its reason, including what was
   rejected. A choice still open belongs in section 4 and nowhere else.

Two habits follow from the same idea. **No compatibility shims**: when code
moves, update the call sites, because a forwarding stub hides the move and
outlives the migration. **Say when a document is wrong**: the design documents
were written before the code was read, the migration has already corrected
several of their claims, and correcting one is part of the change rather than a
follow-up.

Code, comments, build scripts and `README.md` are English. The design documents
under `docs/design` are Chinese. Do not mix within a file.

## Keeping the index true

An index nobody maintains stops being an index. These are the files that have to
move together, and the part of it the build enforces.

| When you | Also update |
|---|---|
| add or change a capability | its row in `capabilities.md`, and the `package.scala` of the package that row names |
| move code between packages or modules | both `package.scala` files, section 5 of `modules.md`, and the 实现位置 of every capability that named the old package |
| create a package | its `package.scala`, naming the main types and the capability ids it carries, and the package table in section 2 of `modules.md` |
| settle an open question | delete it from section 4 of `README.md` and write it into the log in section 6 |
| defer a question | section 4 of `README.md`, and nowhere else |
| add a build constraint | section 4 of `modules.md`, plus a check that fails the build if one can be written |
| change a user-facing option | `docs/reference-en.md` and `docs/reference-cn.md` |
| find a document claim the code contradicts | fix the document in the same commit and say so in the message |

`sbt checkCapabilityIndex` runs in CI and fails when an id in `capabilities.md`
appears in no `package.scala` and is not listed in its section 11, when an id in
a `package.scala` does not exist in `capabilities.md`, or when 实现位置 names a
package that does not exist on disk. Section 11 of capabilities.md is where a
capability with no home yet is declared, with the reason.

The other rows are on you. Those are the ones that rot, so they are what a
review should look at.

## Reading order

New to the repository: this file, then `README.md`, then
`docs/design/README.md`.

Planning work: `docs/design/capabilities.md` for what is in scope, section 4 of
`docs/design/README.md` for what is still undecided, section 5 of
`docs/design/modules.md` for where the code stands.

Writing code: the `package.scala` of the package you are touching, then section
4 of `docs/design/modules.md`, then [docs/contributing.md](docs/contributing.md).
