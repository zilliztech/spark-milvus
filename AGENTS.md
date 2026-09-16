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

Done: the 1.x sources are all in their modules and `src/` no longer exists. 641
unit tests pass. `core` has schema, codec, snapshot, manifest, delete, path,
credential and `io.ObjectStore` over the native filesystem; `compat` has the V2
packed and backup entry points; `client` is complete. Every driver-side read
opens storage through that one store, and no source file in `core` or `compat`
mentions `org.apache.hadoop`.

Not written: `expr`, `index`, `stats` and `write.commit` in core. Their
`package.scala` files exist and state what belongs there. `read.plan` holds
the task description, `read.exec` opens the native reader, `write.exec` opens
the native writer and commits the manifest; the driver-side partition builder
and the job-level commit are still to come.

Layer 3 is split by package: `sources` holds only the `format("milvus")`
entry point, `table` the table, `read` the scan builder, the scan and the
executor-side readers, `read.plan` the partition builder and delete planning,
`write` the two writers, `options` the option parsing, the driver's storage
access and the choice of `SnapshotSource` for a read, `types` the type
mapping. `getTable` resolves the `Snapshot` once and the table carries it to
the scan; moving the partition builder into `core.read.plan` is the next
step. `catalog` and `expr` are empty. Names follow the Names section of
[docs/writing.md](docs/writing.md): the two storage lines are `V2` and `V3`
everywhere, after the snapshot's `storage_version`.

Layer 1 is written and in use: `native-storage` wraps the `loon_*` entry points
for both reading and writing and loads its own libraries. The upstream
milvus-storage Java binding is out of the build, so the 3.5 line cross-compiles
for Scala 2.12 again. `native-vector` is still a placeholder.

Six design questions are still open: 10, 16, 19, 20, 21 and 22 in
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
| Which module and package does a thing belong to? | [docs/design/architecture/modules.md](docs/design/architecture/modules.md) — module table, package design, directory tree, build constraints in section 4, migration state in section 5 |
| How does core reach object storage? | [docs/design/architecture/storage-access.html](docs/design/architecture/storage-access.html) — the route (JNI to the C filesystem), how credentials reach it, the next steps and the three facts still to verify; apply the skill [.agents/skills/spark-milvus-storage-access/SKILL.md](.agents/skills/spark-milvus-storage-access/SKILL.md) |
| How are object storage credentials handled? | [docs/design/architecture/storage-auth.html](docs/design/architecture/storage-auth.html) for the mechanism, the rules and the measured facts; apply the skill [.agents/skills/spark-milvus-storage-auth/SKILL.md](.agents/skills/spark-milvus-storage-auth/SKILL.md) |
| How do bytes and Arrow cross between C and the JVM? | [docs/design/architecture/storage-io.html](docs/design/architecture/storage-io.html) — layer 1's two faces, the per-batch Arrow handshake, handle ownership, the metrics taken on the crossing (G5). Read and write share it |
| How does a read run, today and as designed? | [docs/design/architecture/read.html](docs/design/architecture/read.html) — the four snapshot sources, the one executor read path, `core.read.plan` and `core.read.exec`, the development outline |
| How does a write run, and what is still missing at the entry point? | [docs/design/architecture/write.html](docs/design/architecture/write.html) — the DataSource V2 write chain, where the write table gets the collection schema, the WriteBuilder checks, the three things a segment still lacks before registration, the development outline |
| How will vector queries use persisted Milvus indexes? | [docs/design/architecture/vector-search.html](docs/design/architecture/vector-search.html) — issue #125 development proposal: index metadata, native loading, filtering, row retrieval, global TopK and validation; pending review |
| What is a Snapshot, and how do the four read entry points become one? | [docs/design/architecture/snapshot.html](docs/design/architecture/snapshot.html) — `Snapshot` and `Segment`, the three delete states, what each source cannot supply, `SnapshotCatalog`, the boundary to `SegmentReadTask`. Draft under review |
| How does backfill reach more than one bucket? | [docs/design/apps/backfill-storage.html](docs/design/apps/backfill-storage.html) |
| Illustrated version of the above | [docs/design/architecture/overview.html](docs/design/architecture/overview.html) |
| What options does a user pass? | [docs/reference-en.md](docs/reference-en.md), [docs/reference-cn.md](docs/reference-cn.md) |
| What is a given package responsible for? | The `package.scala` or `package-info.java` in that package |
| How do I build, test and run it? | [README.md](README.md), then [docs/contributing.md](docs/contributing.md) for the mechanics on top |
| How do we review or change the sbt build? | [docs/design/engineering/sbt.html](docs/design/engineering/sbt.html) for principles and practice; apply the repository skill [.agents/skills/spark-milvus-sbt/SKILL.md](.agents/skills/spark-milvus-sbt/SKILL.md) for build work |
| What must run before every commit? | Apply [.agents/skills/spark-milvus-sbt/SKILL.md](.agents/skills/spark-milvus-sbt/SKILL.md); [formatting](docs/contributing.md#formatting) and [unit tests](docs/contributing.md#unit-tests) in the contributing guide hold the required commands and constraints |
| What sits outside this repository? | [docs/context.md](docs/context.md) |
| How should a document here be written? | [docs/writing.md](docs/writing.md) |

## Principles

These come before the rules below. The rules are mechanical and the build checks
them; these are judgment, and judgment decides first.

**Design comes before code, and the design is written down.** No capability is
implemented until the design that governs it exists as a document reachable from
this page. A row in capabilities.md is not a design: it says what and where, not
how. When the row is all there is, writing the design is the first task, not
something done while coding.

Before writing code for a capability, confirm four things: it has a row in
capabilities.md; every decision it depends on is out of section 4 of
docs/design/README.md; the package it lands in exists with a `package.scala`
that states its responsibility; and the design that governs it is written. If
one is missing, that is the work, and it comes first.

A subsystem earns its own document in a topic directory under `docs/design`
when its design cannot be stated in its capability row plus the layering in
`docs/design/README.md`. That document says what the subsystem is, what it touches
globally, how it will be built, which industry practice it follows or rejects
and why, and what is still open.
Add it to the routing table above and follow the
[design directory rules](docs/writing.md#design-document-layout).

Write that file as HTML, not Markdown. Markdown is convenient to write and an
agent writes these now, so convenience is not the constraint; what matters is
that a person can read and review it, and HTML carries diagrams, real tables and
cross-links that Markdown cannot. Markdown stays where it earns its place: this
page, anything a build step parses, and anything appended to and reviewed by
diff. `docs/writing.md` has the rule in full.

The documents are layered on purpose, and that is what keeps the preloaded
context small. This page carries the model, the state and the invariants; each
file under `docs/design` carries one subsystem; each `package.scala` carries one
package; `docs/contributing.md` and its siblings are read only when you are
doing that particular thing. Put every fact at the level that needs it and link
to it from the others. A level that restates the level below costs tokens in
every session and drifts on its own schedule.

**The design has one standard: the whole stays coherent.** When a capability
arrives, the first question is where it belongs in the design that already
exists, not how to make it run. One that fits no existing package is a signal
that the architecture has to change, not a reason to open a package to hold it.
Never lower the design standard to finish a task, and never optimise for what is
convenient this week.

**Fix causes. Never patch the edge.** Swallowing an exception, turning a failure
into an empty result, adding a flag that routes around a defect, special-casing
at one call site: each of these takes a visible problem and makes it invisible
while the system keeps running wrong. The delete path is the live example —
an unreadable delete file currently yields an empty delete plan, so deleted rows
come back with no exception and no warning. That is not robustness.

**No stovepipes.** One class of problem gets one solution. The three
non-standard read entry points implement a single `SnapshotSource` and a single
`SegmentReader`; they do not each get their own path from top to bottom. When
you find yourself building a second parallel route, the first route is the thing
to change.

**Refactor when the design needs it, and never weigh the effort.** "That is too
big a change" is not a reason to keep a wrong design; effort is not an input to
a design decision. Exactly three reasons justify deferring: a decision has not
been made, a dependency does not exist yet, or a fact is not yet known. "It is a
lot of work" and "that is refactoring, not this task" are not among them.

**Removing is design work, and it is never silent.** Subtraction is what keeps
an architecture simple: the count of concepts, packages and options should hold
steady or fall as capabilities land, not climb. But never delete on your own
judgment. Before removing a file, a package, a public type, a configuration
option, a capability row or a test, say what you propose to remove, what
evidence says it is unused, and what breaks if that evidence is wrong — then
wait for a person to agree. "No references in this repository" is evidence, not
proof: reflection, service loading, user code outside this tree, documentation,
and the cloud jobs that pin this artifact all depend on things that grep as
dead.

**Explain the mechanism. No analogies, no metaphors, no invented shorthand.** A
technical question is answered by naming what actually happens: which file is
opened, which field is read, what the failure looks like on screen. An analogy
replaces that with something the reader has to translate back, and the
translation is where the misunderstanding gets in. Shorthand coined mid-sentence
is the same failure in a smaller package: it forces the reader to carry a
definition that exists nowhere else in the repository. Use the official name for
an official concept, define an abbreviation the first time it appears, and when
you reach for a comparison, describe the thing instead.

This is not a style preference. "The layer that fetches bytes, as opposed to
the layers that interpret them" names nothing a reader can check. "Reading one
table means opening five kinds of file — the snapshot JSON, the segment
manifest, a parquet footer, the delete files and the column group data, of which
the JVM opens four and the native library opens the fifth" answers the same
question and can be verified against the code. Write the second kind.
`docs/writing.md` has the full list of banned forms; it applies to explanations,
code comments and naming, not only to documents.

A documented interim state is not a patch. During a migration parts of the tree
will sit in the wrong module on purpose. That is legitimate when it is written
down, has a named end condition and someone is holding it; a patch is the one
you intend to leave there. `spark.read.plan` is the live example, and section
5 of modules.md says so.

## Rules any change has to satisfy

The full list is section 4 of [modules.md](docs/design/architecture/modules.md); these four
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
a `package.scala` does not exist in `capabilities.md`, when 实现位置 names a
package that does not exist on disk, or when a `package.scala` claims ids but
its directory holds no other source file. It reads ids only from the
`Capabilities: … (see docs/design/capabilities.md)` sentence, so "Storage V2"
in prose is not a claim of V2. It checks to the granularity of "code exists in
the package"; a package with code and an unfinished capability passes. Section 11 of capabilities.md is where a
capability with no home yet is declared, with the reason.

The other rows are on you. Those are the ones that rot, so they are what a
review should look at.

## Reading order

New to the repository: this file, then `README.md`, then
`docs/design/README.md`.

Planning work: `docs/design/capabilities.md` for what is in scope, section 4 of
`docs/design/README.md` for what is still undecided, section 5 of
`docs/design/architecture/modules.md` for where the code stands.

Writing code: the `package.scala` of the package you are touching, then section
4 of `docs/design/architecture/modules.md`, then [docs/contributing.md](docs/contributing.md).

Working on sbt: [docs/design/engineering/sbt.html](docs/design/engineering/sbt.html), then the
[spark-milvus-sbt skill](.agents/skills/spark-milvus-sbt/SKILL.md).
