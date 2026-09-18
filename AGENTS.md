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

Since 2026-09-17 the project also covers reading open formats directly
(Parquet, Lance, Iceberg, Hudi, each read by its own rules) and computations
such as K-means over any input, combined as the composition principles below
describe. Neither part is designed or written yet.

Two lines exist. The 1.x line is frozen at tag `v1.6.0` and takes fixes only.
The 2.0 line is a rewrite on branch `refactor/v2`, versioned
`2.0.0-{branch}-{arch}-SNAPSHOT`. Everything below describes 2.0.

## The four layers

Twelve sbt modules, and dependencies only point downward. This is the model
every rule below refers to.

| Layer | Modules | What lives there |
|---|---|---|
| 1 | `native-runtime`, `native-storage`, `native-vector` | Storage uses milvus-storage's upstream JNI and Java/Scala API; vector library loading through Knowhere's upstream C/JNI and Java API. `native-runtime` verifies and extracts the unified platform bundle for both bindings. Nothing above this layer loads a `.so`. |
| 2 | `core`, `compat`, `client` | The Milvus storage format and the client for the online service. All computation happens here. No Spark: a source file mentioning `org.apache.spark` fails the build. |
| 3 | `spark-base`, `spark-3.5`, `spark-4.0`, `spark-4.1`, `spark-4.2` | The DataSource V2 surface. `spark-base` is a shared source directory, not a project; each line project compiles it against its own Spark, Arrow, antlr and Java version. |
| 4 | `apps-4.0` | The jobs users run: backfill and vector search. |

`integration-4.0` sits outside the layering and outside root's aggregate: its
suites need a live Milvus and MinIO.

Only layer 3 splits per Spark line, because the `TableCatalog` and
`ParserInterface` method sets differ there, while Arrow, antlr and the Java
target are also pinned per line. Each `spark-<line>` declares an `assembly`
task, not another module. During migration, the usable fat jar is still root's
`assembly`: its merge and shading rules are not yet wired into the per-line
tasks. [README.md](README.md) has the full module table.

## Where the work stands

The design documents contain both implemented contracts and the remaining 2.0
target. Capability rows, package documentation and the explicit gap lists below
state which side of that boundary each feature is on.

Done: the 1.x sources are all in their modules and `src/` no longer exists.
`core` has schema, codec, snapshot, manifest, delete, path,
credential and `io.ObjectStore` over the native filesystem; `compat` has the V2
packed and backup entry points; `client` has the RPCs required by the currently
implemented paths, including Catalog discovery and collection/index DDL.
Every driver-side read opens storage through that one store, and no source file
in `core` or `compat` mentions `org.apache.hadoop`.

`core.index` executes vector queries through Knowhere: `SearchPlan` cuts a
search into tasks, `SegmentSearch` runs a query group over a segment set by
exact scan or index probe, and `TopKMerger` keeps each query's best k.
`core.expr` holds two explicit scalar contracts: `Expr` / `PlanParser` /
`Evaluator` for Milvus syntax used by ordinary table and persisted-index
filters, and the schema-bound `PredicateExpr` / `PredicateEvaluator` / `Bitmap`
for Spark V2 predicate pushdown. JSON/Array Milvus expressions wait on decision
23. Index writing remains unwritten; the vector search design moves both modes
behind one query-set entry and deletes the two older brute-force paths when that
entry lands. `stats` writes
primary-key bloom filters and reads V2/V3 segment statistics for conservative
R9/R18 segment pruning; R10 row-group pruning remains blocked on
milvus-storage. `write.commit` holds job manifests and registration.
`read.plan` builds tasks and lists delete files; `read.exec` reads them on the
executor and opens the segment reader.

Layer 3 is split by package: `sources` holds only the `format("milvus")`
entry point, `table` the table, `read` the scan builder, the scan and the
executor-side readers,
`write` the two writers, `options` the option parsing, the driver's storage
access and the choice of `SnapshotSource` for a read, `types` the type
mapping. DataSource `getTable` and Catalog `loadTable` share `MilvusTables`,
which resolves the `Snapshot` once before the table carries it to the scan;
partition planning is in `core.read.plan`. `catalog`
implements three-part table loading and latest/name/timestamp
snapshot selection. It also maps Milvus databases to one-level Spark
namespaces and collections to tables for `SHOW NAMESPACES` and `SHOW TABLES`;
`CREATE TABLE` validates the Spark schema and Milvus table properties before it
creates a collection and its vector indexes, while `DROP TABLE` preserves
Spark's confirmed-absence result. Namespace mutation, table alteration and
rename remain unsupported. `expr` translates supported DataSource V2
predicates into core `PredicateExpr` values and leaves each unsupported
predicate tree with Spark. Names follow the Names
section of [docs/writing.md](docs/writing.md): the two storage lines are `V2`
and `V3` everywhere, after the snapshot's `storage_version`.

`procedure` owns the driver-side bodies behind the shared
`CALL milvus.system.<name>(...)` SQL extension. Snapshot, index,
load/release/flush/compact, collection describe, and backfill register are
implemented across all four Spark lines. Append registration still waits for a
Milvus `RegisterSegments` API. Staging cleanup now records collection ownership
and driver heartbeats, audits stale unregistered append jobs fail-closed, and
deletes their file objects; removing the remaining directory entries still
waits for milvus-storage to expose recursive directory deletion.

Layer 1 uses the upstream milvus-storage JNI and Java/Scala API.
`native-storage` compiles the pinned submodule's API for Scala 2.12 and 2.13
and hands the selected JNI path to its upstream loader; it does not maintain a
second JNI implementation. `native-runtime` validates and extracts the unified
platform JAR that supplies `libmilvus-storage-jni` and its dependencies. Without
a unified bundle, the upstream loader retains its packaged-resource and system
library fallback. Upstream additions, ownership rules and current validation
results are in [storage-io.html](docs/design/architecture/storage-io.html#state).
`native-vector` compiles the pinned `knowhere` submodule's Java API and
integrates its loader and BruteForce implementation; Knowhere owns the C
interface, JNI, Java API and native resource loader. The submodule follows the
PR #1829 branch while the superproject gitlink fixes the exact source revision.
Persisted HNSW loading uses upstream BinarySet and index search APIs; Cardinal
stream files require a Cardinal-enabled build. Real-file compatibility and
validation results are recorded in the vector search design.

Five design questions are still open: 10, 19, 22, 23 and 26 in
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
| How does layer 3 turn Spark calls and options into cross-layer contracts? | [docs/design/architecture/spark-interface.html](docs/design/architecture/spark-interface.html) — shared package responsibilities, typed option delivery, task resource ownership, delivery order and acceptance gates |
| How does Spark predicate pushdown preserve semantics? | [docs/design/architecture/expressions.html](docs/design/architecture/expressions.html) — the DataSource V2 support matrix, residual contract, three-valued logic, field-id binding, hidden predicate columns and row/columnar execution |
| How do Catalog discovery, table DDL and fixed-snapshot loading work? | [docs/design/architecture/catalog.html](docs/design/architecture/catalog.html) — one-level namespaces, table listing, identifier and property rules, CREATE/DROP sequencing and failure semantics, latest/version/timestamp selection and HybridTS conversion |
| How does a write run, and what is still missing at the entry point? | [docs/design/architecture/write.html](docs/design/architecture/write.html) — the DataSource V2 write chain, where the write table gets the collection schema, the WriteBuilder checks, the three things a segment still lacks before registration, the development outline |
| How does a `CALL milvus.system.<name>(...)` statement become a call? | [docs/design/architecture/procedure.html](docs/design/architecture/procedure.html) — the grammar, parser extension, logical node and strategy, per-line generated pieces, procedure contracts and bounded-wait semantics |
| How is any input described at a fixed version, and how does it reach reads, search and index building? | [docs/design/architecture/table-version.html](docs/design/architecture/table-version.html) — decision 25 as revised on 2026-09-17: `TableVersion` (evolved from today's `Snapshot`) has a common part and a part only its format's `TableFormat` reads; its units are `DataUnit`s; identity stays in each input's `StorageBinding`; deletes run inside each format; row addresses and capabilities are declared; computations take neutral column batches from a table input or a scan-only DataFrame input. `Snapshot`, `Segment` and `SegmentReadTask` name Milvus objects only |
| How do vector search and index building work? | [docs/design/architecture/vector-search.html](docs/design/architecture/vector-search.html) — conclusion: one `MilvusSearch.search` entry taking a query set with `mode` exact or index, its result schema, scope, options, capabilities checked at planning and failure behaviour, layer duties with the computation kept apart from the Milvus `TableFormat` side, metrics and the cost of each mode; principle: the two Spark stages (candidates then take), the Knowhere buffer contract and zero-copy conditions, exact scan, index probe from snapshot metadata, index file decoding and engine choice, result semantics, index building through `build_index` and external snapshot restore, native library loading and real-file compatibility; then what the simplicity and the design buy |
| How is an external collection read? | [docs/design/architecture/snapshot.html](docs/design/architecture/snapshot.html#external) section 3.1 — how Milvus stores one, the five source formats, synthesized primary key and timestamp; the four read rules every segment shares and the source type table are in [read.html](docs/design/architecture/read.html#rules) 1.1 and 6.4; customer-bucket `extfs.*` credentials in [storage-auth.html](docs/design/architecture/storage-auth.html#external) 3.4. Opening an external segment needs milvus-storage `loon_reader_new` to accept a null schema |
| What is a Snapshot, and how do the four read entry points become one? | [docs/design/architecture/snapshot.html](docs/design/architecture/snapshot.html) — `Snapshot` and `Segment`, the three delete states, what each source cannot supply, `SnapshotCatalog`, the boundary to `SegmentReadTask`. Draft under review |
| How does backfill reach more than one bucket? | [docs/design/apps/backfill-storage.html](docs/design/apps/backfill-storage.html) |
| Illustrated version of the above | [docs/design/architecture/overview.html](docs/design/architecture/overview.html) |
| What options does a user pass? | [docs/reference-en.md](docs/reference-en.md), [docs/reference-cn.md](docs/reference-cn.md) |
| What is a given package responsible for? | The `package.scala` or `package-info.java` in that package |
| How do I build, test and run it? | [README.md](README.md), then [docs/contributing.md](docs/contributing.md) for the mechanics on top |
| How do we review or change the sbt build? | [docs/design/engineering/sbt.html](docs/design/engineering/sbt.html) for principles and practice; apply the repository skill [.agents/skills/spark-milvus-sbt/SKILL.md](.agents/skills/spark-milvus-sbt/SKILL.md) for build work |
| How does CI run the live-service integration suite through Helm? | [docs/design/engineering/integration-tests.html](docs/design/engineering/integration-tests.html) — one local-mode Spark Job, external Milvus and object storage, Secret references, lifecycle and acceptance gates |
| Why and how should storage and Knowhere share native dependencies? | [docs/design/engineering/native-libraries.html](docs/design/engineering/native-libraries.html) — rationale, benefits and maintenance costs of an independent CMake build; pins every upstream Conan recipe revision in `native-build/dependencies.json`, selects the newer conflicting dependency versions, declares integration link relationships in CMake, and packages one native JAR with shared extraction; the historical Knowhere `9dc2b8ad` Cardinal bundle built with the superseded recipe implementation passed native and real-data validation, while the current `1fff20db` gitlink requires a rebuilt bundle and fresh validation |
| What must run before every commit? | Apply [.agents/skills/spark-milvus-sbt/SKILL.md](.agents/skills/spark-milvus-sbt/SKILL.md); [formatting](docs/contributing.md#formatting) and [unit tests](docs/contributing.md#unit-tests) in the contributing guide hold the required commands and constraints |
| How does the Knowhere C ABI and JNI binding work? | [docs/design/architecture/knowhere-jni.html](docs/design/architecture/knowhere-jni.html) — binding interfaces and memory ownership, the capability matrix against the Knowhere core and the connector, the thread model; the Cardinal Aquila items are deferred |
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

**A job is a composition of independent choices.** Each input a job reads is
bound to a location, the identity that opens it, and a format. The job picks a
computation. Each output is bound to its own location, identity and format. A
job may read several inputs with different bindings. A new scenario is
supported by changing one of these choices; no code path is written for one
particular combination. Searching a query set in a customer's bucket against a
collection in ours is the ordinary search given two inputs.

The three parts own separate concerns. Access decides where the data is and
which identity opens it. Format turns files into valid data (schema, version,
deletes, merges) and, when a computation asks for them, index information and
row locations. Computation consumes the columns and vectors it needs; it never
sees a storage address, a credential or a format. Changing the storage or the
identity changes only the access binding, changing the format changes only how
the files are read, and a new computation reuses both. These parts are not
modules: the four layers stay as they are.

**An identity is bound to an input or an output.** Two inputs in the same
bucket behind the same endpoint may use different identities. The order the
inputs are read in, concurrency and task retries leave each input with the
identity it was bound to. Rewriting the session's shared Hadoop configuration to
switch identity binds nothing. Hadoop's per-bucket keys remain a way to derive
an input's identity when its options name none, and they never override an
identity the input states.

**A format is the set of rules the data is read by; the file type does not
decide it.** Parquet or Lance files registered as a Milvus external collection
are Milvus data, like a snapshot or a milvus-backup export: Milvus schema and
types, system fields, the synthesized primary key, the snapshot version, Milvus
delete rules and Milvus indexes apply. The same files read directly are an open
format: that format's own version, schema, deletes and row addresses apply, and
no collection id, Milvus system field or Milvus type is required. The two paths
do not depend on each other and do not share a row identity.

**Capabilities are declared and checked, never inferred.** Reading a format,
using its index and fetching a row by its address are three capabilities. A
computation states which it requires, an input states which it provides, and a
mismatch fails when the job is planned. A format's name and the data's location
imply no capability: a Lance dataset with a vector index does not thereby hold
an index Knowhere can load.

**The target composition is agreed before the implementation is split.** Which
combinations the project supports is decided first, and the design states which
of them work today. Which of Spark data sources, milvus-storage, DataFrames and
the table model carries each part is decided afterwards, by evolving the
existing types: the table model, `TableVersion` (today's `Snapshot`), evolves,
and no second table model is added beside it.

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

Use imports and short type or object names in code; follow the
[import rules](docs/contributing.md#imports) instead of writing fully qualified
names at call sites or in type annotations.

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
