# Working on the Milvus Spark Connector

Orientation for anyone opening this repository, human or agent. It holds no
content of its own: every fact lives in exactly one file, and this page says
which one. Follow the link rather than copying the text.

`CLAUDE.md` is a symlink to this file, so Claude Code and Codex read the same
page. Edit this one.

## What this project is

Spark reads and writes Milvus collections by going straight at the Milvus
storage format on object storage, not through the Milvus query path. A read
lists a snapshot, plans one Spark partition per segment, and pulls Arrow column
batches out of the segment files. A write produces segment files in the same
format and registers them back with Milvus.

Two lines exist. The 1.x line is frozen at tag `v1.6.0` and takes fixes only.
The 2.0 line is a rewrite on branch `refactor/v2`, versioned
`2.0.0-{branch}-{arch}-SNAPSHOT`. Everything below describes 2.0.

The build has eleven sbt modules in four layers, and dependencies only point
downward. [README.md](README.md) has the module table and the layer rules.

## Where things are written down

| Question | File |
|---|---|
| What does the connector commit to doing? | [docs/design/capabilities.md](docs/design/capabilities.md) — 51 capabilities with ids (R\*, W\*, C\*, A\*, V\*, K\*, O\*, G\*) that everything else references |
| How is it layered, and what is still undecided? | [docs/design/README.md](docs/design/README.md) — read path, write path, priorities, open decisions in section 4, decision log in section 6 |
| Which module and package does a thing belong to? | [docs/design/modules.md](docs/design/modules.md) — module table, package design, directory tree, build constraints in section 4, migration state in section 5 |
| How do we compare with the Lance Spark connector? | [docs/design/lance-spark.md](docs/design/lance-spark.md) |
| Illustrated versions of the above | [docs/design/overview.html](docs/design/overview.html), [docs/design/lance-spark.html](docs/design/lance-spark.html) |
| What options does a user pass? | [docs/reference-en.md](docs/reference-en.md), [docs/reference-cn.md](docs/reference-cn.md) |
| What is a given package responsible for? | The `package.scala` or `package-info.java` in that package. Every one names its main types and the capability ids it carries. |
| How do I build, test and run it? | [README.md](README.md) for the user-facing path, [docs/contributing.md](docs/contributing.md) for the mechanics on top |
| What sits outside this repository? | [docs/context.md](docs/context.md) |
| How should a document here be written? | [docs/writing.md](docs/writing.md) |

## Rules any change has to satisfy

The full list is [section 4 of modules.md](docs/design/modules.md); these four
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
   section 6 of `docs/design/README.md` with its reason, including the reasons
   considered and rejected. A choice still open belongs in section 4 and nowhere
   else.

Two habits that follow from the same idea:

- **No compatibility shims.** When code moves, update the call sites. A
  forwarding stub hides the move and outlives the migration.
- **Say when a document is wrong.** The design documents were written before the
  code was read, and the migration has already corrected several of their
  claims. Correcting a document is part of the change, not a follow-up.

## Language

Code, comments, build scripts and `README.md` are English. The design documents
under `docs/design` are Chinese. Do not mix within a file.

## Reading order

New to the repository: this file, then `README.md`, then
`docs/design/README.md`.

Planning work: `docs/design/capabilities.md` for what is in scope, section 4 of
`docs/design/README.md` for what is still undecided, section 5 of
`docs/design/modules.md` for where the code stands.

Writing code: the `package.scala` of the package you are touching, then section
4 of `docs/design/modules.md`, then [docs/contributing.md](docs/contributing.md).
