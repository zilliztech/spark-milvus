# Context outside this repository

The design does not stand on its own. These are the sources it depends on, with
what each one settles. None of their content is copied into this repository;
follow the pointer when you need it.

## Submodules in the tree

| Path | What it is | Why it matters here |
|---|---|---|
| `milvus-proto` | The Milvus protobuf definitions | `common.proto` and `schema.proto` are generated into `core`, the five service files into `client`. The storage format's schema *is* `schema.proto`, so core does not build a parallel model of it. |
| `milvus-storage` | The native storage library, also known as Loon | Supplies the `loon_*` C interface that `native-storage` wraps, and ships its own Python bindings, which is why Ray does not need anything from us for the storage half. |

## Repositories expected alongside this one

These are checked out next to `spark-milvus`, not vendored. Paths are relative
to the parent directory.

| Repository | What it settles |
|---|---|
| `milvus` (milvus-io/milvus) | The authority on what the write path can register. Reading DataCoord's RPC surface is how we established that backfill can ship on `BatchUpdateManifest` today, while append needs a new `RegisterSegments` RPC that does not exist yet. Also the authority on the on-disk delete-file format. |
| `lance-spark` | The connector we compare against layer by layer. The comparison is written up in `docs/design/lance-spark.md`; the repository is where to check a claim about it. |

## Design decks

Three HTML decks live outside the repository, in a sibling
`spark-milvus-design-docs` directory. They are the product-level framing that
`docs/design` implements:

| Deck | Subject |
|---|---|
| `vector-batch-platform.html` | The Vector Lakebase framing: where batch vector processing sits in the product |
| `spark-milvus-deck.html` | The original connector pitch |
| `spark-milvus-deck-v2.html` | The current connector pitch, which `docs/design` builds on |

They are not under version control here, so treat them as background rather than
as a citable source. When a deck and `docs/design` disagree, `docs/design` is
what the code follows, and the disagreement is worth raising.

## Downstream consumers

Zilliz Cloud runs this connector as a set of Spark batch jobs. The publish
coordinate `com.zilliz:spark-connector_2.13` is pinned there, which is why the
2.0 root project keeps producing a fat jar under the 1.x name even though every
source file now lives in a module.

Backfill is the only one of those jobs that writes to a collection. That is why
the write path's registration story is settled for backfill and still open for
append.
