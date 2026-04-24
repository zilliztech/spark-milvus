# Milvus Storage Versions — Reference

Authoritative definitions of `storage_version` as used by Milvus
segment-info, plus the naming conventions used in this connector. If anything
below disagrees with `milvus/internal/storage/rw.go`, `rw.go` wins.

> **TL;DR:** there are three on-disk formats — V1 (binlog), V2 (packed
> parquet, no manifest), V3 (packed parquet with AVRO manifest). The name
> "V2" is overloaded: the milvus-storage C++ library calls its own
> manifest-based format "format v2", which **is the server's V3**. Always
> clarify which "V2" you mean.

---

## The segment-info enum (source of truth)

From `milvus/internal/storage/rw.go`:

| Value | Name       | On-disk layout |
|-------|------------|----------------|
| **0** | StorageV1  | Row-oriented binlog. One binlog file per field per segment under `insert_log/{coll}/{part}/{seg}/{fieldID}/{logID}`. Each field has its own schema; no column grouping. |
| **1** | *(unused)* | No segment has this value. |
| **2** | StorageV2  | Packed parquet, **no external manifest**. Column groups live at `insert_log/{coll}/{part}/{seg}/{slot}/{logID}` where `slot` is the column-group slot (a single-field group's slot is the field ID; a multi-field group's slot is the smallest unused int `< 100`). Layout is recovered at read time by combining (a) the per-segment AVRO from the snapshot's `manifest_list`, which gives `slot → file paths`, with (b) any one parquet file's footer KV-metadata `group_field_id_list`, which gives `slot → real field IDs`. |
| **3** | StorageV3  | Packed parquet **with** external loon manifest at `{basePath}/_metadata/manifest-<ver>.avro`. The manifest alone describes column groups, file paths, and row counts — no snapshot-side AVRO decode needed. Written and read via milvus-storage's `loon_reader_new` / loon transaction FFI. Listed separately in the snapshot JSON under `storagev2_manifest_list`. |

Parquet files in V2 and V3 share the same footer schema and KV trio
(`row_group_metadata`, `storage_version = "1.0.0"`, `group_field_id_list`).
Only the **manifest layer** differs — V2 has none, V3 has an AVRO file.

---

## Two "V2"s (naming trap)

| Context | "V2" means |
|---------|-----------|
| Milvus segment-info enum | `storage_version = 2` — non-manifest packed parquet |
| `milvus-storage` C++ library internals | Its own on-disk format "version 2" — which actually corresponds to the server's **V3** (manifest-based) |
| Parquet footer KV `storage_version = "1.0.0"` | Yet another versioning namespace (milvus-storage's parquet-level format string) |

Consequences:

- A class called `MilvusLoonXxxV2` may handle V3 (library sense) — **check
  the class comment for which "V2" it uses**.
- The snapshot JSON wire key `storagev2_manifest_list` carries V3 manifests.
  This is a historical mis-name baked into the datacoord snapshot writer;
  cannot be renamed from the connector alone.

---

## How segments are surfaced in the connector

### Snapshot mode (offline, no Milvus client)

A snapshot JSON produced by milvus-datacoord has two separate arrays:

- `manifest_list` — per-segment AVRO paths. These can contain **V1 or V2**
  segments; `MilvusSegmentManifestReader` decodes each AVRO and branches on
  the inner `storage_version` field.
- `storagev2_manifest_list` — array of `StorageV2ManifestItem`. These are
  **V3** (despite the key name). Each carries a `basePath` and a `ver` that
  locates the loon manifest.

`MilvusDataSource.planInputPartitionsFromSnapshot` dispatches:

| Source                          | InputPartition                      | Reader                            |
|---------------------------------|-------------------------------------|-----------------------------------|
| `storagev2_manifest_list` items | `MilvusStorageV3InputPartition`     | `MilvusLoonPartitionReader`       |
| `SnapshotV2Segments` option     | `MilvusPackedV2InputPartition`      | `MilvusPackedV2PartitionReader`   |

Both sources can coexist (mixed-version snapshot).

### Client mode (online, via gRPC `getPersistentSegmentInfo`)

`MilvusClient.getSegments` returns `MilvusSegmentInfo` with a
`storageVersion: Long` field reflecting the segment-info enum.

**As of 2026-04-24 the client-mode planner does NOT distinguish V2 from V3.**
It filters `storageVersion >= 2` and routes every match to the V3 reader. A
collection containing real V2 segments will therefore fail to read in client
mode. See the "Client-mode V2 dispatch" task tracked separately.

---

## Connector class / option map (post-rename, 2026-04-24)

| Symbol                                 | Handles            |
|----------------------------------------|--------------------|
| `read/MilvusStorageV3InputPartition`   | V3 (loon manifest) |
| `read/MilvusPackedV2InputPartition`    | V2 (non-manifest)  |
| `read/MilvusLoonPartitionReader`       | V3                 |
| `read/MilvusPackedV2PartitionReader`   | V2                 |
| `read/MilvusSegmentManifestReader`     | Decodes per-segment AVROs; produces `AvroManifestEntry` with inner `storageVersion` field (can be 0/2/3 — 2 is then fed into `V2SegmentLoader`; 3 is exposed via `storagev2_manifest_list`; 0 is V1 and not supported for backfill) |
| `read/MilvusParquetFooterReader`       | Reads `storage_version`/`group_field_id_list`/`row_group_metadata` from parquet footer KV (used only for V2 — V3 learns the same info from the loon manifest) |
| `read/V2SegmentLoader`                 | Turns `AvroManifestEntry` + parquet footer into a `V2SegmentInfo` |
| `read/V2SegmentInfo` / `V2ColumnGroup` | V2 runtime view    |
| `write/MilvusLoonWriter`               | V3 writer (FFI transaction) |
| `write/MilvusV2BinlogWriter`           | V2 writer (direct `AvroParquetWriter` per field) |
| `operations/backfill/V2SegmentArtifact` | V2 backfill output, consumed to patch snapshot AVRO |
| `operations/backfill/SegmentBackfillResult.committedVersion` / `.manifestPaths` | V3 backfill output (new manifest version) |
| JSON wire key `storagev2_manifest_list` | V3 manifest list — wire name is historical, frozen |
| JSON wire key `manifest_list`            | Per-segment AVRO paths (V1 or V2) |
| Spark option `milvus.snapshot.manifests` | Serialized `Seq[StorageV2ManifestItem]` — i.e. V3 |
| Spark option `milvus.snapshot.v2.segments` | Serialized `Seq[V2SegmentInfo]` — i.e. V2 |

---

## Decision flow for new code

When you need to reason about storage versions, answer these in order:

1. **Which layer am I at?**
   - Server segment-info enum (`storageVersion` on `MilvusSegmentInfo`,
     `AvroManifestEntry.storageVersion`) → use the **V1/V2/V3** mapping above.
   - milvus-storage C++ format name → use "library format v2" = server V3.
     Don't mix the two.
2. **Do I have a manifest file?**
   - Yes, at `_metadata/manifest-*.avro` → V3. Use `MilvusLoonPartitionReader`
     / `MilvusLoonWriter`.
   - No → V2. Use `MilvusPackedV2PartitionReader` /
     `MilvusV2BinlogWriter`.
3. **Am I planning from a snapshot or a live Milvus client?**
   - Snapshot: look at both `manifest_list` (V1/V2) and
     `storagev2_manifest_list` (V3).
   - Client: beware — planner does not currently dispatch V2 vs V3
     (see separate task).

## Pitfalls

- **Don't trust a class name ending in `V2`** — read the class comment.
  Several "V2" names still refer to V3 for historical reasons.
- **Don't derive the column-group slot from the field IDs inside the file.**
  For multi-field groups the slot is a small int allocator's output, not a
  field ID. For single-field groups it happens to equal the field ID —
  backfill exploits this invariant but general code shouldn't.
- **Don't rename `@JsonProperty("storagev2_manifest_list")`** on the
  deserialize-only `SnapshotMetadata` case class — the wire key is produced
  by milvus-datacoord and renaming it here silently drops V3 parsing.
- **V1 segments are read-only to this connector.** Backfill does not write
  binlog format; a collection that still has V1 segments must be flushed to
  V2/V3 before backfill runs.
- **Parquet footer `storage_version = "1.0.0"`** is milvus-storage's
  parquet-level format version, *not* the segment-info enum. Both V2 and V3
  parquet files carry the same footer value; it is not a discriminator.
