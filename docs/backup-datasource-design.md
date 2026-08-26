# Design Doc — milvus-backup as a Milvus Read Data Source (spark-milvus)

**Status:** Implemented
**Audience:** Milvus / Spark connector / storage-v2 engineers
**Last updated:** 2026-08-25

## 1. Background & Goals

Milvus 2.6 deployments that cannot use the server snapshot feature (or that want
a fully offline, client-free read path) currently have no way to read data
through the connector without a live Proxy connection. `milvus-backup create`
already produces a byte-identical, point-in-time copy of the flushed binlogs,
but the connector had no way to consume it.

This change adds a **backup datasource mode**: point the connector at a
binlog-format milvus-backup export and read it as a Spark DataFrame with the
existing StorageV2 packed-parquet read stack — no Milvus client connection
required.

**Goals**

- Input: a backup directory (`milvus.backup.dir`, e.g. `s3a://bucket/backup/<name>`
  or a local path) produced by `milvus-backup create`.
- Output: a Spark DataFrame supporting column pruning, `milvus.extra.columns`
  (`partition`, `$segment_id`, `$row_offset`), and the same delete semantics as
  snapshot reads (`milvus.read.apply.deletes`).
- Reuse the existing StorageV2 packed read path (`MilvusPackedV2PartitionReader`
  + the milvus-storage JNI reader) unchanged.
- **No changes to milvus-backup**: existing exports work as-is.

**Out of scope**

- StorageV3 (loon manifest) segments.
- Write / backfill flows and the client snapshot fast path.
- Snapshot-format backups (`format == "snapshot"`), which bundle a different
  directory layout and are rejected explicitly.

**Core gap and the chosen approach**

milvus-backup's meta records only `log_size` per binlog, not `entries_num`
(per-file row count). The packed-V2 reader requires exact per-file row counts to
derive the `(start_index, end_index)` ranges for each file; missing or wrong
counts are rejected or silently mis-read. **Approach A**: recover each file's
row count by reading its parquet footer at planning time. This requires no
backup-side changes and works on any existing export.

## 2. Verified Key Facts

| Fact | Source |
|---|---|
| Backup binlog objects are byte-identical copies of Milvus storage objects | `milvus-backup/core/backup/coll_dml_task.go` (`CopyObjectsTask`) |
| Backup layout `binlogs/insert_log/{coll}/{part}/{groupID}/{seg}/{field}/{log}`; `groupID` is a virtual partition for restore only | `milvus-backup/internal/storage/mpath/path.go` |
| Full metadata lives in `meta/full_meta.json` (schema + partitions + segments incl. L0) | `milvus-backup/internal/meta/meta.go`, `meta_builder.go` |
| `SegmentBackupInfo` carries id / `num_of_rows` / `storage_version` / `group_id` / `is_l0` / `binlogs` / `deltalogs` | `milvus-backup/core/proto/backup.proto` |
| `Binlog` records only `log_path/log_size/log_id`; `entries_num` exists but is deprecated and unset | `backup.proto`; `coll_dml_task.go` |
| Packed read requires exact `fileRowCounts` | `MilvusPackedV2PartitionReader.scala`; `v2_column_groups_builder.h` |
| Real field IDs are recoverable from each parquet file's own schema (`PARQUET:field_id`) | `MilvusParquetFooterReader.readFieldIdsFromSchema` |
| Delta-log decoding uses only `logPath`; `entriesNum` is unused | `MilvusDeltaLogReader.scala` |
| L0 (delete-only) segments have no column groups; they feed partition-scoped inherited delete plans | `MilvusDataSource.scala` |

## 3. Overall Design

A new **backup offline mode** sits alongside snapshot mode:

```
MilvusDataSource / MilvusTable  isBackupMode? ─┐
                                              ▼
MilvusScan.computeInputPartitions ──> planInputPartitionsFromBackup()
                                              │   via BackupMetaReader
                                              ▼
                           (schemaBytes, Seq[V2SegmentInfo])
                                              │   reuse
                                              ▼
                    buildSnapshotPartitions() → MilvusPackedV2InputPartition[]
                                              │
              createReaderFactory() → MilvusPackedV2PartitionReader (unchanged)
```

Branch precedence: snapshot mode > backup mode > client mode.

Two gaps versus a Milvus snapshot are closed in `BackupMetaReader`:

1. **Per-file row counts** (`fileRowCounts`) — recovered from each parquet
   footer via the new `MilvusParquetFooterReader.readRowCount` (head file read
   once via `readFieldIdsAndRowCount`, remaining files read in parallel).
2. **Slot → real field ID mapping** — the AVRO segment-info is not copied by
   the backup, so real field IDs are recovered from each parquet file's own
   schema via the existing `readFieldIdsFromSchema`.

Path resolution: the `log_path` values in `full_meta.json` are the **original
Milvus source keys** — milvus-backup copies each binlog into a separately
computed `DestKey` under the backup dir and records only the source key in the
meta (`coll_dml_task.go` asserts the two differ). Backup object paths are
therefore **reconstructed** from `milvus.backup.dir` plus the segment's
collection/partition/group/segment/field/log IDs, mirroring
`insertLogsAttrs`/`deltaLogAttrs`:

```
insert: {backupDir}/binlogs/insert_log/{coll}/{part}/{group}/{seg}/{field}/{log}
delta:  {backupDir}/binlogs/delta_log/{coll}/{part}/{seg}/{log}          (part == -1)
        {backupDir}/binlogs/delta_log/{coll}/{part}/{group}/{seg}/{log}  (part != -1)
```

The `groupID` level is always present for `insert_log` (it is the virtual
partition id; `0` for `partition_id == -1`) and present for L1 `delta_log`
only when `partition_id != -1`. Two path forms are produced:

- **Qualified** (`s3a://bucket/backup/b1/binlogs/...`) for the Hadoop-side
  reads (parquet footers, delta logs, meta).
- **Bucket-relative keys** (`backup/b1/binlogs/...`) for the milvus-storage
  native packed reader: its `FilesystemCache::resolve_config` rejects
  scheme-qualified URIs (demanding `extfs.*` config), and the filesystem proxy
  prepends `fs.bucket_name`. The connector therefore canonicalizes
  `fs.bucket_name` to the backup URI's bucket for the native reader.

## 4. Changes

### 4.1 `src/main/scala/MilvusOption.scala`

- New option `MilvusOption.BackupDir = "milvus.backup.dir"`.
- New helpers `backupDir(options)`, `isBackupMode(options)`, and
  `validateBackupModeOptions(options)` — backup mode and snapshot mode are
  mutually exclusive.

### 4.2 `src/main/scala/read/BackupMetaReader.scala` (new — core)

Parses a binlog-format backup's `meta/full_meta.json` (wire keys match the Go
`encoding/json` tags of `backuppb`) and exposes:

```scala
object BackupMetaReader {
  def metaPath(backupDir: String): String          // <dir>/meta/full_meta.json
  def readMeta(hadoopConf: Configuration, backupDir: String,
               maxBytes: Long = MaxSnapshotJsonBytes): Either[Throwable, BackupInfo]
  def parse(json: String): Either[Throwable, BackupInfo]
  def toProtobufSchemaBytes(schema: BackupCollectionSchema): Array[Byte]
  def toV2Segments(info: BackupInfo, hadoopConf: Configuration, backupDir: String,
                   applyDeletes: Boolean, collectionId: Long): Either[Throwable, Seq[V2SegmentInfo]]
}
```

Embedded JSON model mirrors `backuppb.BackupInfo` → `CollectionBackupInfo` →
`PartitionBackupInfo` / `SegmentBackupInfo` → `FieldBinlog`/`Binlog`, plus
`CollectionSchema`/`FieldSchema` (a mirror of Milvus `schemapb`, including
`type_params`, `data_type`, `is_primary_key`, `element_type`, `is_dynamic`,
`nullable`, and `default_value_base64`).

Behavior:

- Rejects snapshot-format backups and non-L0 segments whose `storage_version`
  is not `2` (StorageV1/V3) — both fail loudly rather than producing a partial
  dataset.
- Skips L0 segments when `applyDeletes = false` (otherwise L0 segments, which
  Milvus creates without a storage version, bypass the V2 filter and feed the
  inherited delete-plan path).
- L0 segments produce a `V2SegmentInfo` with empty `columnGroups` plus
  `deltaLogs`, feeding the inherited delete-plan path.
- Fails hard for a StorageV2 data segment with rows but no binlogs (would
  otherwise silently drop rows), and for dynamic collections whose meta lacks
  the `$meta` field (default backups; points at `--backup_index_extra`).
- `readMeta` reads `full_meta.json` with a bounded reader
  (`milvus.snapshot.max.json.bytes`, default 64 MiB) and caches the parsed
  `BackupInfo` per backup dir so table init and scan planning read it once.

### 4.3 `src/main/scala/read/MilvusParquetFooterReader.scala`

- New `readRowCount(path, hadoopConf): Either[Throwable, Long]` — sums the
  row groups' row counts from the parquet footer (a `HEAD` + a single tail
  `GET`, same cost profile as the existing footer reads).
- New `readFieldIdsAndRowCount(path, hadoopConf)` — field IDs + row count from
  a single footer open (avoids opening the head file twice).

### 4.4 `src/main/scala/sources/MilvusDataSource.scala`

- `MilvusDataSource.getTable` / `inferSchema`: allow backup mode without
  `milvus.uri`; enforce snapshot/backup mutual exclusion; return an empty
  schema from inference (callers supply `.schema()`).
- `MilvusTable`: `isBackupMode`, `initFromBackup()` (materializes the collection
  schema and collection id from the backup meta — matched by
  `milvus.collection.name` — so metadata rehydration for vector columns works
  like snapshot mode), and `schema()` handling for offline modes.
- `MilvusScan.planInputPartitionsFromBackup()`: resolves the collection by
  `milvus.database.name` + `milvus.collection.name` (`resolveBackupCollection`,
  ambiguous names rejected), rejects partition/segment selectors, validates
  that the meta carries a collection schema with a primary key, builds
  `V2SegmentInfo`, loads delete plans, and hands everything to the shared
  `buildSnapshotPartitions` with `inlineInheritedDeletePlans = true`.
- Shared `buildSnapshotPartitions` dedups each segment's column groups by slot
  (`V2SegmentInfo.dedupColumnGroupsBySlot`) so a field carried by an old
  multi-field group and a newer single-field group (add-field + backfill) is
  read from the newest owner — the same gap the snapshot read path had.
  `MilvusBackfill.dedupColumnGroupsBySlot` delegates to the same method, so the
  rule has a single implementation.
- `MilvusScan.pushFilters`: backup mode returns all filters as unsupported
  (the packed-V2 reader has no filter pushdown), matching the packed-V2 snapshot
  path.
- `MilvusScan.buildSnapshotHadoopConf` refactored into the companion
  `buildHadoopConfForOptions(rawOptions, path)` so both the planner and table
  schema rehydration share it. `snapshotBucket` now treats non-S3 schemes as
  "no bucket" (so `file://` backup dirs don't raise a snapshot-flavoured error).

## 5. Data Mapping (`full_meta.json → V2SegmentInfo`)

| `V2SegmentInfo` | Source | Notes |
|---|---|---|
| `segmentId` | `SegmentBackupInfo.segment_id` | |
| `partitionId` | `.partition_id` | `-1` preserved for L0 / all-partition |
| `numOfRows` | `.num_of_rows` | |
| `storageVersion` | `.storage_version` | L0 handled before this; non-L0 must be `== 2`, else the read fails hard |
| `columnGroups` | `.binlogs[]` grouped by `fieldID` (slot) | slot = directory name |
| `cg.fieldIds` | head file of each group via `readFieldIdsAndRowCount` | |
| `cg.filePaths` | reconstructed from `backupDir` + IDs (never the meta `log_path`), **bucket-relative** for the native reader | `insert_log` carries the groupID level; sorted by `log_id` |
| `cg.fileRowCounts` | head via combined read, rest via parallel `readRowCount` | gap-closer; `size == paths.size` enforced |
| `cg.slotFieldId` | group's `fieldID` | used for slot-based dedup |
| `deltaLogs` | reconstructed `delta_log` paths from `backupDir` + IDs (**qualified**, Hadoop-side) | `entriesNum = 0` (unused by the decoder) |
| L0 segment | `is_l0 = true` → empty `columnGroups` + `deltaLogs` | inherited delete-plan path; Milvus creates L0 without a storage version, so it bypasses the V2 filter |

## 6. Delete Semantics (reuses existing logic)

- `milvus.read.apply.deletes` (default `true`).
- L1 data segments with their own `deltalog` → per-segment own delete plan
  (`loadV2DeletePlans`), attached to that segment's partition.
- L0 delete-only segments → partition-scoped inherited plans
  (`loadPartitionScopedDeletePlans`); `partition_id = -1` means
  collection-wide.
- At read time, rows are filtered by `(pk, timestamp)` inside the packed-V2
  reader, matching snapshot-read behavior.

## 7. Edge Cases & Limitations

- Backups default to a flush, so only disk-resident (flushed) data is read — a
  consistent point-in-time copy, same constraint as snapshot reads.
- Empty segments (`num_of_rows == 0`, no binlogs) emit `columnGroups =
  Seq.empty` and are skipped during partition planning. A StorageV2 data
  segment with rows but no binlogs fails hard rather than silently dropping
  rows.
- Dynamic collections: a default backup (no etcd access) does not record the
  `$meta` field, so reading it would return null `$meta` rows; such backups are
  rejected with a pointer to `--backup_index_extra`.
- `milvus.partition.name` / `milvus.partition.id` / `milvus.segment.id`
  selectors are rejected (not yet supported); read the whole backup and filter
  in Spark.
- The collection is selected by `milvus.database.name` +
  `milvus.collection.name` (never `.head`); an unqualified name that is
  ambiguous across databases, or no name with multiple collections, is
  rejected.
- Non-L0 segments that are not StorageV2 (`storage_version != 2`) fail hard on
  the driver rather than returning a partial dataset; `0`/absent is reported as
  StorageV1. L0 delete-only segments bypass the storage-version check (Milvus
  creates them without one).
- The driver validates that the backup meta carries a collection schema with a
  primary key before planning, and that the read has at least one packed
  (non-delete-only) segment — an L0-only backup otherwise reads zero rows.
  `full_meta.json` is read with a bounded reader
  (`milvus.snapshot.max.json.bytes`, default 64 MiB) and **not** cached.
- `toV2Segments` materializes segments only for the resolved `collectionId`, so
  a multi-collection backup never leaks another collection's segments into the
  read.
- Schema conversion drops system fields by field ID (`0`/`1`), never by name:
  milvus-backup's schema carries only user fields, so a user field literally
  named `RowID`/`Timestamp` survives.
- `milvus.backup.dir` normalizes the `s3://` alias to `s3a://`; `file:///...`
  and bare local paths resolve to the same native keys.
- A dynamic collection whose `$meta` field is present is not duplicated by the
  computed schema.
- Local paths (`/data/backup/...` or `file:///...`) work for the meta/footer
  mapping layer (and unit tests); the JNI packed reader itself requires S3
  storage.
- A backup must contain exactly one collection per datasource read.

## 8. Usage

```scala
spark.read
  .format("milvus")
  .schema(userSchema)
  .option("milvus.backup.dir", "s3a://bucket/backup/b1")
  .option("milvus.collection.name", "demo")
  // reuse the existing fs.* / s3.* options for S3 credentials:
  .option("fs.address", "localhost:9000")
  .option("fs.bucket_name", "a-bucket")
  .option("fs.access_key_id", "minioadmin")
  .option("fs.access_key_value", "minioadmin")
  .load()
```

## 9. Testing

- `BackupMetaReaderTest` — meta parsing from a fixture `full_meta.json`,
  schema round-trip, column-group / row-count recovery against local parquet
  files written with parquet-mr (carrying `PARQUET:field_id`), L0 skipping with
  `applyDeletes = false`, StorageV1/V3 branches, and snapshot-format rejection.
- `MilvusParquetFooterReaderTest` — `readRowCount` sums row groups.
- `MilvusOptionTest` — `isBackupMode` and snapshot/backup mutual exclusion.
- End-to-end: Milvus 2.6 → `milvus-backup create` → `spark.read` with
  `milvus.backup.dir`, cross-checked against `count(*)` / distinct PKs.

## 10. Out of Scope / Future Work

- StorageV3 (loon) segments: requires milvus-backup to copy `.milvus_manifest`
  or footer-based layout recovery for V3.
- Optional: milvus-backup persisting `entries_num` (the connector already
  falls back to footer reads when the meta lacks it).
