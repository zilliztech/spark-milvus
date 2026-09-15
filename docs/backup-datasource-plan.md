# Implementation Plan — milvus-backup as a Milvus Read Data Source (spark-milvus)

> **⚠️ SUPERSEDED**: This file is the implementation plan from the **first commit**. Eleven
> subsequent review rounds corrected large parts of the design (path reconstruction,
> L0/storage_version handling, non-S3 rejection, collection matching, single meta parse,
> **consistency semantics**, dynamic-field minimum version, and more); much of this document
> now contradicts the implementation.
> **Please treat [`docs/backup-datasource-design.md`](backup-datasource-design.md) (English,
> continuously updated, implemented) as authoritative.** In particular, do NOT implement
> §5's "parse `log_path` directly / no groupID reverse-engineering" — `log_path` is the
> original Milvus source key; following it would point reads at the source cluster's objects.

## 1. Background & Goals

**Scenario**: Milvus 2.6 with no snapshot available. Use `milvus-backup create`'s output
(a binlog-format backup) as a read-only data source for spark-milvus, in place of the client /
snapshot read, providing an offline read capability.

**Goals**:
- Input: an S3 backup directory (`milvus.backup.dir`, e.g. `s3a://bucket/backup/<name>`).
  Local / `file://` dirs are exercised by the meta/footer mapping layer and unit tests only;
  actual reads require S3 (see §7).
- Output: a Spark DataFrame with column pruning, `milvus.extra.columns`
  (`_segment_id`, `_row_offset`, `_timestamp`), and delete semantics (apply
  deletes).
- Reuse the existing StorageV2 packed read stack (`MilvusV2PartitionReader` +
  milvus-storage JNI) — **no changes to milvus-backup**.
- **Excluded**: StorageV3 (loon manifest) segments, write/backfill, the client snapshot fast path.

**Core gap & approach**: backup meta's `Binlog` only records `log_path/log_size/log_id`;
`entries_num` (per-file row count) is not populated. The packed read requires exact
`fileRowCounts` (cumulatively forming `(start_index,end_index)`; missing values are rejected /
mis-read). **Approach A: recover each file's row count by reading its parquet footer** —
zero changes to existing backups.

## 2. Verified Key Facts

| Fact | Source |
|---|---|
| Backup binlog objects are byte-identical to Milvus storage objects | `milvus-backup/core/backup/coll_dml_task.go:511-546` (CopyObjectsTask copies as-is) |
| Backup layout `binlogs/insert_log/{coll}/{part}/{groupID}/{seg}/{field}/{log}`; `groupID` is a virtual partition, restore-only | `milvus-backup/internal/storage/mpath/path.go:17-26, 288-297` |
| Full meta in `meta/full_meta.json` (schema + partitions + segments + L0) | `milvus-backup/internal/meta/meta.go:129-139`; `meta_builder.go:327-337` |
| `SegmentBackupInfo`: id / `num_of_rows` / `storage_version` / `group_id` / `is_l0` / `binlogs` / `deltalogs` | `milvus-backup/core/proto/backup.proto:102-120` |
| `Binlog` only records `log_path/log_size/log_id`; `entries_num` exists but is deprecated/unset | `backup.proto:494-501`; `coll_dml_task.go:101,131` |
| Packed read requires exact `fileRowCounts` | `src/main/scala/read/MilvusV2PartitionReader.scala:253-260`; `milvus-storage/cpp/include/milvus-storage/ffi_internal/v2_column_groups_builder.h:35-43` |
| Real field IDs recoverable from the parquet's own schema `PARQUET:field_id` | `src/main/scala/read/ParquetFooterReader.readFieldIdsFromSchema` |
| Delta-log decoding uses only `logPath`; `entriesNum` unused | `src/main/scala/read/DeltaLogReader.scala:127-145` |
| L0 segments have no column groups; their delta-log descriptors stay on delete-only `Segment` records and are inherited by data tasks in the same partition (`-1` applies collection-wide) | `compat/src/main/scala/com/zilliz/milvus/storage/compat/backup/BackupMetaReader.scala`; `core/src/main/scala/com/zilliz/milvus/storage/read/plan/DeleteFileListing.scala` |

## 3. Overall Design

Add a **backup offline mode**, parallel to snapshot mode:

```
MilvusDataSource / MilvusTable
              │
              ▼
SnapshotSources.forRead() → BackupSnapshotSource → Snapshot
                                                      │
                                                      ▼
                                 DeleteFileListing.of + ReadPlan.of
                                                      │
                                                      ▼
                                      MilvusV2InputPartition[]
                                                      │
                          createReaderFactory() → MilvusV2PartitionReader
```

Branch precedence: `isSnapshotMode` > `isBackupMode` > client.

## 4. Changed Files

### 4.1 `src/main/scala/MilvusOption.scala`
- New `MilvusOption.BackupDir = "milvus.backup.dir"`.
- New `isBackupMode(options)`: enabled when `BackupDir` is non-empty.
- `MilvusDataSource.getTable/inferSchema`: allow backup mode without `milvus.uri`; add snapshot/backup mutual-exclusion validation at `validateSnapshotModeOptions`.
- S3 config reuses existing `fs.*` / `s3.*`.

### 4.2 New `src/main/scala/read/BackupMetaReader.scala` (core)
Parses backup `full_meta.json` with Jackson (mirroring the snapshot JSON parser, now `core.snapshot.json`):
```scala
object BackupMetaReader {
  def readMeta(hadoopConf: Configuration, backupDir: String,
               maxBytes: Long = SnapshotJson.MaxBytes): Either[Throwable, BackupInfo]
  def toProtobufSchemaBytes(schema: BackupCollectionSchema): Array[Byte]
  def toV2Segments(info: BackupInfo, hadoopConf: Configuration, backupDir: String,
                   applyDeletes: Boolean, collectionId: Long): Either[Throwable, Seq[V2SegmentInfo]]
}
```
Embedded JSON model: `BackupInfo`, `CollectionBackupInfo`, `PartitionBackupInfo`,
`SegmentBackupInfo`, `FieldBinlog`, `Binlog`, `CollectionSchema`/`FieldSchema` (a mirror of
schemapb, including `type_params`, `data_type`, `is_primary_key`, `element_type`, `is_dynamic`,
`nullable`, etc.).

### 4.3 `src/main/scala/read/ParquetFooterReader.scala`
- New `readRowCount(path, hadoopConf): Either[Throwable, Long]`: sums
  `ParquetFileReader.getFooter.getBlocks` (per-file total row count), reusing the existing
  `readWithFileSystem`.

### 4.4 `src/main/scala/sources/MilvusDataSource.scala`
- `MilvusTable.isBackupMode` → `initFromBackup()` (skips the client; schema from
  `BackupMetaReader` or the user's `.schema()`).
- The current route is shared with the other snapshot sources:
  1. `SnapshotSources.forRead` selects `BackupSnapshotSource`.
  2. `BackupMetaReader` materializes the schema, data segments and delete-only L0
     segments into one core `Snapshot`.
  3. `MilvusScan` calls `DeleteFileListing.of` and `ReadPlan.of`; the driver lists
     delete-file descriptors but does not decode delete plans.
  4. Layer 3 wraps each `SegmentReadTask` as a `MilvusV2InputPartition`.

## 5. Data Mapping (`full_meta.json → V2SegmentInfo`)

| `V2SegmentInfo` | Source | Notes |
|---|---|---|
| `segmentId` | `SegmentBackupInfo.segment_id` | |
| `partitionId` | `.partition_id` | part=-1 preserved (L0 / all-partition) |
| `numOfRows` | `.num_of_rows` | |
| `storageVersion` | `.storage_version` | **Implemented**: L0 (no version / `0`) handled before the version filter; non-L0 non-V2 fails hard (`0`/absent = StorageV1) |
| `columnGroups` | `.binlogs[]` grouped by `field_id` (slot) | slot = directory name |
| `cg.fieldIds` | head file of each group via `readFieldIdsAndRowCount` | a single open yields fieldIDs + row count |
| `cg.filePaths` | **Implemented: reconstructed from `backupDir`+IDs** (below); never the meta `log_path` | native reader uses bucket-relative keys; Hadoop reads use scheme-qualified URIs |
| `cg.fileRowCounts` | head file combined read + remaining in parallel `readRowCount` | **Approach A gap-closer**; `size==paths.size` enforced |
| `cg.slotFieldId` | group's `field_id` | read path dedups by slot (`dedupColumnGroupsBySlot`) |
| `deltaLogs` | **Implemented: reconstructed from `backupDir`+IDs**; `partitionId != -1` carries the group level | `entriesNum=0` (unused by the decoder) |
| L0 segment | `is_l0=true` → `columnGroups=Seq.empty` + deltaLogs | inherited delete-plan path |

Path resolution (**implemented**, opposite of the initial draft): the meta's `log_path` is the
original Milvus source key and must **not** be parsed directly. Backup object paths are
**reconstructed** from `backupDir` + collection/partition/group/segment/field/log IDs following
milvus-backup's `DestKey` layout:

```
insert: {backupDir}/binlogs/insert_log/{coll}/{part}/{group}/{seg}/{field}/{log}
delta:  {backupDir}/binlogs/delta_log/{coll}/{part}/{seg}/{log}          (part == -1)
        {backupDir}/binlogs/delta_log/{coll}/{part}/{group}/{seg}/{log}  (part != -1)
```

Hadoop-side reads (footer/meta/delta) use scheme-qualified URIs (`s3a://...`); the native JNI
reader's `filePaths` use bucket-relative keys with `fs.bucket_name` canonicalized from the
backup URI. **Reads require an S3 (`s3a://`) backup dir; local / `file://` is rejected at
planning.**

## 6. Delete Semantics (reuses existing logic)

- `milvus.read.apply.deletes` (default true).
- An L1 data segment keeps its own `delta_log` descriptors on the segment.
- L0 delete-only segments remain in the `Snapshot`; `DeleteFileListing` adds the
  current partition's descriptors and the collection-wide (`-1`) descriptors to
  every affected data task.
- The executor's `DeletePlans` reads and merges the task's files. Rows are then
  filtered by `(pk, timestamp)` inside the packed reader (`isDeleted`).

## 7. Edge Cases & Limitations

- **Consistency (implemented, opposite of the initial draft)**: backups default to a flush and
  contain only disk-resident data, but they are **not** a server-enforced transactionally
  consistent point-in-time view — the GC pause is best-effort (may fail silently, #1119), each
  collection is sealed at its own flush timestamp, and skipFlush omits the flush, so a
  GC/compaction race can tear the view. **Do not use it as a cutover/reconciliation source of
  truth** (that guarantee belongs to a snapshot). See `docs/backup-datasource-design.md` §7.
- Empty segment / no binlogs: an empty `columnGroups` is emitted only when `num_of_rows == 0`;
  a segment with rows but no binlogs fails hard.
- Dynamic field `$meta` (**implemented**): when the meta lacks the `$meta` record (a default
  backup without etcd access) the read is **rejected** with a pointer to `--backup_index_extra`;
  when present it is preserved.
- Non-L0 non-StorageV2 segments fail hard; L0 segments without a version are preserved (delete
  semantics).
- Local paths (`/data/backup/xxx` / `file://`): usable by the meta/footer mapping layer and unit
  tests only; actual reads are rejected at planning (the JNI reader requires S3).
- The collection is matched by `milvus.database.name` + `milvus.collection.name` (never
  `.head`). Passing `"default"` selects the default-database collection (matching a meta that
  records `""` or `"default"`), while leaving the database name empty performs
  single-candidate/ambiguity resolution — so with `default.orders` and `db2.orders` present,
  pass `"default"` (or `"db2"`) to disambiguate.

## 8. Test Plan

1. **Unit tests**
   - `BackupMetaReaderSpec`: fixture `full_meta.json` (one L1 segment + one L0 segment + multiple
     column groups) → assert schema bytes, `V2SegmentInfo` fieldIds/filePaths/order, L0 empty
     columnGroups.
   - `readRowCountSpec`: local parquet row-count correctness.
2. **Integration script** (mirroring existing demos): MinIO + Milvus 2.6 → ingest/flush →
   `milvus-backup create -n b1` → `spark.read.format(...).options("milvus.backup.dir", ...)`,
   comparing `count(*)` and distinct PK.
3. **Regression**: snapshot mode and client read unaffected.

## 9. Out of Scope / Future

- StorageV3 (loon) segments: requires milvus-backup to copy `.milvus_manifest` or footer-based
  V3 layout recovery.
- Option B: milvus-backup populating `entries_num` (**implemented: the connector always reads
  the footer row count, not preferring the meta's entries_num**; if milvus-backup populates it
  in the future, an "use-if-present" fallback could be added).
