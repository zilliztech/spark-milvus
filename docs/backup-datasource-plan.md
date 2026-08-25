# 实现计划：milvus-backup 作为 Milvus 读数据源（spark-milvus）

## 1. 背景与目标

**场景**：Milvus 2.6，无 snapshot 可用。希望用 `milvus-backup create` 的产物（binlog 格式备份）作为 spark-milvus 的只读数据源，替代 client / snapshot 读，提供一致的离线读能力。

**目标**：
- 输入：一个 backup 目录（`milvus.backup.dir`，形如 `s3a://bucket/backup/<name>` 或本地路径）。
- 输出：Spark DataFrame，支持列裁剪、`milvus.extra.columns`（partition / `$segment_id` / `$row_offset`）、删除语义（apply deletes）。
- 完全复用现有 StorageV2 packed read 栈（`MilvusPackedV2PartitionReader` + milvus-storage JNI），**不改 milvus-backup**。
- **排除**：StorageV3(loon manifest) 段、写入/回填、client snapshot 快路径。

**核心缺口与方案**：backup meta 的 `Binlog` 只写 `log_path/log_size/log_id`，`entries_num`（每文件行数）未填充；而 packed 读要求精确的 `fileRowCounts`（累加出 `(start_index,end_index)`，缺失即拒/错读）。**方案 A：连接器读各 parquet footer 的总行数补齐**，对现有 backup 零改动。

## 2. 关键事实（已核实）

| 事实 | 出处 |
|---|---|
| backup 的 binlog 对象与 Milvus 存储逐字节一致 | `milvus-backup/core/backup/coll_dml_task.go:511-546`（CopyObjectsTask 原样复制） |
| backup 布局 `binlogs/insert_log/{coll}/{part}/{groupID}/{seg}/{field}/{log}`；`groupID` 为虚拟 partition，仅 restore 用 | `milvus-backup/internal/storage/mpath/path.go:17-26, 288-297` |
| 完整 meta 在 `meta/full_meta.json`（schema + partitions + segments + L0） | `milvus-backup/internal/meta/meta.go:129-139`；`meta_builder.go:327-337` |
| `SegmentBackupInfo`：id / `num_of_rows` / `storage_version` / `group_id` / `is_l0` / `binlogs` / `deltalogs` | `milvus-backup/core/proto/backup.proto:102-120` |
| `Binlog` 仅填 `log_path/log_size/log_id`，`entries_num` 字段存在但 deprecate 未填充 | `backup.proto:494-501`；`coll_dml_task.go:101,131` |
| packed 读要求 `fileRowCounts` 精确 | `src/main/scala/read/MilvusPackedV2PartitionReader.scala:253-260`；`milvus-storage/cpp/include/milvus-storage/ffi_internal/v2_column_groups_builder.h:35-43` |
| 真实 field ID 可从 parquet 自带 schema 的 `PARQUET:field_id` 恢复 | `src/main/scala/read/MilvusParquetFooterReader.readFieldIdsFromSchema` |
| delta log 解码只用 `logPath`，`entriesNum` 不参与 | `src/main/scala/read/MilvusDeltaLogReader.scala:127-145` |
| L0 段无 column groups、只有 delta log → inherited partition 级 delete plan | `src/main/scala/sources/MilvusDataSource.scala:1628-1662, 2269-2303` |

## 3. 总体设计

新增 **backup 离线模式**，与 snapshot 模式平级：

```
MilvusDataSource / MilvusTable  isBackupMode? ─┐
                                              ▼
MilvusScan.computeInputPartitions ──> planInputPartitionsFromBackup()
                                              │  用 BackupMetaReader 产出
                                              ▼
                           (schemaBytes, Seq[V2SegmentInfo])
                                              │  复用
                                              ▼
                    buildSnapshotPartitions() → MilvusPackedV2InputPartition[]
                                              │
              createReaderFactory() → MilvusPackedV2PartitionReader（不变）
```

分支优先级：`isSnapshotMode` > `isBackupMode` > client。

## 4. 改动文件清单

### 4.1 `src/main/scala/MilvusOption.scala`
- 新增 `MilvusOption.BackupDir = "milvus.backup.dir"`。
- 新增 `isBackupMode(options)`：`BackupDir` 非空即开启。
- `MilvusDataSource.getTable/inferSchema`：backup 模式下允许无 `milvus.uri`；`validateSnapshotModeOptions` 处新增 snapshot/backup 互斥校验。
- S3 配置复用现有 `fs.*` / `s3.*`。

### 4.2 新增 `src/main/scala/read/BackupMetaReader.scala`（核心）
Jackson 解析 backup `full_meta.json`（仿 `MilvusSnapshotReader`），对外：
```scala
object BackupMetaReader {
  def readMeta(hadoopConf: Configuration, backupDir: String): Either[Throwable, BackupInfo]
  def toProtobufSchemaBytes(schema: BackupCollectionSchema): Array[Byte]
  def toV2Segments(info: BackupInfo, hadoopConf: Configuration, bucket: String, applyDeletes: Boolean): Either[Throwable, Seq[V2SegmentInfo]]
}
```
内嵌 JSON 模型：`BackupInfo`、`CollectionBackupInfo`、`PartitionBackupInfo`、`SegmentBackupInfo`、`FieldBinlog`、`Binlog`、`CollectionSchema`/`FieldSchema`（镜像 schemapb，含 `type_params`、`data_type`、`is_primary_key`、`element_type`、`is_dynamic`、`nullable` 等）。

### 4.3 `src/main/scala/read/MilvusParquetFooterReader.scala`
- 新增 `readRowCount(path, hadoopConf): Either[Throwable, Long]`：`ParquetFileReader.getFooter.getBlocks` 求和（每文件总行数），复用现有 `readWithFileSystem`。

### 4.4 `src/main/scala/sources/MilvusDataSource.scala`
- `MilvusTable.isBackupMode` → `initFromBackup()`（跳过 client，schema 由 `BackupMetaReader` 或用户 `.schema()` 提供）。
- `MilvusScan.planInputPartitionsFromBackup()`：
  1. 构建 backup bucket hadoop conf（复用 `buildSnapshotHadoopConf`）。
  2. `BackupMetaReader.readMeta` → schema bytes + `V2SegmentInfo`。
  3. 复用 `loadV2DeletePlans`（L1 段）与 `loadPartitionScopedDeletePlans`（L0 段）。
  4. 调用现有 `buildSnapshotPartitions(v2Segments=..., v2DeletePlans=...)`。

## 5. 数据映射（`full_meta.json → V2SegmentInfo`）

| `V2SegmentInfo` | 来源 | 备注 |
|---|---|---|
| `segmentId` | `SegmentBackupInfo.segment_id` | |
| `partitionId` | `.partition_id` | part=-1 保留（L0/全分区） |
| `numOfRows` | `.num_of_rows` | |
| `storageVersion` | `.storage_version` | 仅支持 ==2；>2 报错，<2 跳过 |
| `columnGroups` | `.binlogs[]` 按 `field_id`(slot) 分组 | slot 即目录名 |
| `cg.fieldIds` | 每组首个文件 `readFieldIdsFromSchema` | |
| `cg.filePaths` | 组内 `log_path` 按 `log_id` 升序 | 路径显式，含 `binlogs/` 与 groupID 层级，直接解析 |
| `cg.fileRowCounts` | 每组每文件 `readRowCount` | **方案 A 补齐**；`size==paths.size` 校验 |
| `cg.slotFieldId` | 组的 `field_id` | 供 slot 去重 |
| `deltaLogs` | `.deltalogs[].binlogs[]` → `(log_id, log_path)` | `entriesNum=0`（解码不使用） |
| L0 段 | `is_l0=true` → `columnGroups=Seq.empty` + deltaLogs | 走 inherited plan |

路径解析：`V2SegmentLoader.resolvePath(logPath, bucket)` 加 `s3a://bucket/`；不做 groupID 反推。

## 6. 删除语义（复用现有逻辑）

- `milvus.read.apply.deletes`（默认 true）。
- L1 段 `delta_log` → `loadV2DeletePlans`（段级 plan）。
- L0 段 → `loadPartitionScopedDeletePlans`（`-1` = 全集合删除）。
- 读时按 `(pk, timestamp)` 在 packed reader 内过滤（`isDeleted`）。

## 7. 边界与限制

- backup 默认 flush，仅含落盘数据；一致时间点快照，与 snapshot 读语义一致。
- 空段/无 binlog：`columnGroups=Seq.empty`，planning 跳过（沿用 `skippedDeleteOnlySegments`）。
- 动态字段 `$meta`：保留 `enableDynamicField` 与 `$meta`（packed 读 `ToleratedUnmappedColumns` 容忍）。
- V3 段直接报"不支持"，避免静默读错。
- 本地路径（`/data/backup/xxx`）走本地 FS，便于 dev/test。

## 8. 测试计划

1. **单元测试**
   - `BackupMetaReaderSpec`：fixture `full_meta.json`（1 个 L1 段 + 1 个 L0 段 + 多列分组）→ 断言 schema bytes、`V2SegmentInfo` 的 fieldIds/filePaths/排序、L0 空 columnGroups。
   - `readRowCountSpec`：本地 parquet 行数正确性。
2. **集成脚本**（仿现有 demo）：MinIO + Milvus 2.6 → 灌数据/flush → `milvus-backup create -n b1` → `spark.read.format(...).options("milvus.backup.dir", ...)`，对比 `count(*)` 与 distinct PK。
3. **回归**：snapshot 模式、client 读不受影响。

## 9. 不在本次范围 / 后续可选

- StorageV3(loon) 段：需 milvus-backup 拷贝 `.milvus_manifest` 或 footer 反推 V3 布局。
- 方案 B：milvus-backup 填充 `entries_num`（连接器留 fallback：meta 有则优先，无则读 footer）。
