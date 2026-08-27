# 实现计划：milvus-backup 作为 Milvus 读数据源（spark-milvus）

> **⚠️ 已废弃 / SUPERSEDED**：本文件是**首个提交**时的实现计划，此后 8 轮 review 修正了大量设计（路径重建、L0/storage_version 处理、非 S3 拒绝、collection 匹配、meta 单次解析等），本文多处已与实现**相反**。
> **请以 [`docs/backup-datasource-design.md`](backup-datasource-design.md)（英文、持续更新、已实现）为准。** 尤其不要按本文件 §5 的"直接解析 `log_path` / 不做 groupID 反推"实现——`log_path` 是原始 Milvus 源 key，照做会把读打到源集群对象路径上。

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
  def readMeta(hadoopConf: Configuration, backupDir: String,
               maxBytes: Long = MilvusSnapshotReader.MaxSnapshotJsonBytes): Either[Throwable, BackupInfo]
  def toProtobufSchemaBytes(schema: BackupCollectionSchema): Array[Byte]
  def toV2Segments(info: BackupInfo, hadoopConf: Configuration, backupDir: String,
                   applyDeletes: Boolean, collectionId: Long): Either[Throwable, Seq[V2SegmentInfo]]
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
| `storageVersion` | `.storage_version` | **已实现**：L0 段（无版本/`0`）在版本过滤前处理；非 L0 非 V2 一律硬失败（`0`/absent 即 StorageV1） |
| `columnGroups` | `.binlogs[]` 按 `field_id`(slot) 分组 | slot 即目录名 |
| `cg.fieldIds` | 每组首个文件 `readFieldIdsAndRowCount` | 单次打开同时取 fieldIDs 与行数 |
| `cg.filePaths` | **已实现：从 `backupDir`+IDs 重建**（见下），绝不使用 meta 的 `log_path` | 原生 reader 用 bucket 相对 key；Hadoop 读用带 scheme URI |
| `cg.fileRowCounts` | 首文件合并读取 + 其余并行 `readRowCount` | **方案 A 补齐**；`size==paths.size` 校验 |
| `cg.slotFieldId` | 组的 `field_id` | 读路径按 slot 去重（`dedupColumnGroupsBySlot`） |
| `deltaLogs` | **已实现：从 `backupDir`+IDs 重建**；`partitionId != -1` 含 group 层级 | `entriesNum=0`（解码不使用） |
| L0 段 | `is_l0=true` → `columnGroups=Seq.empty` + deltaLogs | 走 inherited plan |

路径解析（**已实现**，与初稿相反）：meta 的 `log_path` 是原始 Milvus 源 key，**不能直接解析**。备份对象路径按 milvus-backup 的 `DestKey` 布局从 `backupDir` + collection/partition/group/segment/field/log ID **重建**：

```
insert: {backupDir}/binlogs/insert_log/{coll}/{part}/{group}/{seg}/{field}/{log}
delta:  {backupDir}/binlogs/delta_log/{coll}/{part}/{seg}/{log}          (part == -1)
        {backupDir}/binlogs/delta_log/{coll}/{part}/{group}/{seg}/{log}  (part != -1)
```

Hadoop 侧读（footer/meta/delta）用带 scheme 的 URI（`s3a://...`）；原生 JNI reader 的 `filePaths` 用 bucket 相对 key，`fs.bucket_name` 由 backup URI 规范化。**读必须用 S3（`s3a://`）backup dir，本地/`file://` 在规划期被拒绝。**

## 6. 删除语义（复用现有逻辑）

- `milvus.read.apply.deletes`（默认 true）。
- L1 段 `delta_log` → `loadV2DeletePlans`（段级 plan）。
- L0 段 → `loadPartitionScopedDeletePlans`（`-1` = 全集合删除）。
- 读时按 `(pk, timestamp)` 在 packed reader 内过滤（`isDeleted`）。

## 7. 边界与限制

- backup 默认 flush，仅含落盘数据；一致时间点快照，与 snapshot 读语义一致。
- 空段/无 binlog：`num_of_rows == 0` 且无 binlogs 才发射空 `columnGroups`；有行数但无 binlogs 硬失败。
- 动态字段 `$meta`（**已实现**）：meta 无 `$meta` 记录（默认 backup 无 etcd 访问）时**拒绝读取**并提示 `--backup_index_extra`；有记录则正常保留。
- 非 L0 且非 StorageV2 段硬失败；L0 段无版本也保留（删除语义）。
- 本地路径（`/data/backup/xxx` / `file://`）：仅 meta/footer 映射层与单测可用；**实际读在规划期拒绝**（JNI reader 需 S3）。
- collection 按 `milvus.database.name` + `milvus.collection.name` 匹配（`default` 与空库名等价），不允许 `.head` 兜底。

## 8. 测试计划

1. **单元测试**
   - `BackupMetaReaderSpec`：fixture `full_meta.json`（1 个 L1 段 + 1 个 L0 段 + 多列分组）→ 断言 schema bytes、`V2SegmentInfo` 的 fieldIds/filePaths/排序、L0 空 columnGroups。
   - `readRowCountSpec`：本地 parquet 行数正确性。
2. **集成脚本**（仿现有 demo）：MinIO + Milvus 2.6 → 灌数据/flush → `milvus-backup create -n b1` → `spark.read.format(...).options("milvus.backup.dir", ...)`，对比 `count(*)` 与 distinct PK。
3. **回归**：snapshot 模式、client 读不受影响。

## 9. 不在本次范围 / 后续可选

- StorageV3(loon) 段：需 milvus-backup 拷贝 `.milvus_manifest` 或 footer 反推 V3 布局。
- 方案 B：milvus-backup 填充 `entries_num`（**已实现：连接器总是读 footer 行数，不优先 meta 的 entries_num**；若未来 milvus-backup 填充该字段，可加"有则用"的 fallback）。
