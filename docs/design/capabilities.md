# 2.0 功能规划 `[草稿]`

2.0 的功能按用户入口分七组：表读、表写、目录与 DDL、CALL、向量与索引、兼容入口、场景与工具。每条写用户怎么用、哪个模块实现、依赖什么、排在哪个优先级（优先级见 README 第 3 节）。不做的功能列在末尾。

## 1 表读

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| R1 | collection 是一张表，三段名 | `spark.table("milvus.db.coll")`，`milvus.db.coll` 出现在任何 SQL 里 | spark.catalog | 目录服务见 C1 | P1 |
| R2 | 读固定快照 | loadTable 的 `version` 指快照名，`timestamp` 指时间点；缺省最新 | core.snapshot，spark.catalog | 快照由 Milvus 生成 | P0 |
| R3 | 读不经 Milvus 服务 | 只配对象存储凭证即可读 | core.snapshot、core.read | 快照目录可列 | P0 |
| R4 | 列式扫描，向量零拷贝 | 自动 | core.read，spark.scan | native-storage | P0 |
| R5 | 列裁剪 | `select` | spark.scan → core.read 只读需要的列组 | | P0 |
| R6 | 谓词下推：Spark 谓词 | `where` 里的比较、IN、IS NULL、字符串前后缀、AND、OR、NOT | spark.expr 翻成 IR，core.expr 求值 | | P1 |
| R7 | 谓词下推：Milvus 表达式 | 表 option `milvus.filter`，字符串按 Milvus 文法 | core.expr 解析（Plan.g4）和求值 | JSON、Array、json_contains 语义按 Milvus 源码 | P1 |
| R8 | 删除生效 | 自动；快照时间戳之前的删除 | core.delete | | P0 |
| R9 | 段级剪枝 | 自动；主键等值和 IN 用段的 bloom filter | core.snapshot、core.stats | Milvus 侧统计（README 第 5 节） | P1 |
| R10 | row group 级剪枝 | 自动；标量列 min/max | core.read | 同上 | P1 |
| R11 | Limit 下推 | `limit` | spark.scan | | P1 |
| R12 | 元数据列 | `_segment_id`、`_row_offset`、`_timestamp`（名字见决策 5） | spark.table | | P1 |
| R13 | 表统计 | 自动；行数和字节数给 Spark 选 join 策略 | spark.table ← core.snapshot | | P1 |
| R14 | 回表 | 下游算子按 (段 id, 行号) 取列 | core.read 的 take | | P2 |
| R15 | 类型覆盖 | 标量、VarChar、JSON、Array、Float/Float16/BFloat16/Int8/Binary/Sparse 向量、Text | core.schema，spark.expr 的类型映射 | 透传或转换见决策 6；Text 要 LOB 还原 | P0 |
| R16 | 分区和段选择 | option `milvus.partitions`、`milvus.segments` | spark.scan → core.snapshot 过滤段列表 | | P1 |
| R17 | 交付下游列式算子 | 列批、向量 buffer 地址、位图 | core.read 的出口 | 决策 12 | P0 |

## 2 表写

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| W1 | append 写新段 | `df.writeTo("milvus.db.coll").append()` | spark.write，core.write | 登记要 Milvus 新增 RegisterSegments RPC；接口到位前写出的段只能经 import 过渡 | P1 |
| W2 | backfill 只写新列组 | `.option("write.mode","backfill").option("columns","f")` | spark.write，core.write | AddCollectionField 先做；登记走 BatchUpdateManifest | P2 |
| W3 | 原子提交 | 自动；暂存前缀、作业清单、幂等 commit、abort 清理 | core.write | | P1 |
| W4 | truncate 和 overwrite | `.overwrite()`，只接受全表 | spark.write | 登记接口 | P1 |
| W5 | DELETE | `DELETE FROM milvus.db.coll WHERE ...`，只接能翻成 Milvus 表达式的谓词 | spark.table 的 DeleteV2 → client | Milvus 在线 | P1 |
| W6 | 索引随段写出 | 写 option `index.<field>=HNSW,...` | core.index，native-vector | Milvus 认 Manifest 里的索引登记 | P2 |
| W7 | 小批量 gRPC 写入 | `format("milvus")` 的旧路径 | ops.legacy | 保留作兜底 | 已有 |

## 3 目录与 DDL

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| C1 | 数据库和 collection 目录 | `SHOW NAMESPACES`、`SHOW TABLES`、`DESCRIBE TABLE` | spark.catalog → client | Milvus 在线 | P1 |
| C2 | 建表删表 | `CREATE TABLE milvus.db.coll (...) TBLPROPERTIES (...)`、`DROP TABLE` | spark.catalog → client | 向量维度、主键、索引参数走表属性 | P1 |
| C3 | schema 来自快照 | 自动 | core.schema | | P0 |

## 4 CALL

| 编号 | 功能 | 用户入口（Spark 4；3.5 为同名函数） | 实现位置 | 优先级 |
|---|---|---|---|---|
| P1 | 快照 | `CALL milvus.system.create_snapshot('db.coll')`，drop、list、describe；建前默认 Flush（决策 11） | spark.procedure → client | P1 |
| P2 | 索引 | `create_index`、`drop_index`，可等待完成 | 同上 | P1 |
| P3 | 生命周期 | `load`、`release`、`flush`、`compact` | 同上 | P1 |
| P4 | 登记 | `register('db.coll', job => 'staging/{job}')`：读作业清单，backfill 走 BatchUpdateManifest，append 走 RegisterSegments | spark.procedure → core.write、client | P1 |
| P5 | 描述 | `describe`：schema、段数、索引状态 | 同上 | P1 |

## 5 向量与索引

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| V1 | knowhere 封装 | 下游算子和 CALL 用；create、build、search、range_search、serialize、deserialize、bruteforce | native-vector | knowhere 的 C shim | P2 |
| V2 | 加载 Milvus 建的索引 | 自动；按快照里的索引文件 | core.index | 索引文件路径在快照或 Manifest 里 | P2 |
| V3 | 索引写出 | W6 | core.index | | P2 |
| V4 | 索引来源与缓存 | 自动；Milvus 建的、Spark 写回的、任务内即时建的三级，按 (build id, 版本, 段, 字段) 缓存 | core.index | | P2 |
| V5 | 暴力搜索 | 形态见决策 16 | native-vector 的 bruteforce；1.x 实现在 ops.search | | P2 |
| V6 | 向量和位图以地址交出 | R17 | core.read | | P0 |

## 6 兼容入口

| 编号 | 功能 | 用户入口 | 实现位置 | 说明 |
|---|---|---|---|---|
| K1 | Storage V2 packed 段 | 自动识别 storage_version=2 的段 | compat.v2packed | 主路径只认 V3；这是 1.x 的 reader 迁入 |
| K2 | 离线 option 塞段列表 | `milvus.snapshot.manifests` 等 1.x option | compat.offline | 产出 core 的 Snapshot |
| K3 | backup 目录 | `milvus.backup.dir` | compat.backup | 读 milvus-backup 导出，产出 Snapshot |
| K4 | 1.x option 名 | `milvus.collection.name` 等 | spark.options | 映射到三段名和新 option，打告警 |

## 7 场景与工具

| 编号 | 功能 | 用户入口 | 实现位置 | 说明 |
|---|---|---|---|---|
| O1 | backfill 作业 | `spark-submit --class ...BackfillApp`，24 个 flag，结果 JSON | ops.backfill | 1.x 迁入；内部改用 W2 |
| O2 | 调试工具 | ListV2SegmentsApp、ReadSourceOnlyApp | ops.tools | 1.x 迁入 |
| O3 | JVM 暴力搜索 | DataFrame 方法、6 个 SQL 函数 | ops.search | 1.x 迁入，形态见决策 16 |

## 8 不做

TopN 和 Aggregates 下推；UPDATE 和 MERGE；text_match 一族（依赖 tantivy 文本索引）；GIS 表达式；struct 数组表达式；random_sample。
