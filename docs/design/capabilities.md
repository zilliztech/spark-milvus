# 2.0 功能规划 `[草稿]`

51 条功能按用户入口分八组。读的 19 条全程不经 Milvus 服务；写的 7 条止于作业清单，登记归 Milvus（README 2.4）。优先级取值和顺序见 README 第 3 节，实现位置用 [modules.md](architecture/modules.md) 的包名。名词（段、列组、Manifest、快照、backfill）沿用 README 第 0 节。

## 1 表读

读全程不经 Milvus 服务：快照定视图，列式 reader 出批，剪枝和谓词求值都在核心层。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| R1 | collection 是一张表 | `spark.table("milvus.db.coll")`；SQL 里直接写三段名 | spark.catalog | C1 | P1 |
| R2 | 读固定快照 | loadTable 的 `version` 指快照名，`timestamp` 指时间点；缺省最新 | core.snapshot、spark.catalog | 快照无保留策略，旧快照的段可能已被 compaction 或 GC 回收（README 第 5 节） | P0 |
| R3 | 读不经 Milvus 服务 | 只配对象存储凭证即可读 | core.snapshot、core.path、core.credential、core.read | 靠 list `snapshots/{coll}/metadata/` 前缀选快照；catalog 文件是 README 第 5 节的 ask；凭证在 executor 上按需刷新，不是 driver 下发一次的静态值（长作业会过期） | P0 |
| R4 | 列式扫描，向量零拷贝 | 自动 | core.read.exec、spark.scan、native-storage | 列组文件只认 Parquet；Vortex 列组见第 9 节 | P0 |
| R5 | 列裁剪 | `select` | spark.scan → core.manifest 选列组 → core.read.plan | | P0 |
| R6 | 谓词下推：Spark 谓词 | `where` 里的比较、IN、IS NULL、字符串前后缀、AND、OR、NOT | spark.expr 翻成 IR → core.expr 求值 | 只实现 DataSource V2 谓词，不实现 V1 Filter（第 9 节） | P1 |
| R7 | 谓词下推：Milvus 表达式 | 表 option `milvus.filter`（2.0 新增），字符串按 Milvus 文法 | core.expr 用 Milvus 的表达式文法（Plan.g4）解析并求值 | JSON、Array、json_contains 语义按 Milvus 源码逐条复刻 | P1 |
| R8 | 删除生效 | 自动；快照时间戳之前的删除 | core.delete | 两种删除文件格式的共存期见决策 13；1.x 的 `milvus.read.apply.deletes` 是去是留一并定 | P0 |
| R9 | 段级剪枝 | 自动；主键等值和 IN 用段统计文件里的布隆过滤器剪段 | core.stats 出剪枝结果，core.snapshot 过滤段列表 | Milvus 侧写统计（README 第 5 节） | P1 |
| R10 | row group 级剪枝 | 自动；标量列 min/max | core.stats 出剪枝结果，core.read.plan 执行 | 同上 | P1 |
| R11 | Limit 下推 | `limit` | spark.scan | | P1 |
| R18 | 运行时过滤 | 自动；join 侧的过滤值下推到段和 row group | spark.scan（SupportsRuntimeV2Filtering）→ core.stats | R9、R10 的统计到位 | P1 |
| R12 | 元数据列 | `_segment_id`、`_row_offset`、`_timestamp`；`partition` 列是否保留、用 `_` 还是 1.x 的 `$` 见决策 5 | spark.table 声明，spark.scan 拼进批和行 | | P1 |
| R13 | 表统计 | 自动；行数和字节数给 Spark 选 join 策略 | spark.table ← core.snapshot | | P1 |
| R14 | 回表 | 下游算子按 (段 id, 行号) 取列 | core.read.exec 的 take | R12 | P1 |
| R15 | 类型覆盖 | 标量、VarChar、JSON、Array、Float/Float16/BFloat16/Int8/Binary/Sparse 向量、Text（大对象只在列批里放引用，正文按需取；引用带正文字节数，写侧才能在值还只有几百字节时按真实大小顶批量上限）、nullable 向量（变长 Binary，压紧后生成 valid 位图，非零拷贝） | core.schema 定 Milvus 与 Arrow 的映射，spark.types 定 Arrow 与 Spark 的映射 | 透传还是转换见决策 6 | P0 |
| R16 | 分区和段选择 | option `milvus.partitions`、`milvus.segments` | spark.scan → core.snapshot 过滤段列表 | 1.x 的 `milvus.partition.name`、`milvus.partition.id`、`milvus.segment.id` 映射到这两个键，见 K4 | P1 |
| R19 | 按分区报分区 | 自动；同一分区的段落在同一个 Spark 分区，join 少一次 shuffle | spark.scan 的 SupportsReportPartitioning → core.read.plan | 只能按 partition id 分组，段内主键无序，做不到列级；收益待实测 | 待评估 |
| R17 | 交付下游列式算子 | 列批、向量 buffer 地址、位图 | core.read.exec 的出口 | 出口是否压掉被过滤的行见决策 12；交给原生消费者的签名用裸 long 地址，不用 Arrow 的 Java 类型，否则调用方被绑死在我们 classloader 里的 Arrow 版本 | P0 |

## 2 表写

写止于作业清单：W1、W4 等 Milvus 侧的 RegisterSegments RPC（README 第 5 节），其余可先做。登记规则只在 A4 写一次。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| W1 | append 写新段 | `df.writeTo("milvus.db.coll").append()` | spark.write、core.write.exec、core.write.commit | 登记见 A4；RegisterSegments 未到位前不能交付，写出的段留在暂存前缀 | P1 |
| W2 | backfill 只写新列组 | `.option("milvus.write.mode","backfill").option("milvus.write.columns","f")` | spark.write、core.write | AddCollectionField 先于登记；目标段必须 Flushed；段的 base_path 和 Manifest 版本只能从快照 metadata 取（README 第 5 节缺 API）；无段级冻结，与 compaction、索引、schema 变更竞争；写侧分布与排序见决策 10；登记见 A4 | P2 |
| W3 | 原子提交 | 自动；暂存前缀、作业清单、幂等 commit、abort 清理 | core.write.commit | 暂存前缀避开 `insert_log`，否则 86400 秒后被 GC 回收 | P1 |
| W4 | truncate 和 overwrite | `.overwrite()`，只接受全表 | spark.write | 登记见 A4 | P1 |
| W5 | DELETE | `DELETE FROM milvus.db.coll WHERE ...`，只接能翻成 Milvus 表达式的谓词 | spark.table 的 DeleteV2 → core.expr 的 ExprPrinter → client.api | Milvus 在线 | P1 |
| W6 | 索引随段写出 | 写 option `milvus.index.<field>=HNSW,...` | spark.options 校验，core.index 编码与登记，native-vector 建索引 | Milvus 认 Manifest 里的索引登记（README 第 5 节）。三条约束：分片按段 id 的连续区间切，不交错；规划与构建钉同一个快照版本，提交时才碰活的元数据；调优参数只在 Spark 层消费，不透传给 knowhere | P2 |
| W7 | 小批量 gRPC 写入 | `format("milvus")` 的旧路径 | apps.legacy | 迁入前先修 abort：task 级 abort 必须丢弃缓冲，不得刷进 Milvus | P3 |

## 3 目录与 DDL

目录和 DDL 都经 Milvus 在线服务，是 Connector 调用 Milvus 的三处之一。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| C1 | 数据库和 collection 目录 | `SHOW NAMESPACES`、`SHOW TABLES`、`DESCRIBE TABLE` | spark.catalog → client.api | client 新增 ListDatabases、ShowCollections | P1 |
| C2 | 建表删表 | `CREATE TABLE milvus.db.coll (...) TBLPROPERTIES (...)`、`DROP TABLE` | spark.catalog → client.api | 向量维度、主键、索引参数走表属性 | P1 |
| C3 | schema 来自快照 | 自动 | core.schema | | P0 |

## 4 CALL

Table 接口表达不了的动作走 CALL：Spark 4 用 ProcedureCatalog，Spark 3.5 是同名函数。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| A1 | 快照 | `CALL milvus.system.create_snapshot('db.coll')`，drop、list、describe | spark.procedure → client.api | 建前是否先 Flush 见决策 11 | P1 |
| A2 | 索引 | `create_index`、`drop_index`，可等待完成 | spark.procedure → client.api | client 新增 CreateIndex、DropIndex、DescribeIndex | P1 |
| A3 | 生命周期 | `load`、`release`、`flush`、`compact` | spark.procedure → client.api | client 新增 LoadCollection、ReleaseCollection、ManualCompaction | P1 |
| A4 | 登记 | `register('db.coll', staging => 's3://.../staging/<job-id>')`：读作业清单后登记 | spark.procedure → core.write.commit、client.api | backfill 分支走 BatchUpdateManifest，可先做；append 分支走 RegisterSegments，待 Milvus 新增 | P1 / append 待定 |
| A5 | 描述 | `describe`：schema、段数、索引状态 | spark.procedure → client.api | | P1 |
| A7 | 清理暂存 | `cleanup_staging('db.coll')`：删掉没登记成的作业前缀 | spark.procedure → core.write.commit | 作业被 kill 时 abort 不执行，暂存前缀会留垃圾 | P1 |
| A6 | 读时自动建快照 | 表 option `milvus.snapshot.auto`，读完按 `milvus.snapshot.auto.cleanup` 删 | spark.catalog → client.api | 1.x live 快路径的等价物；与决策 11 一并定 | P1 |

## 5 向量与索引

向量能力整组排 P2：加载、写出、缓存都要先有 knowhere 的 C 封装。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| V1 | knowhere 封装 | 仓库内经 core.index 调用；下游算子直接依赖 native-vector | native-vector | knowhere 的 C shim | P2 |
| V2 | 加载 Milvus 建的索引 | 自动；按快照或 Manifest 里的索引文件 | core.index | 索引文件路径登记在 Manifest 里（README 第 5 节） | P2 |
| V4 | 索引来源与缓存 | 自动；Milvus 建的、Spark 写回的、任务内即时建的三级，按 (build id, 索引版本, 段, 字段) 缓存 | core.index | | P2 |
| V5 | 暴力搜索 | 入口与归属见决策 16 | native-vector 的 bruteforce；1.x 的 JVM 实现留在 apps.search 作对拍 | 决策 16 | P2 |

## 6 兼容入口

四条兼容入口不进主路径，产出核心层的 Snapshot 或 SegmentReader 后与主路径合流。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| K1 | Storage V2 packed 段 | 自动识别 storage_version=2 的段 | compat.v2packed 实现 core.read 的 SegmentReader | 分发靠 core 的注册表，不能让 core 依赖 compat | P0 |
| K2 | 离线 option 塞段列表 | 1.x 的 `milvus.snapshot.manifests` 等 | compat.offline 实现 SnapshotSource | backfill 读原表今天走它 | P0 |
| K3 | backup 目录 | `milvus.backup.dir` | compat.backup 实现 SnapshotSource | | P1 |
| K4 | 1.x option 名 | `milvus.collection.name`、`milvus.partition.name`、`milvus.partition.id`、`milvus.segment.id`、`milvus.extra.columns` 等 | spark.options | 映射到三段名和新 option，打告警 | P1 |

## 7 场景与工具

场景和工具在 apps 模块，各自是独立入口，删掉任何一个不影响其他。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| O1 | backfill 作业 | `spark-submit --class ...BackfillApp`，24 个 flag，结果 JSON | apps.backfill | 1.x 迁入，flag 照收（`--input-format` 只有 parquet 有实现）；内部改用 W2，因此不早于 W2 | P2 |
| O2 | 调试工具 | ListV2SegmentsApp、ReadSourceOnlyApp | apps.tools | 1.x 迁入 | P3 |

## 8 配置

配置分表 option、写 option、会话配置三级；1.x 的 89 个键在这里收敛并映射。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| G1 | 表 option | 快照名或时间点、`milvus.filter`、分区和段选择 | spark.options | R2、R7、R16 | P1 |
| G2 | 写 option | 写完自动建快照、单段文件大小上限、写模式与列、索引参数 | spark.options → core.write | W2、W6；替代 1.x 的 `milvus.writer.commitType`、`milvus.writer.fieldIds` | P1 |
| G3 | 会话配置：内存与批 | off-heap 预算、批大小、预取上限 | spark.options → core.read.exec | 替代 1.x 的 `milvus.insertMaxBatchSize`、`s3.preloadPoolSize` | P1 |
| G5 | 指标 | 会话配置开关；读写吞吐、拷贝次数、JNI 跨界耗时、native 内存占用 | native-storage 的 JNI 层留计数器，spark 层汇总 | 没有指标口径就量不出「拷贝 6 次降到 2 次」；native 分配的 buffer 归谁记账要和 G3 的 off-heap 预算对齐 | P1 |
| G4 | 会话配置：索引与 GPU | 索引缓存上限、GPU 开关、两套服务的地址和凭证 | spark.options → core.index、core.credential | V4；GPU 产物见 README 2.7 | P2 |

## 9 已知缺口

原子的 CREATE TABLE AS SELECT。Lance 这类元数据权威在格式内的表可以实现 Spark 的 StagedTable，建表和写入一次提交；Milvus Storage 的权威在 etcd 和 DataCoord，C2 建表走 gRPC、W1 的登记是独立的 CALL，两步之间必然有窗口。CTAS 失败会留下一张空表，用户要自己删。

## 10 不做

TopN 和 Aggregates 下推；UPDATE 和 MERGE；text_match 一族（依赖 tantivy 文本索引）；GIS 表达式；struct 数组表达式；random_sample；DataSource V1 Filter（1.x 走 V1，2.0 只实现 V2 谓词，见 R6）；Vortex 列组的读写（milvus-storage 支持，Connector 不接）。

## 11 尚未落地

这几条在第 1 到第 8 节里有行，但代码里还没有任何包声明承载它们。列在这里是让
`checkCapabilityIndex` 知道这是已知的空缺，不是索引漂移。落地时删掉对应行，并在目标包的
`package.scala` 里写上编号。

| 编号 | 为什么还没有 |
|---|---|
| R19 | 按分区报分区，优先级是「待评估」。收益要实测，见 README 第 4 节决策 19 |
| A7 | 清理暂存要 `spark.procedure`，那个包目前是空的，等第 3 层拆分 |
| G5 | 指标要 native-storage 的 JNI 层留计数器。第 1 层已经写了（读写两侧的 loon_* 封装加自己的加载器），计数器还没加 |
| R1 | 三段名要 `spark.catalog` 的 MilvusCatalog；四条线的 catalog 包都只有 package.scala。今天读表走 `format("milvus")` 加 option |
| R6 | Spark 谓词下推要 `spark.expr` 翻成 IR 再由 `core.expr` 求值，两个包都是零文件。现有下推在 MilvusDataSource 里且走 V1 的 `SupportsPushDownFilters`，接口选型见 README 第 4 节决策 20 |
| R7 | Milvus 表达式要 `core.expr` 按 Plan.g4 解析求值，零文件 |
| K2 | 离线 option 的段列表要 `compat.offline` 实现 SnapshotSource；包是空壳，规划代码仍在 `spark-base/sources`（modules.md 第 5 节记为过渡态） |
| W7 | gRPC 小批量写要归 `apps.legacy`；包是空壳，整条 `format("milvus")` 写链还在 `spark-base/write`，下放要先有 W7 的注册表 |
| A2 | 建索引、删索引的 CALL 要 `spark.procedure`（3.5 线是 `spark.functions`），全部是空壳；client 侧还要新增三个 RPC |
| A3 | load / release / flush / compact 的 CALL，同 A2；client 侧还要新增三个 RPC |
| A4 | 登记的 CALL 要 `spark.procedure` 加 `core.write.commit` 读作业清单，两端都是零文件；append 的登记 RPC 在 Milvus 侧也还没有（README 第 5 节） |
| A5 | describe 的 CALL，同 A2 |
| V1 | knowhere 的 C shim 与 JNI 要 `native-vector`，目前只有 package-info.java |
| V2 | 加载 Milvus 建的索引要 `core.index`，零文件 |
| V4 | 索引来源与缓存要 `core.index`，零文件 |

