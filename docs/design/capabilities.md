# 2.0 功能规划 `[草稿]`

51 条功能按用户入口分八组。读的 19 条全程不经 Milvus 服务；写的 7 条止于作业清单，登记归 Milvus（README 2.4）。优先级取值和顺序见 README 第 3 节，实现位置用 [modules.md](architecture/modules.md) 的包名。名词（段、列组、Manifest、快照、backfill）沿用 README 第 0 节。

## 1 表读

读全程不经 Milvus 服务：快照定视图，列式 reader 出批，剪枝和谓词求值都在核心层。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| R1 | collection 是一张表 | `spark.table("milvus.db.coll")`；SQL 里直接写三段名 | spark.catalog | 已知名字用现有 `getCollectionInfo` 取得 collection id；SHOW/LIST 另属 C1 | P1 |
| R2 | 读固定快照 | loadTable 的 `version` 指快照名，`timestamp` 指时间点；缺省最新 | core.snapshot、spark.catalog | 表加载（`getTable` / `loadTable`）解析一次，Table、schema、统计与 Scan 共用同一个 `Snapshot`；快照无保留策略，旧快照的段可能已被 compaction 或 GC 回收（README 第 5 节） | P0 |
| R3 | 读不经 Milvus 服务 | 只配对象存储凭证即可读 | core.snapshot、core.path、core.credential、core.read | driver 的每个 ObjectStore 只活到 Snapshot 或删除文件清单物化完成，成功失败都关闭；Milvus 元数据里的 `s3://endpoint:port/bucket/key` 只在已知元数据边界按已配置端点识别，用户 URI 仍按标准 S3 规则解析；凭证在 executor 上按需刷新，不由 driver 下发一次性的静态值 | P0 |
| R4 | 列式扫描，向量可零拷贝 | 默认 `milvus.read.columnar=true`，设为 false 使用行式；向量原始字节再设 `milvus.read.vector.raw=true`；向量搜索仍走行式 | core.read.exec、spark.read、native-storage | 行式与列式消费同一个 `SegmentReader`；已知行数的段在读到 EOF 时统一核对物理输出行数，短读直接失败；列组文件只认 Parquet，Vortex 列组见第 10 节 | P0 |
| R5 | 列裁剪 | `select`，可再用 `fieldIDs` 限定数值字段 id | spark.read → core.manifest 选列组 → core.read.plan | 字段 id 只取自快照 schema 的 `milvus.field_id` metadata；`fieldIDs` 在 inferSchema 和 Table 都生效，外部 schema 必须选同一组 id 且名称、类型不冲突；缺失、未知或非数值 id 报错，不按列序号猜 | P0 |
| R6 | 谓词下推：Spark 谓词 | `where` 里的比较、IN、IS NULL、字符串前后缀、AND、OR、NOT | spark.expr 翻成 IR → core.expr 求值 | 只实现 DataSource V2 谓词，不实现 V1 Filter（第 9 节） | P1 |
| R7 | 谓词下推：Milvus 表达式 | 已实现 MilvusSearch 的 filter 标量子集；表 option `milvus.filter` 仍待接入 | core.expr 用 Milvus 的表达式文法（Plan.g4）解析并求值 | 当前比较、IN、IS NULL、逻辑运算；JSON、Array、json_contains 待按 Milvus 源码逐条复刻 | P1 |
| R8 | 删除生效 | 默认自动；`milvus.read.apply.deletes=false` 可显式关闭 | core.delete、core.read.plan、core.read.exec | driver 只把段内、本分区 L0 与全 collection L0 的删除文件描述装进任务；executor 读取、合并并关闭这些文件，读不到就让 task 失败，不返回空删除计划。`_delta/` 两种编码都认（决策 13 撤销）。backfill 按物理行对齐列组时不应用删除，那是 W2 内部读法 | P0 |
| R9 | 段级剪枝 | 自动；主键等值和 IN 用段统计文件里的布隆过滤器剪段 | core.stats 出剪枝结果，core.snapshot 过滤段列表 | Milvus 侧写统计（README 第 5 节） | P1 |
| R10 | row group 级剪枝 | 自动；标量列 min/max | core.stats 出剪枝结果，core.read.plan 执行 | 同上 | P1 |
| R11 | Limit 下推 | `limit` | spark.read | | P1 |
| R18 | 运行时过滤 | 自动；join 侧的过滤值下推到段和 row group | spark.read（SupportsRuntimeV2Filtering）→ core.stats | R9、R10 的统计到位 | P1 |
| R12 | 元数据列 | `_segment_id`、`_row_offset`、`_timestamp`，按此固定顺序附加；`_` 前缀，不留 `partition` 列（决策 5 已定） | spark.table 声明，spark.read 拼进批和行 | `_segment_id`、`_row_offset` 由任务与 reader 位置产生；`_timestamp` 是存储里的系统字段 id 1，按 V2/V3 各自列名读取，不在 Spark 侧合成 | P1 |
| R13 | 表统计 | 自动；行数和字节数给 Spark 选 join 策略 | spark.read ← core.read.plan | | P1 |
| R14 | 回表 | 下游算子按 (段 id, 行号) 取列 | core.read.exec 的 take | R12 | P1 |
| R15 | 类型覆盖 | Bool、Int8/16/32/64、Float、Double、String、VarChar、Text、JSON、标量 Array、Float/Float16/BFloat16/Int8/Binary/Sparse 向量；nullable 与字段 metadata 保持快照定义 | core.schema 定 Milvus 与 Arrow 的映射，spark.types 定 Arrow 与 Spark 的映射 | 一份映射同时给快照 schema、Table 和 reader；Geometry、Timestamptz、ArrayOfVector/struct-array 与未知类型尚未实现，必须失败，不能退成 Binary 或 null；向量透传还是转换见决策 6 | P0 |
| R16 | 分区和段选择 | option `milvus.partitions`、`milvus.segments`，逗号分隔数值 id | spark.options → core.snapshot 过滤段列表 | 两个选择器在所有 SnapshotSource 解析后统一应用，可组合取交集；每个请求 id 必须存在；收窄数据段时保留适用于所选分区的 L0 与全 collection L0 删除段 | P1 |
| R19 | 按分区报分区 | 自动；同一分区的段落在同一个 Spark 分区，join 少一次 shuffle | spark.read 的 SupportsReportPartitioning → core.read.plan | 只能按 partition id 分组，段内主键无序，做不到列级；收益待实测 | 待评估 |
| R17 | 交付下游列式算子 | 列批、向量 buffer 地址、位图 | core.read.exec 的出口 | 出口是否压掉被过滤的行见决策 12；交给原生消费者的签名用裸 long 地址，不用 Arrow 的 Java 类型，否则调用方被绑死在我们 classloader 里的 Arrow 版本 | P0 |

## 2 表写

写止于作业清单：W1 等 Milvus 侧的 RegisterSegments RPC（README 第 5 节），其余可先做。登记规则只在 A4 写一次。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| W1 | append 写新段 | `df.writeTo("milvus.db.coll").append()`；catalog（R1）落地前是 `df.write.format("milvus").mode("append")`（2026-09-15 接通，UAT 验过），设计见 [write.html](architecture/write.html) | spark.write、core.write.exec、core.write.commit | 登记见 A4；RegisterSegments 未到位前不能交付，写出的段留在暂存前缀。连接器已按 Milvus 的策略切分列组，并写出主键 bloom-filter 统计；仍缺系统字段 RowID（0）和 Timestamp（1），因此当前生成的段还不是可由 Milvus 登记并加载的完整 V3 段 | P1 |
| W2 | backfill 只写新列组 | `.option("milvus.write.mode","backfill").option("milvus.write.columns","f")` | spark.write、core.write | AddCollectionField 先于登记；目标段必须 Flushed；段的 base_path 和 Manifest 版本只能从快照 metadata 取（README 第 5 节缺 API）；无段级冻结，与 compaction、索引、schema 变更竞争；写侧分布与排序见决策 10；登记见 A4 | P2 |
| W3 | 原子提交 | 自动；暂存前缀、作业清单、幂等 commit、abort 清理 | core.write.commit | 暂存前缀避开 `insert_log`，否则 86400 秒后被 GC 回收 | P1 |
| W6 | 索引随段写出 | 写 option `milvus.index.<field>=HNSW,...` | spark.options 校验，core.index 编码与登记，native-vector 建索引 | Milvus 认 Manifest 里的索引登记（README 第 5 节）。三条约束：分片按段 id 的连续区间切，不交错；规划与构建钉同一个快照版本，提交时才碰活的元数据；调优参数只在 Spark 层消费，不透传给 knowhere | P2 |

## 3 目录与 DDL

目录和 DDL 都经 Milvus 在线服务，是 Connector 调用 Milvus 的三处之一。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| C1 | 数据库和 collection 目录 | `SHOW NAMESPACES IN milvus`、`SHOW TABLES IN milvus.db` | spark.catalog → client.api | 已实现：ListDatabases 把 database 映射成单层 namespace，ShowCollections 列该 database 的全部 collection；目录不读快照或对象存储，只把确认不存在翻成 Spark 的 namespace 不存在语义；已知表的 `DESCRIBE TABLE` 由 R1 加载 | P1 |
| C2 | 建表删表 | `CREATE TABLE milvus.db.coll (...) TBLPROPERTIES (...)`、`DROP TABLE` | spark.catalog → client.api | 向量维度、主键、索引参数走表属性 | P1 |
| C3 | schema 来自快照 | 自动 | core.schema | | P0 |

## 4 CALL

Table 接口表达不了的动作走 CALL：四条线走同一个 SQL 语法扩展（决策日志 2026-09-10，设计见 [procedure.html](architecture/procedure.html)），不用 Spark 4.0 才有的 ProcedureCatalog，3.5 线不另做同名函数。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| A1 | 快照 | `CALL milvus.system.create_snapshot('db.coll')`，drop、list、describe | spark.procedure → client.api | 建前是否先 Flush 见决策 11 | P1 |
| A2 | 索引 | `create_index`、`drop_index`，可等待完成 | spark.procedure → client.api | client 新增 CreateIndex、DropIndex、DescribeIndex | P1 |
| A3 | 生命周期 | `load`、`release`、`flush`、`compact` | spark.procedure → client.api | client 新增 LoadCollection、ReleaseCollection、ManualCompaction | P1 |
| A4 | 登记 | `CALL milvus.system.register('db.coll', staging => '{root}/staging/<job-id>', \`milvus.uri\` => ..., \`fs.*\` => ...)`（#17，2026-09-16 起，四条线）；Scala 入口 `Register.run` | spark.extensions → spark.procedure → core.write.commit、client.api | backfill 分支已通（2026-09-15 #16）：`spark.procedure.Register.run` 读作业清单，`client.api.batchUpdateManifest` 走 Milvus 3.0 公开的 BatchUpdateManifest，UAT 上 backfill → register → 在线查到新列；SQL 的 `CALL` 前端未接（#17），先是 Scala 入口。append 分支走 RegisterSegments，待 Milvus 新增 | P1 / append 待定 |
| A5 | 描述 | `describe`：schema、段数、索引状态 | spark.procedure → client.api | | P1 |
| A7 | 清理暂存 | `cleanup_staging('db.coll')`：删掉没登记成的作业前缀 | spark.procedure → core.write.commit | 作业被 kill 时 abort 不执行，暂存前缀会留垃圾 | P1 |

## 5 向量与索引

向量能力整组排 P2：原生接口采用 Knowhere PR #1829 的固定提交。V1 当前交付库加载、C ABI 校验及版本查询；V5 为现有逐段查询接入原生 BruteForce。索引文件加载、写出、缓存仍待实现。

issue #125 的[索引查询开发方案](architecture/vector-search.html)中，[库加载契约](architecture/vector-search.html#library-loading)已经确定；V1 的索引执行、V2 和 V4 的持久化加载部分及提议的 V7 集合级向量查询仍待评审。V7 尚未加入正式能力行，实现前须先确定 README 第 4 节的相关决策。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| V1 | Knowhere 接入 | NativeVectorLibrary 加载及版本查询；NativeVectorIndex 包装持久化加载和搜索 | native-vector | 固定 Knowhere PR #1829 的 Java/JNI/原生产物，C ABI=1；Cardinal 文件要求对应构建特性 | P2 |
| V2 | 加载 Milvus 建的索引 | 自动；按快照或 Manifest 里的索引文件 | core.index | 保留快照段记录的 index_files；使用 Manifest 索引登记时须核验目标版本（README 第 5 节）。加载契约见[方案](architecture/vector-search.html#metadata) | P2 |
| V4 | 索引来源与缓存 | 自动；Milvus 建的、Spark 写回的、任务内即时建的三级，按 (build id, 索引版本, 段, 字段) 缓存 | core.index | | P2 |
| V5 | 暴力搜索 | 现有 vector.search.*，每段 TopK | spark.read、core.index、native-vector | 上游 Knowhere.bruteForce；[执行设计](architecture/vector-search.html#native-brute-force)；集合级入口仍见决策 16 | P2 |
| V7 | 持久化索引向量查询 | MilvusSearch.search 返回带全局 TopK 的 DataFrame | spark.read、core.index | V2、R7、SegmentReader.take；[查询契约](architecture/vector-search.html#api) | P2 |

## 6 兼容入口

四条兼容入口不进主路径，产出核心层的 Snapshot 或 SegmentReader 后与主路径合流。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| K1 | Storage V2 packed 段 | 自动识别 storage_version=2 的段 | core.read.exec 按 SegmentLayout 开同一个 SegmentReader；compat.v2 只在 driver 上恢复列组布局；spark.read 一个行式 reader，列名规则在 V2ColumnBinding | 分发靠 core 的注册表，不能让 core 依赖 compat | P0 |
| K3 | backup 目录 | `milvus.backup.dir` | compat.backup 实现 SnapshotSource | | P1 |

## 7 场景与工具

场景和工具在 apps 模块，各自是独立入口，删掉任何一个不影响其他。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| O1 | backfill 作业 | `spark-submit --class ...BackfillApp`，24 个 flag，结果 JSON | apps.backfill | flag 集按 W2 重新定义，与 zilliz-cloud 的调用方对接；不早于 W2 | P2 |

## 8 配置

配置分表 option、写 option、会话配置三级；不兼容 1.x 的键名（决策日志 2026-09-14）。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| G1 | 表 option | 快照名或时间点、`milvus.filter`、分区和段选择 | spark.options | R2、R7、R16；布尔值只认 true/false，正数与数值 id 严格解析，`vector.search.query` / `topK` 必须成对且格式有效 | P1 |
| G2 | 写 option | 写完自动建快照、单段文件大小上限、写模式与列、索引参数 | spark.options → core.write | W2、W6；替代 1.x 的 `milvus.writer.commitType`（`milvus.writer.fieldIds` 与 `vector.<f>.dim` 已于 2026-09-15 删除，字段 id 和维度从 collection schema 取） | P1 |
| G3 | 会话配置：内存与批 | off-heap 预算、批大小、预取上限 | spark.options → core.read.exec | 替代 1.x 的 `milvus.insertMaxBatchSize`、`s3.preloadPoolSize` | P1 |
| G5 | 指标 | 自动，Spark SQL 页的 scan / write 节点：JNI 调用次数与耗时、过界的 Arrow 批数与字节数、C 侧拷贝次数与字节数、物化成 InternalRow 的行数、allocator 峰值；不设开关（决策日志 2026-09-16） | native-storage 的 holder 计数，core.read.exec 的 ReadMetrics、core.write.exec 的 WriteMetrics，spark.metrics 翻成 CustomMetric | 设计见 [storage-io.html 第五节](architecture/storage-io.html#metrics)；量不到的两处（对象存储读取字节、native 内存总量）写在那里；allocator 峰值就是 G3 预算要卡的数 | P1 |
| G4 | 会话配置：索引与 GPU | 索引缓存上限、GPU 开关、两套服务的地址和凭证 | spark.options → core.index、core.credential | V4；GPU 产物见 README 2.7 | P2 |

## 9 已知缺口

原子的 CREATE TABLE AS SELECT。Lance 这类元数据权威在格式内的表可以实现 Spark 的 StagedTable，建表和写入一次提交；Milvus Storage 的权威在 etcd 和 DataCoord，C2 建表走 gRPC、W1 的登记是独立的 CALL，两步之间必然有窗口。CTAS 失败会留下一张空表，用户要自己删。

## 10 不做

TopN 和 Aggregates 下推；UPDATE 和 MERGE；text_match 一族（依赖 tantivy 文本索引）；GIS 表达式；struct 数组表达式；random_sample；DataSource V1 Filter（1.x 走 V1，2.0 只实现 V2 谓词，见 R6）；Vortex 列组的读写（milvus-storage 支持，Connector 不接）；truncate 和 overwrite（2026-09-16 定，原 W4）：`mode("overwrite")` 是 Spark ETL 模板里的常见写法，接进来后改个表名就让生产 collection 的全部旧数据不可见，Milvus 没有回滚；不接时 Spark 在分析阶段报 `Table does not support overwrite`，数据不动，全量刷新改为在 Milvus 侧 drop/recreate 或 SDK 全表 delete 后再 append，误操作要在 Milvus 的界面或 SDK 里显式做。1.x 没有这个能力，不接不是退化；以后有明确需求再加，加是兼容的，加了再拿掉不是。DELETE（2026-09-16 定，原 W5）：`DELETE FROM milvus.db.coll WHERE ...` 会把谓词翻成 Milvus 表达式后调 Milvus 的 Delete RPC，删的是生产 collection 的在线数据，谓词写宽了没有回滚；Milvus SDK 已经用同一套表达式语法提供 delete，连接器接进来只是把同一个删除动作换到 Spark 作业里发，多一个出错的地方，不多一种能力。连接器的职责是读写存储格式的文件，不碰在线数据的删除。不接的代价为零：用户在 SDK 里执行同一条表达式。随之 `core.expr` 不再需要把中间表示打印回 Milvus 语法的 ExprPrinter，决策 20 只剩 R6 和 R7 两条依赖。

## 11 尚未落地

这几条在第 1 到第 8 节里有行，但代码里还没有任何包声明承载它们。列在这里是让
`checkCapabilityIndex` 知道这是已知的空缺，不是索引漂移。落地时删掉对应行，并在目标包的
`package.scala` 里写上编号。

| 编号 | 为什么还没有 |
|---|---|
| R19 | 按分区报分区，优先级是「待评估」。收益要实测，见 README 第 4 节决策 19 |
| A7 | 清理暂存要 `spark.procedure`，那个包目前是空的，等第 3 层拆分 |
| R6 | Spark 谓词下推要 `spark.expr` 翻成 IR；`core.expr` 已有 R7 标量子集，Spark 翻译尚未落地。`MilvusScanBuilder` 暂时保留 V1 的 `SupportsPushDownFilters` 接口，但不接受任何 `Filter`，全部作为 residual 交还 Spark 求值；接口选型见 README 第 4 节决策 20 |
| A2 | 建索引、删索引的 CALL 要 `spark.procedure`（3.5 线是 `spark.functions`），全部是空壳；client 侧还要新增三个 RPC |
| A3 | load / release / flush / compact 的 CALL，同 A2；client 侧还要新增三个 RPC |
| A5 | describe 的 CALL，同 A2 |
| V4 | 持久化索引来源已跟随快照传递；跨任务缓存、其他来源仍未实现，当前每个查询任务持有并关闭自己的索引 |
