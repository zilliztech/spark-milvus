# 2.0 功能规划 `[草稿]`

48 条功能按用户入口分八组。读的 20 条全程不经 Milvus 服务；写的 5 条止于作业清单或快照，登记归 Milvus（README 2.4）。优先级取值和顺序见 README 第 3 节，实现位置用 [modules.md](architecture/modules.md) 的包名。名词（段、列组、Manifest、快照、backfill）沿用 README 第 0 节。

## 1 表读

读全程不经 Milvus 服务：快照定视图，列式 reader 出批，剪枝和谓词求值都在核心层。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| R1 | collection 是一张表 | `spark.table("milvus.db.coll")`；SQL 里直接写三段名 | spark.catalog | 已知名字用现有 `getCollectionInfo` 取得 collection id；SHOW/LIST 另属 C1 | P1 |
| R2 | 读固定快照 | loadTable 的 `version` 指快照名，`timestamp` 指时间点；缺省最新 | core.snapshot、spark.catalog | 表加载（`getTable` / `loadTable`）解析一次，Table、schema、统计与 Scan 共用同一个 `Snapshot`；快照无保留策略，旧快照的段可能已被 compaction 或 GC 回收（README 第 5 节） | P0 |
| R3 | 读不经 Milvus 服务 | 只配对象存储凭证即可读 | core.snapshot、core.path、core.credential、core.read | driver 的每个 ObjectStore 只活到 Snapshot 或删除文件清单物化完成，成功失败都关闭；Milvus 元数据里的 `s3://endpoint:port/bucket/key` 只在已知元数据边界按已配置端点识别，用户 URI 仍按标准 S3 规则解析；凭证在 executor 上按需刷新，不由 driver 下发一次性的静态值 | P0 |
| R4 | 列式扫描，向量可零拷贝 | 默认 `milvus.read.columnar=true`，设为 false 使用行式；向量原始字节再设 `milvus.read.vector.raw=true`；向量搜索仍走行式 | core.read.exec、spark.read、native-storage | 行式与列式消费同一个 `SegmentReader`，JNI 与逐批 Arrow 导出由 milvus-storage 上游提供；已知行数的段在读到 EOF 时统一核对物理输出行数，短读直接失败；列组文件只认 Parquet，Vortex 列组见第 10 节 | P0 |
| R5 | 列裁剪 | `select`，可再用 `fieldIDs` 限定数值字段 id | spark.read → core.manifest 选列组 → core.read.plan | 字段 id 只取自快照 schema 的 `milvus.field_id` metadata；`fieldIDs` 在 inferSchema 和 Table 都生效，外部 schema 必须选同一组 id 且名称、类型不冲突；缺失、未知或非数值 id 报错，不按列序号猜 | P0 |
| R6 | 谓词下推：Spark 谓词 | `where` 里的比较、IN、IS NULL、字符串前后缀、AND、OR、NOT；设计见 [expressions.html](architecture/expressions.html) | spark.expr 翻成 IR → core.expr 求值 | 只实现 DataSource V2 谓词，不实现 V1 Filter（第 10 节）；每个顶层谓词完整翻译才接受，否则整棵留给 Spark；向量搜索读取全部 residual | P1 |
| R7 | 谓词下推：Milvus 表达式 | 普通表 option `milvus.filter`；`MilvusSearch.search` 的 `filter` | spark.options 严格解析 → spark.read 绑定隐藏列 → core.expr 按 Milvus 标量语义求值 | 已实现比较、IN、IS NULL 与逻辑运算，行式/列式及 V2/V3 共用；普通表 filter 与 Spark predicate、删除、Limit 组合；JSON、Array、json_contains 等开放决策 23 | P1 |
| R8 | 删除生效 | 默认自动；`milvus.read.apply.deletes=false` 可显式关闭 | core.delete、core.read.plan、core.read.exec | driver 只把段内、本分区 L0 与全 collection L0 的删除文件描述装进任务；executor 读取、合并并关闭这些文件，读不到就让 task 失败，不返回空删除计划。`_delta/` 两种编码都认（决策 13 撤销）。backfill 按物理行对齐列组时不应用删除，那是 W2 内部读法 | P0 |
| R9 | 段级剪枝 | 自动；主键等值和 IN 用段统计文件里的布隆过滤器剪段 | core.stats 出剪枝结果，core.snapshot 过滤段列表 | V2 读 <code>statslog_files</code>，V3 读钉住 Manifest 的 <code>stats</code>；单 object 与 compound array 都认，任一统计缺失、损坏或不匹配则保留段 | P1 |
| R10 | row group 级剪枝 | 自动；标量列 min/max | core.stats 出剪枝结果，core.read.plan 执行 | 仍需 milvus-storage 写 row-group 统计并提供可指定 row group 的 reader 入口；本仓库不用段级 Bloom 模拟 | P1 |
| R11 | Limit 下推 | `limit` | spark.read | | P1 |
| R18 | 运行时过滤 | 自动；join 侧的主键值在固定快照的普通扫描上下推到段；向量 TopK 不接入 | spark.read（SupportsRuntimeV2Filtering）→ core.stats | R9；重复调用累计取交集并复用段统计缓存，不声称 R10 row-group 剪枝 | P1 |
| R12 | 元数据列 | `_segment_id`、`_row_offset`、`_timestamp`，按此固定顺序附加；`_` 前缀，不留 `partition` 列（决策 5 已定） | spark.table 声明，spark.read 拼进批和行 | `_segment_id`、`_row_offset` 由任务与 reader 位置产生；`_timestamp` 是存储里的系统字段 id 1，按 V2/V3 各自列名读取，不在 Spark 侧合成 | P1 |
| R13 | 表统计 | 自动；行数和字节数给 Spark 选 join 策略 | spark.read ← core.read.plan | | P1 |
| R14 | 回表 | 下游算子按 (段 id, 行号) 取列 | core.read.exec 的 take | R12 | P1 |
| R15 | 类型覆盖 | Bool、Int8/16/32/64、Float、Double、String、VarChar、Text、JSON、标量 Array、Float/Float16/BFloat16/Int8/Binary/Sparse 向量；nullable 与字段 metadata 保持快照定义 | core.schema 定 Milvus 与 Arrow 的映射，spark.types 定 Arrow 与 Spark 的映射 | 一份映射同时给快照 schema、Table 和 reader；Geometry、Timestamptz、ArrayOfVector/struct-array 与未知类型尚未实现，必须失败，不能退成 Binary 或 null；向量透传还是转换见决策 6 | P0 |
| R16 | 分区和段选择 | option `milvus.partitions`、`milvus.segments`，逗号分隔数值 id | spark.options → core.snapshot 过滤段列表 | 两个选择器在所有 SnapshotSource 解析后统一应用，可组合取交集；每个请求 id 必须存在；收窄数据段时保留适用于所选分区的 L0 与全 collection L0 删除段 | P1 |
| R19 | 按分区报分区 | 自动；同一分区的段落在同一个 Spark 分区，join 少一次 shuffle | spark.read 的 SupportsReportPartitioning → core.read.plan | 只能按 partition id 分组，段内主键无序，做不到列级；收益待实测 | 待评估 |
| R17 | 交付下游列式算子 | 列批、向量 buffer 地址、位图 | core.read.exec 的出口 | 部分实现：出口目前是 Arrow `VectorSchemaRoot`，由 spark.read 包成 Spark `ColumnarBatch`；面向原生消费者的裸 long 地址加位图签名尚未写。出口是否压掉被过滤的行见决策 12；交给原生消费者的签名用裸 long 地址，不用 Arrow 的 Java 类型，否则调用方被绑死在我们 classloader 里的 Arrow 版本 | P0 |
| R20 | 读 external collection | 与普通 collection 相同的入口：快照目录或 client 模式；不接 backup 与 option 串来源 | core.snapshot、core.credential、core.schema、core.read.exec、spark.read、spark.types | Milvus 3.0 起；五种格式 parquet、vortex、iceberg-table、lance-table、milvus-table；只经 milvus-storage 读；列名、读取 schema、类型、系统字段四条规则与普通 collection 统一，外表另需 `extfs.<collectionID>.*` 凭证；段一律用空 schema 打开，依赖 milvus-storage 的 `loon_reader_new` 接受空 schema（决策日志 2026-09-16）；外表只读，写路径不对它声明 BATCH_WRITE；外表段的索引 label 与段内行号的对应要用 Milvus 生成的文件单独验证。设计见 [snapshot.html 3.1](architecture/snapshot.html#external)、[read.html 1.1 与 6.4](architecture/read.html#rules)、[storage-auth.html 3.4](architecture/storage-auth.html#external) | P0 |

## 2 表写

写止于作业清单或快照：W1 等 Milvus 侧的 RegisterSegments RPC（README 第 5 节），W8 把写出的段连同索引写成快照、由 Milvus 恢复成新 collection，其余可先做。登记规则只在 A4 写一次。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| W1 | append 写新段 | `df.writeTo("milvus.db.coll").append()`；catalog（R1）落地前是 `df.write.format("milvus").mode("append")`（2026-09-15 接通，UAT 验过），设计见 [write.html](architecture/write.html) | spark.write、core.write.exec、core.write.commit | 登记见 A4；RegisterSegments 未到位前不能交付，写出的段留在暂存前缀。连接器已按 Milvus 的策略切分列组，并写出主键 bloom-filter 统计；系统字段 RowID（0）与 Timestamp（1）按决策 22 写占位值（段内行号、作业时间的 HybridTS），三项前提齐了，能否登记取决于 Milvus 的 RegisterSegments | P1 |
| W2 | backfill 只写新列组 | `.option("milvus.write.mode","backfill").option("milvus.write.columns","f")` | spark.write、core.write | AddCollectionField 先于登记；目标段必须 Flushed；段的 base_path 和 Manifest 版本只能从快照 metadata 取（README 第 5 节缺 API）；无段级冻结，与 compaction、索引、schema 变更竞争；写侧分布与排序见决策 10；登记见 A4 | P2 |
| W3 | 原子提交 | 自动；暂存前缀、作业清单、幂等 commit、abort 清理 | core.write.commit | 暂存前缀避开 `insert_log`，否则 86400 秒后被 GC 回收 | P1 |
| W6 | 建索引 | `CALL milvus.system.build_index(...)`：driver 另起 Spark 作业，回读已写出的段建向量索引；不设写 option（决策日志 2026-09-17） | spark.procedure → core.index 构建，core.codec 编码索引文件，core.write.commit 记录索引，native-vector 调 Knowhere | 交付途径二选一：W8 的快照恢复（只能是新 collection），或 Milvus 认 Manifest 里的索引登记（README 第 5 节）。云上的索引要用含 Cardinal 的 Knowhere 构建，该产物只在云上作业运行时提供，不进公开产物。三条约束：分片按段 id 的连续区间切，不交错；规划与构建钉同一个快照版本，提交时才碰活的元数据；调优参数只在 Spark 层消费，不透传给 knowhere。设计见 [vector-search.html 第 2.7 节](architecture/vector-search.html#build) | P2 |
| W8 | 写快照，恢复成新 collection | `CALL milvus.system.write_snapshot(...)` 写出快照文件；恢复成新 collection 待 `restore_snapshot` | core.write.commit 写快照 JSON 与段 Avro 清单（复用 core.snapshot.json 的形状），client.api 调外部快照恢复，spark.procedure | Milvus master 的 RestoreSnapshot 带 external 标志：目标 collection 必须不存在，恢复时复制段文件与向量、标量、文本、JSON 索引文件并重新分配 build id；3.0.x 是否包含未核对。段必须是 Milvus 能加载的完整段，受决策 22 约束。已实现写文件一半：`SnapshotWriter` 把源快照的文档与每段的 Avro 清单原样复制，只替换索引登记、索引定义、build_ids、segment_ids 和快照自己的 name/id/create_ts，因此段的通道、位置、提交时间戳、数据与删除文件都不是猜的，V2 与 V3 段一视同仁。2026-09-20 实测：Milvus v3.0.2 的 `RestoreExternalSnapshot` 接受这份快照并恢复成新 collection，索引直接可用、不重建，搜索结果与独立基准一致。外部恢复要求快照元数据 URI 推出的根覆盖所有数据路径，所以 `output` 必须是数据路径的祖先前缀（为已有 collection 建索引即实例自己的根）。连接器这一侧调 `restore_snapshot` 的过程仍未实现，见 [vector-search.html 2.7](architecture/vector-search.html#build) | P2 |

## 3 目录与 DDL

目录和 DDL 都经 Milvus 在线服务，是 Connector 调用 Milvus 的三处之一。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| C1 | 数据库和 collection 目录 | `SHOW NAMESPACES IN milvus`、`SHOW TABLES IN milvus.db` | spark.catalog → client.api | 已实现：ListDatabases 把 database 映射成单层 namespace，ShowCollections 列该 database 的全部 collection；目录不读快照或对象存储，只把确认不存在翻成 Spark 的 namespace 不存在语义；已知表的 `DESCRIBE TABLE` 由 R1 加载 | P1 |
| C2 | 建表删表 | `CREATE TABLE milvus.db.coll (...) TBLPROPERTIES (...)`、`DROP TABLE` | spark.catalog → client.api | 已实现；`milvus.primary.key` 指定非空 Int64/VarChar 主键，歧义字段用 `milvus.field.<field>.*` 明确 Milvus 类型及维度/长度/容量，每个向量字段用 `milvus.index.<field>` JSON 定义索引；CREATE 先校验并预检名称，再创建 collection 与索引，不创建快照且不是跨步骤事务；DROP 在确认 database 或 collection 不存在时返回 `false` | P1 |
| C3 | schema 来自快照 | 自动 | core.schema | | P0 |

## 4 CALL

Table 接口表达不了的动作走 CALL：四条线走同一个 SQL 语法扩展（决策日志 2026-09-10，设计见 [procedure.html](architecture/procedure.html)），不用 Spark 4.0 才有的 ProcedureCatalog，3.5 线不另做同名函数。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| A1 | 快照 | `create_snapshot('db.coll', 'name')`、`drop_snapshot`、`list_snapshots`、`describe_snapshot`；`restore_snapshot` 待实现（W8） | spark.extensions → spark.procedure → client.api | 已实现前四个；连接器转发在线侧快照 RPC，不自行决定建前 Flush | P1 |
| A2 | 索引 | `create_index`、`drop_index`；创建可显式等待完成 | spark.extensions → spark.procedure → client.api | 已实现；默认只提交，等待有 600 秒默认上限并识别 Failed 终态 | P1 |
| A3 | 生命周期 | `load`、`release`、`flush`、`compact` | spark.extensions → spark.procedure → client.api | 已实现；load/compact 可显式有界等待，flush 只报告请求已提交 | P1 |
| A4 | 登记 | `CALL milvus.system.register('db.coll', staging => '{root}/staging/<job-id>', \`milvus.uri\` => ..., \`fs.*\` => ...)`（#17，2026-09-16 起，四条线）；Scala 入口 `Register.run` | spark.extensions → spark.procedure → core.write.commit、client.api | backfill 分支已通：SQL 与 Scala 入口都读作业清单，经公开的 BatchUpdateManifest 登记并写幂等标记；append 分支仍等 Milvus 提供 RegisterSegments | P1 / append 待定 |
| A5 | 描述 | `describe`：collection id、schema、段数、load 与索引状态 | spark.extensions → spark.procedure → client.api | 已实现；按 schema 顺序输出，无索引字段保留一行且索引列为 NULL，不把段行数相加冒充当前行数 | P1 |
| A7 | 清理暂存 | `CALL milvus.system.cleanup_staging('db.coll', retention_seconds => 604800, dry_run => true, \`fs.*\` => ...)` | spark.procedure → core.write.commit | 已实现带 collection owner、append/backfill 模式、driver heartbeat 的 fail-closed 候选判定；默认保留 7 天、最小 5 分钟、dry-run 默认打开，只处理未登记 append，并在删除前复核最新文件修改时间与完整 fingerprint。当前 JNI 只能逐文件删除，结果明确报告残留目录且 `prefix_deleted=false`；完整前缀删除等待 milvus-storage 暴露递归目录删除 | P1 / 目录删除待上游 |

## 5 向量与索引

向量搜索只有一个入口 `MilvusSearch.search`：输入一组查询，输出每条查询的全局 TopK；段内按精确扫描（V5）或索引探查（V7）算候选，快照、删除与过滤位图、候选合并、回表两者共用（决策日志 2026-09-17，设计见 [vector-search.html](architecture/vector-search.html#overall)）。向量能力整组排 P2。原生接口采用 Knowhere PR #1829 分支，精确提交由根目录子模块 gitlink 固定。V1 已交付库加载、C ABI 校验、版本查询和持久化索引封装；V5、V7 已接通查询集入口的两阶段执行——段组与查询组规划、两条下发路、段内精确扫描与索引探查、按查询合并、按段回表；建索引与更多索引格式仍待实现。 让搜索直接写在 DataFrame 或 SQL 计划里是长期优化项，设计见 [vector-search.html 2.10](architecture/vector-search.html#rollout)。

issue #125 的[索引查询设计](architecture/vector-search.html)已经落地首个互操作范围。每个任务只加载一次本段的索引，任务结束时关闭，不做跨任务缓存（决策日志 2026-09-17）。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| V1 | Knowhere 接入 | NativeVectorLibrary 加载及版本查询；NativeVectorIndex 包装持久化加载和搜索 | native-vector、native-runtime | [统一动态依赖与解压](engineering/native-libraries.html)；根目录子模块固定 Knowhere PR #1829 的 Java/JNI/原生源码，C ABI=1；Cardinal 文件要求对应构建特性 | P2 |
| V2 | 加载 Milvus 建的索引 | 自动；按快照或 Manifest 里的索引文件 | core.index | 保留快照段记录的 index_files；使用 Manifest 索引登记时须核验目标版本（README 第 5 节）。加载契约见[方案](architecture/vector-search.html#metadata) | P2 |
| V5 | 精确搜索 | `MilvusSearch.search(..., mode = exact)`，输入查询集，每条查询返回全局 TopK | spark.read、core.index、native-vector | 上游 Knowhere.bruteForce，一次调用算一批向量对整组查询；按查询有界合并，合并后再回表；外表依赖 R20。查询集下发、两阶段执行、合并与回表已实现；2026-09-18 在 UAT 上跑通（7 个用例，见 [vector-search.html 2.9](architecture/vector-search.html#interop)）。`vector.search.*` 逐段入口和 spark-base `filter` 包的 JVM 暴力搜索已删除（决策日志 2026-09-18）。设计见 [vector-search.html 第一、二节](architecture/vector-search.html#overall) | P2 |
| V7 | 持久化索引向量查询 | `MilvusSearch.search(..., mode = index)`，输入查询集 | spark.read、core.index | V2、R7、SegmentReader.take；与 V5 共用段内执行器接口、合并与回表；范围是 HNSW 家族、IVF 家族与两个 FLAT，元素类型随列，nullable 列按 `valid_data` 位图换算行号（决策 21，2026-09-18）；2026-09-18 按真实 Faiss HNSW 索引文件端到端验过（合并、过滤与删除、回表、混合未建索引段）；云上 Cardinal 流索引的复验要含 Cardinal 的原生构建（[2.9](architecture/vector-search.html#interop)）；[查询契约](architecture/vector-search.html#api) | P2 |
| V8 | SQL 向量函数 | `cosine_similarity`、`l2_distance`、`inner_product`、`hamming_distance`、`jaccard_distance`，经 apps 的 SessionExtensions 注册 | apps.search | 已实现；JVM 逐行计算 DataFrame 里已有的值，不读 collection；`vector_knn` 已删除（决策 24，2026-09-18） | P2 |

## 6 兼容入口

两条兼容入口不进主路径，产出核心层的 Snapshot 或 SegmentReader 后与主路径合流。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| K1 | Storage V2 packed 段 | 自动识别 storage_version=2 的段 | core.read.exec 按 SegmentLayout 开同一个 SegmentReader；compat.v2 只在 driver 上恢复列组布局；spark.read 一个行式 reader，列名规则在 V2ColumnBinding | 分发靠 core 的注册表，不能让 core 依赖 compat | P0 |
| K3 | backup 目录 | `milvus.backup.dir` | compat.backup 实现 SnapshotSource | | P1 |

## 7 场景与工具

用户场景在 apps 模块，是连接器语义之外的独立入口。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| O1 | backfill 作业 | `spark-submit --class ...BackfillApp`，24 个 flag，结果 JSON | apps.backfill | flag 集按 W2 重新定义，与 zilliz-cloud 的调用方对接；不早于 W2 | P2 |

## 8 配置

配置分表 option、写 option、会话配置三级；不兼容 1.x 的键名（决策日志 2026-09-14）。

| 编号 | 功能 | 用户入口 | 实现位置 | 依赖或前提 | 优先级 |
|---|---|---|---|---|---|
| G1 | 表 option | 快照名或时间点、`milvus.filter`、分区和段选择 | spark.options | R2、R7、R16；布尔值只认 true/false，正数与数值 id 严格解析；`milvus.filter` 在规划期完成语法与 schema 校验；搜索的三个 `milvus.search.*` 字节上限必须是正整数 | P1 |
| G2 | 写 option | `milvus.write.file.rolling.bytes` 严格解析为正 Long，默认 2 GiB，V2/V3 writer 均映射为上游 `writer.file_rolling.size`；写模式与列、写完自动建快照仍随对应能力落地；索引参数归 W6 的过程，不是写 option | spark.options → core.write.exec | W2；rolling 按上游未压缩写入字节累计，不等于最终 Parquet 大小；替代 1.x 的 `milvus.writer.commitType`（`milvus.writer.fieldIds` 与 `vector.<f>.dim` 已于 2026-09-15 删除，字段 id 和维度从 collection schema 取） | P1 |
| G3 | 会话配置：内存与批 | `milvus.read.batch.max.rows` / `.bytes` 经 `ReadLimits` 随 task 交付原生 reader；`milvus.read.arrow.max.bytes` 限制每个 Spark read task 的 Arrow child allocator，覆盖行式、列式与向量回表 | spark.options → core.read.plan → core.read.exec；spark.read / spark.types 持有 child allocator | Arrow 上限不包含 milvus-storage native 内存池；上游尚无 per-reader 预取上限接口，prefetch limit 未交付；替代 1.x 的 `s3.preloadPoolSize` | P1 |
| G5 | 指标 | 自动，Spark SQL 页的 scan / write 节点：JNI 调用次数与耗时、过界的 Arrow 批数与字节数、C 侧拷贝次数与字节数、物化成 InternalRow 的行数、allocator 峰值；不设开关（决策日志 2026-09-16） | native-storage 编译的上游批读取 holder 计数，core.read.exec 的 ReadMetrics、core.write.exec 的 WriteMetrics，spark.metrics 翻成 CustomMetric | 设计见 [storage-io.html 第五节](architecture/storage-io.html#metrics)；量不到的两处（对象存储读取字节、native 内存总量）写在那里；allocator 峰值就是 G3 预算要卡的数 | P1 |
| G4 | 会话配置：索引与 GPU | Knowhere 搜索与建索引线程池大小、GPU 开关、两套服务的地址和凭证 | spark.options → core.index → native-vector、core.credential | 未开始：钉住的 Knowhere 子模块 Java API 已提供 `Knowhere.resizeSearchThreadPool` / `resizeBuildThreadPool` 及对应读取方法，连接器没有任何调用，也没有对应选项；GPU 开关上游 Java API 未提供（README 第 5 节）。搜索与建索引任务每个 executor 只跑一个、线程池用满该 executor 的核；GPU 产物见 README 2.7 | P2 |

## 9 已知缺口

CREATE TABLE AS SELECT 当前不能完成。C2 创建 collection 后不生成 Connector 快照，Spark 因而没有可加载并继续写入的 Table；失败可能留下空 collection，用户要显式删除。即使以后接通，Milvus Storage 的权威在 etcd 和 DataCoord，C2 建表走 gRPC、W1 的登记是独立的 CALL，两步之间也必然有窗口，不能提供 StagedTable 式原子提交。

## 10 不做

TopN 和 Aggregates 下推（向量距离函数的排序下推属长期优化项，见 [vector-search.html 2.10](architecture/vector-search.html#rollout)）；UPDATE 和 MERGE；text_match 一族（依赖 tantivy 文本索引）；GIS 表达式；struct 数组表达式；random_sample；DataSource V1 Filter（1.x 走 V1，2.0 只实现 V2 谓词，见 R6）；Vortex 列组的读写（milvus-storage 支持，Connector 不接）；truncate 和 overwrite（2026-09-16 定，原 W4）：`mode("overwrite")` 是 Spark ETL 模板里的常见写法，接进来后改个表名就让生产 collection 的全部旧数据不可见，Milvus 没有回滚；不接时 Spark 在分析阶段报 `Table does not support overwrite`，数据不动，全量刷新改为在 Milvus 侧 drop/recreate 或 SDK 全表 delete 后再 append，误操作要在 Milvus 的界面或 SDK 里显式做。1.x 没有这个能力，不接不是退化；以后有明确需求再加，加是兼容的，加了再拿掉不是。DELETE（2026-09-16 定，原 W5）：`DELETE FROM milvus.db.coll WHERE ...` 会把谓词翻成 Milvus 表达式后调 Milvus 的 Delete RPC，删的是生产 collection 的在线数据，谓词写宽了没有回滚；Milvus SDK 已经用同一套表达式语法提供 delete，连接器接进来只是把同一个删除动作换到 Spark 作业里发，多一个出错的地方，不多一种能力。连接器的职责是读写存储格式的文件，不碰在线数据的删除。不接的代价为零：用户在 SDK 里执行同一条表达式。随之 `core.expr` 不再需要把中间表示打印回 Milvus 语法的 ExprPrinter。 精确 KNN 基准与召回评测作业（2026-09-18 定，原 O3）：召回率是两次 `MilvusSearch.search` 结果的一个 join —— 同一查询集分别以 `mode = exact` 和 `mode = index` 跑，结果里的 `_segment_id` 与 `_row_offset` 就是行身份，left_semi join 后除以基准行数。连接器给的是两个 mode 和结果里的行身份（V5），这两样已经有了；作业本身是十行 DataFrame 代码，归调用方。收进仓库等于替调用方维护他的评测脚本，还要给它一套 flag 和一种输出格式。

## 11 尚未落地

这几条在第 1 到第 8 节里有行，但代码里还没有任何包声明承载它们。列在这里是让
`checkCapabilityIndex` 知道这是已知的空缺，不是索引漂移。落地时删掉对应行，并在目标包的
`package.scala` 里写上编号。

| 编号 | 为什么还没有 |
|---|---|
| R10 | milvus-storage 尚未写可用的 row-group min/max 统计，FFI 也没有传入 row group 选择的 reader 入口；现有 Parquet predicate 实现为空，见 storage-access 4.5 |
| R19 | 按分区报分区，优先级是「待评估」。收益要实测，见 README 第 4 节决策 19 |
| R20 | 读 external collection 的设计已写（snapshot.html 3.1、read.html 1.1 与 6.4、storage-auth.html 3.4），代码未开始；打开外表段依赖 milvus-storage 的 `loon_reader_new` 接受空 schema |
| G4 | 索引与 GPU 的会话配置没有任何选项或调用；spark.options 曾声明 G4 但目录里无对应代码，2026-09-20 移到此处。上游 Knowhere Java API 已开放线程池大小接口，接入不再阻塞于上游；GPU 开关上游未提供 |
