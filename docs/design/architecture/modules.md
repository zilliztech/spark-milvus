# 2.0 模块、包与目录 `[草稿]`

依赖只能向下：apps → spark-`<line>`（源码来自 spark-base）→ compat、client → core → native-storage、native-vector。核心层没有 Spark，原生层没有业务逻辑，对外的入口和遗留代码只在 apps。

核心层不依赖 Spark 换来三件事：它在四条 Spark 线上只编一次、只出一个产物；它的测试不用拉起 SparkSession；边界由编译期检查守着。换不来的是跨语言复用 —— Ray 是 Python，依赖不了 JVM 的 jar。

跨语言调用使用上游提供的原生接口与绑定。milvus-storage 自带 Python 绑定（`python/` 目录）；Knowhere 也有 Python 绑定，不能据 JVM 的模块划分推断它没有非 JVM 入口。
向量库的 C 接口、JNI、Java API 与加载器采用 Knowhere PR #1829 的固定提交，Connector 的 `native-vector` 负责调用上游加载入口并核验 ABI。
本仓库原先自行编写 `mv_*` C shim 的计划已替换；非 JVM 调用方消费 Knowhere 的上游接口。固定来源和加载契约见[向量查询设计第 6.1 节](vector-search.html#library-loading)。
功能编号见 [capabilities.md](../capabilities.md)，名词沿用 [总体设计](../README.md) 第 0 节。

## 1 模块

| 模块 | 层 | 包名 | 依赖 | 产物 |
|---|---|---|---|---|
| native-storage | 第 1 层 | 上游 `io.milvus.storage` | 固定 milvus-storage 子模块的 Java/Scala 绑定、Arrow（provided）及 C/JNI 库 | `com.zilliz:spark-milvus-native-storage_<scala>`，jar 内 `native/{os}-{arch}/` 打包上游 JNI 与依赖 |
| native-vector | 第 1 层 | `com.zilliz.milvus.jni.vector` | Knowhere PR #1829 的 Java API 与 JNI 产物 | `com.zilliz:spark-milvus-native-vector`；依赖原生产物保留上游 `native/knowhere/1/<platform>/` 布局，由上游加载器提取与加载 |
| core | 第 2 层 | `com.zilliz.milvus.storage` | native-storage、native-vector、Arrow（provided）、milvus-proto 的消息类；`hadoop-common` 只为 parquet-mr 的签名在编译类路径上，代码里不用 | `com.zilliz:spark-milvus-core_<scala>` |
| compat | 第 2 层 | `com.zilliz.milvus.storage.compat` | core | `com.zilliz:spark-milvus-compat_<scala>` |
| client | 第 2 层 | `com.zilliz.milvus.client` | core、ScalaPB、gRPC；带 service 的 proto 在这里生成 | `com.zilliz:spark-milvus-client_<scala>` |
| spark-base | 第 3 层 | `com.zilliz.spark.connector` | 不是 sbt project，是四条线引用的源码目录 | 无 |
| spark-3.5 / 4.0 / 4.1 / 4.2 | 第 3 层 | 同上 | spark-base 的源码 + 本线专属目录；core、compat、client；本线 Spark 为 provided | `com.zilliz:spark-milvus-<line>_<scala>` |
| apps | 第 4 层 | `com.zilliz.spark.connector.apps` | 一条线的 spark 模块 | `com.zilliz:spark-milvus-apps-<line>_<scala>`：fat jar。只在云上跑的那条线上建；只有一个消费者，源码直接放在这条线的目录里，不设共享 base |
| integration | 测试 | | 一条线的 spark 与 apps 模块；需 MinIO 和 Milvus | 不发布。Spark 线跑一条，存储后端做成参数化的 fixture 整体重跑（本地、MinIO、S3、OSS、COS、OBS） |

展开后 11 个 sbt project：native 两个、core、compat、client、spark 四条线、apps 一个、integration 一个。交叉编译由 `crossScalaVersions` 控制，不增加 project 数。

只有 spark 必须按线拆：`TableCatalog` 与 `ParserInterface` 的方法集有差异，Arrow、antlr、Java 目标版本也按线定。fat jar 不单独成模块，assembly 是 spark-`<line>` 上的一个任务；apps 和 integration 各只建一个，加线是加一行配置。

Scala：3.5 线出 2.12 和 2.13，4.x 线只出 2.13；native-storage、core、compat、client、spark-base 交叉编译两个版本；apps 和 integration 跟随所在线。

1.x 的坐标 `com.zilliz:spark-connector_2.13` 在 2.0 之后不再更新，1.x 的修复仍发到它。

## 2 包

### 2.1 core `com.zilliz.milvus.storage`

对外只暴露 `snapshot`、`read`、`write`、`index`、`schema`、`expr` 六个包的公开类型；其余是实现细节。

| 包 | 职责 | 主要类型 |
|---|---|---|
| `snapshot` | 列快照目录，选快照，把 JSON 和 Avro 变成实体；SnapshotSource 接口 | SnapshotCatalog、Snapshot、Segment、CollectionIndex、SegmentIndex、SegmentIndexes、V2ColumnGroup、DeltaLogFile、SnapshotSource |
| `snapshot.json` | 快照 JSON 文件的形状，一个 JSON 对象一个类型，名字带 `Json` 后缀；只描述不计算 | SnapshotJson、CollectionSchemaJson、FieldJson、SegmentJson、ManifestItemJson、SegmentListJson（option 串里的段列表，随 1.x option 读法一起删） |
| `manifest` | 一个段的 Manifest：列组、删除文件、统计、索引登记 | Manifest、ColumnGroup、ManifestReader |
| `schema` | 字段 id、名字、Milvus 类型、Arrow 类型的唯一映射；不含 Spark 类型 | SchemaMapper、MilvusTypes、ArrowTypes、FieldMetadata |
| `path` | 三种路径形态到 (bucket, key) | StoragePath、Located |
| `io` | 对象存储读写的最小接口和它唯一的实现（走 C 的 `loon_filesystem_*`） | ObjectStore、ObjectStoreFactory、NativeObjectStore、FileInfo |
| `codec` | 列值与 Milvus binlog 共同封装的字节编解码，文件访问归 io | FloatConverter、SparseFloatVectorConverter、BinlogCodec（从 DeltaLogReader 提取，删除及索引复用） |
| `credential` | 对象存储凭证的取用和下发 | Credentials、CredentialSource |
| `expr` | R7 的手写 Milvus 标量文法与名称绑定求值；R6 的字段 id / 类型绑定表示、三值逻辑与 Arrow 列批位图 | R7：Expr、PlanParser、Evaluator；R6：PredicateExpr、PredicateEvaluator、Bitmap。Spark V2 Predicate 翻译归第 3 层；表读取的 `milvus.filter` 与 JSON/Array 语义仍待扩展 |
| `delete` | 删除文件解码，按行号置位 | DeleteBitset、DeltaLogDecoder |
| `stats` | 段统计：写侧的主键 bloom filter，读侧的保守剪枝 | PrimaryKeyStats、BlockedBloomFilter（blobloom 的逐位移植，#15）、PrimaryKeyFilter、PrimaryKeyBloomPruner（R9/R18 段级）；R10 row-group min/max 仍等仓库外依赖 |
| `read.plan` | 分区规划，纯 JVM，可序列化 | SegmentReadTask、SegmentLayout、DeleteSource、ReadPlan、DeleteFileListing：`DeleteFileListing.of` 在 driver 上列删除文件（V3 段要开 manifest），`ReadPlan.of` 把 Snapshot 变成任务列表，任务带 `DeleteSource.Files`（#12、#13）。Partitioner 待 R19（决策 19）与 R16 定了再加，一段一分区之外还没有第二种切法 |
| `read.exec` | 批读取、行号取列、出口；碰 native | SegmentReader、SegmentReaderRegistry、TakeResult；take 包装 loon_take，接收有序唯一行号。列式出口的 Spark 类型归第 3 层，进 core 的仍是 VectorSchemaRoot |
| `write.exec` | 段写出、暂存布局；碰 native | SegmentWriter（V3SegmentWriter、V2SegmentWriter）、WrittenColumnGroups、ManifestTransaction、StagingLayout |
| `write.commit` | 作业清单、所有权、心跳、提交、幂等，以及 A7 的 fail-closed 候选审计与文件删除；完整目录删除等待原生 API | JobManifest、Committer、StagingCleaner |
| `index` | 持久化索引选择、加载、排除位图、向量执行与段内 TopK | SegmentIndexQuery、IndexFileCodec、MilvusIndexFileDecoder、PersistedIndexSearch、BruteForceSearch；索引来源随 Snapshot 固定，任务独占并关闭。IndexCache、IndexWriter 待实现 |

### 2.2 compat `com.zilliz.milvus.storage.compat`

三个适配器实现 core 的接口，由 spark 层在启动时注册进 core 的注册表；core 源码不出现 compat 的包名。

| 包 | 职责 |
|---|---|
| `v2` | Storage V2 packed 段的 SegmentReader |
| `backup` | milvus-backup 导出目录转 Snapshot，实现 SnapshotSource |

### 2.3 native-storage：上游 `io.milvus.storage`

| 来源 | 职责 |
|---|---|
| `milvus-storage/java/src/main` | MilvusStorageFileSystem、Properties、Reader、Writer、PackedWriter、Manifest、ColumnGroups、Transaction 及 NativeLibraryLoader |
| `milvus-storage/cpp/src/jni` | `libmilvus-storage-jni`；调用 `loon_*`，转换异常，持有 Arrow 批次并记录复制计数 |
| Connector 的 `native-storage` 模块 | 按 Scala 2.12/2.13 编译上述固定源码，打包 JNI 和原生依赖；不含第二套 native 声明或加载器 |

`Compile / sourceGenerators` 用 `Sync` 把子模块源码复制到 `sourceManaged` 后编译，避免 Connector 的格式化任务改写子模块。上游 sbt 项目不进入构建；Arrow 标为 provided，以 4.0 线作编译基线，运行版本由 Spark 线决定。完整所有权及迁移范围见 [storage-io.html 第六节](storage-io.html#upstream-jni)。本次迁移的编译、原生功能与真实 UAT 结果及剩余限制见 [存储 I/O 验收状态](storage-io.html#state)。

Arrow 过界的三条约定：
1. 两个出口。给 JVM 的返回 Arrow Java 的 reader；给原生消费者的只交 `long` 地址，签名不出现 Arrow 的 Java 类型 —— 类型化参数会把调用方绑死在我们 classloader 里的那个 Arrow 版本，而 C Data Interface 的 ABI 跨版本稳定。
2. 导出前严格比对。原生 schema 与分区声明的 schema 按字段名、顺序、类型、可空性递归比对，对不上就整个分区拒绝、退回列式 reader，不做部分修补。
3. 关闭顺序固定且幂等：release 回调（对已被接管的做幂等判断）→ 释放结构体 → 关 reader → 关句柄，异常用 `addSuppressed` 累积。

### 2.4 native-vector `com.zilliz.milvus.jni.vector`

| 包 | 职责 |
|---|---|
| `jni.vector` | NativeVectorLibrary：上游加载、ABI 与构建特性校验；NativeVectorSearch：调用上游 BruteForce；NativeVectorIndex：BinarySet、deserialize、search 的资源所有权 |

C 接口、JNI native 方法、`io.knowhere` Java API 和原生资源加载器均由固定的上游提交提供。
本模块以 Java 11 编译，不依赖 Spark 或 Arrow，不重复定义 native 方法、提取器或 C++ shim。
`src/main/cpp/README.md` 记录原生代码的上游归属，Connector 中不保留第二份实现。
现有 vector.search.* 经 core.index.BruteForceSearch 调用上游 BruteForce，按批计算、合并段内 TopK。索引入口经 NativeVectorIndex 调用同一上游的 BinarySet、deserialize、search；不新增 Knowhere JNI。文件格式在 core 解释，Cardinal stream 要求经过特性校验的 WITH_CARDINAL 构建。
Faiss 与 Cardinal 的选择依据 payload 标识，实际引擎注册名与 BinarySet key 分开。`META-INF/milvus/knowhere-runtime.properties` 从所选平台产物的校验记录生成；storage 共用依赖按 `storage-compatibility*.properties` 中唯一匹配的明确摘要记录生成，细节见 [sbt 约定](../engineering/sbt.html#validation)。本次上游 storage JNI 组合已重新通过真实 Cardinal HNSW/COSINE 查询，结果及验收边界见 [存储 I/O 验收状态](storage-io.html#state)；上游 DiskANN 测试失败与本地产物限制见 [向量查询第 6.4 节](vector-search.html#interop)。

### 2.5 client `com.zilliz.milvus.client`

| 包 | 职责 |
|---|---|
| `grpc` | ScalaPB 生成的 stub，重试拦截器 |
| `api` | MilvusClient：ListDatabases、ShowCollections、DDL、describe、Delete、快照、索引、load、release、flush、compact、BatchUpdateManifest、RegisterSegments（待 Milvus 提供）；proto 的 DataType 与 core 的 MilvusType 互转 |

### 2.6 spark `com.zilliz.spark.connector`

| 包 | 职责 | 按线 |
|---|---|---|
| `catalog` | MilvusCatalog：TableCatalog 与 SupportsNamespaces；database 是唯一一层 namespace，collection 是 table；目录发现、三段名与 loadTable 快照重载属 C1/R1/R2，schema/属性校验后创建 collection 与向量索引、确认存在后删除 collection 属 C2；namespace 变更及 table alter/rename 不支持 | 主体在 base，按线只留公开类与 createTable 输入适配 |
| `table` | MilvusTables 统一校验并解析固定 Snapshot，供 DataSource 与 Catalog 构造 MilvusTable；MilvusTable 算 schema、能力集、元数据列并把 Snapshot 交给 scan | 否 |
| `read` | ScanBuilder、Scan、Batch、InputPartition、ColumnarPartitionReader、ColumnVector 实现。包名与 `write` 和 `core.read` 对称，类名沿用 Spark 的 Scan | 否 |
| `expr` | DataSource V2 Predicate 到 `PredicateExpr` 的翻译；每个不完整支持的谓词树作为 residual 交还 Spark | 否 |
| `types` | Arrow 类型到 Spark 类型的映射，向量列的 Spark 表示 | 否 |
| `write` | WriteBuilder、BatchWrite、DataWriterFactory、DataWriter；append 与 backfill 模式（truncate、overwrite 不做，能力表第 10 节） | 否 |
| `metrics` | core 的 ReadMetrics / WriteMetrics 翻成 DataSource V2 的 CustomMetric / CustomTaskMetric，读写各一张清单；G5 | 否 |
| `options` | option 名、别名、校验；ReadMode；按 ReadMode 构造这次读的 SnapshotSource（SnapshotSources，含 ClientSnapshotSource、OptionStringsSnapshotSource，把 compat 的 backup 实现和 V2 footer 解析器接进 core）；`fs.*` 到桶、Hadoop 配置和 driver 侧 ObjectStore 的翻译（StorageOptions、HadoopStorageKeys） | 否 |
| `sources` | 只有 MilvusDataSource，`format("milvus")` 的 TableProvider。留在这个包名下是因为 apps 和用户作业按字符串引用它的全名 | 否 |
| `procedure` | 过程体：`Procedure` 接口（参数表、结果表、driver 上的 `run`）、静态注册表、共用的 collection/client/有界等待规则；已实现快照、索引、load/release/flush/compact、describe、backfill `Register`，以及 A7 的 `CleanupStagingProcedure`；节点和策略在 `extensions`，append 登记与完整目录删除尚未实现 | 否 |
| `extensions` | SparkSessionExtensions、`CALL milvus.system.<name>(...)` 的解析器扩展、CallProcedure 节点与策略；文法 `spark-base/src/main/antlr4/MilvusCall.g4` 一份，设计见 procedure.html | antlr 生成的解析器按线（本线 antlr 版本），`MilvusSqlParser` 适配器按线（4.0 起多 `parseRoutineParam`）；其余共享 |

按线的还有 `META-INF/services` 资源。

CALL 走语法扩展，不走 `ProcedureCatalog`：后者是 Spark 4.0 才有的接口，用它就要为 3.5 再写一套函数入口，同一批动作两份实现。改成自己的 SQL 语法加逻辑节点加物理节点加一个 planner 策略后，语义一份代码覆盖四条线，按线只剩 parser 壳。

版本漂移的隔离按一条判据分两种做法：变的是方法体，用模板方法（基类在 base，按线子类只覆写变的那一处）；变的是接口的方法集本身（如 `ParserInterface` 在 Spark 4 多了一个方法），抽基类会被新方法打穿，只能整份复制。

### 2.7 apps `com.zilliz.spark.connector.apps`

第 4 层：对外的入口和遗留代码。名字不用 ops，内部已有一个叫 OPS 的系统。

| 包 | 内容 |
|---|---|
| `backfill` | BackfillApp、配置、join 键、列映射、merge 模式、结果 JSON |
| `search` | VectorBruteForceSearch、SQL 函数扩展 |

两个包互不依赖，各自是独立入口。`format("milvus")` 的短名归 apps 之后，只有加载 apps jar 才能用旧写法；三段名 `milvus.db.coll` 不需要 apps。

## 3 目录

```
spark-milvus/
  build.sbt                        聚合、版本、发布
  project/                         插件、依赖版本、Spark 线与 Scala 版本矩阵
  milvus-storage/                  固定上游子模块，java/src/main 与 cpp/src/jni
  native-storage/
    src/main/resources/native/     上游 JNI、storage 引擎与动态依赖
    target/scala-*/src_managed/     上游 Java/Scala 源码的生成副本
  native-vector/
    src/main/java/                 com.zilliz.milvus.jni.vector
    src/main/cpp/                  上游 C/JNI 归属说明，代码由 Knowhere 提供
  core/
    src/main/scala/com/zilliz/milvus/storage/{snapshot,manifest,schema,path,credential,expr,delete,stats,read,write,index}
    src/main/antlr4/               空目录说明；R7 解析器按第 4 节第 8 条手写
    src/main/resources/            段清单的 Avro schema
  compat/src/main/scala/com/zilliz/milvus/storage/compat/{v2,backup}
  client/
    src/main/scala/com/zilliz/milvus/client/{grpc,api}
    src/main/protobuf/             milvus-proto 子模块的引用
  spark-base/src/main/scala/com/zilliz/spark/connector/{catalog,sources,table,read,expr,types,write,options,procedure,extensions}
  spark-3.5/src/main/{scala,resources}/  catalog、ParserInterface 适配、antlr 生成物、META-INF/services
  spark-4.0/  spark-4.1/  spark-4.2/     catalog、ParserInterface 适配、antlr 生成物、META-INF/services
  apps-4.0/src/main/{scala,resources}/   com.zilliz.spark.connector.apps.{backfill,search}
  integration-4.0/src/test/scala/  需要 MinIO 与 Milvus
  src/                             1.x 的代码，按模块逐个迁走
  docs/design/                     设计文档
  docs/                            用户文档
```

目录全部平铺，不用分组目录：`native/`、`spark/` 这类分组目录本身不是 sbt project，在 IDE 里只是普通文件夹，和真模块混在一起看不出区别。

单测在各模块的 `src/test/scala`；需要原生库或 UAT 环境的用例自行检查前提，缺少时取消并报告原因。当前 CI 不构建原生库，提交前的单测范围与结果报告要求见 [contributing.md](../../contributing.md#unit-tests)。

## 4 构建约束

1. core、compat、client 的依赖里没有 spark-*；用 sbt 任务扫描源码，出现 `org.apache.spark` 即编译失败。
2. C 接口与 JNI、加载器由固定的 milvus-storage 与 Knowhere 上游提供；Connector 不保留自有 JNI 转发实现。原生库由第 1 层的上游加载器加载；driver 通过 `core.io` 读取快照、清单等文件时也会加载原生库，executor 通过 `core.read.exec`、`core.write.exec` 或 `core.index` 调用原生读写与索引接口。清单内容在 JVM 解析，文件访问仍经 `core.io`，不能因为只读本地文件就绕开存储接口。
3. Arrow 版本由 spark-`<line>` 钉，与本线 Spark 自带的对齐（3.5 用 12.0.1，4.0 用 18.1.0，4.1 用 18.3.0，4.2 用 19.0.0）；core 与 native-storage 只按接口编译，`arrow-vector`、`arrow-memory-core`、`arrow-c-data`、`arrow-format` 全标 provided，实现由运行时的 Spark 提供，版本按 4.0 线取。Spark 的 patch 版取每条线最低的维护版，编译版本就是兼容下限。
4. Java 目标版本按线：core、compat、client、native-* 钉 `-release 11`；spark-`<line>`、apps 按本线（3.5 用 11，4.x 用 17）。
5. 交叉编译的模块统一 `import scala.jdk.CollectionConverters._`，加 `scala-collection-compat` 为 2.12 补齐，禁止 `scala.collection.JavaConverters`。
6. fat jar 只 relocate protobuf 和 guava；`io.milvus.storage.**`、`com.zilliz.milvus.jni.vector.**`、`io.knowhere.**` 和 `org.apache.arrow.**` 不 relocate，JNI 的导出符号已按包名编进 .so；`META-INF/services` 用 merge 策略。Knowhere 的资源布局和校验和文件保持上游约定，库提取和依赖加载交给上游加载器；所需 JRE `libjsig` 在 JVM 启动前预加载，不能靠运行中的 Java 调用补齐。
7. apps 的每个包能单独删除而不影响编译。
8. SQL 扩展的语法文件放共享源码目录，每条线用本线的 antlr 版本各生成一份，antlr 运行时标 provided 用 Spark 自带的。core 里的 Milvus 表达式解析器不用 antlr：core 是跨线单产物，生成的解析器在 3.5 的 4.9.3 和 4.x 的 4.13.1 之间不通用。
9. 打开段只有一个入口，凭证刷新在那里做；`core.read.exec` 与 `core.write.exec` 不得绕过它直接开文件。
10. 共享源码只能用各条线都有的 Spark API。这条由 spark-3.5 兜住：它用最低的那条线编译共享源码，用了高版本才有的 API，它先编译失败。
11. 目录镜像包名。1.x 的 41 个文件不是这样（文件在 `src/main/scala/read/`，包是 `com.zilliz.spark.connector.read`），迁移时一并对齐。
12. 模块的显示名跟目录走，发布坐标用 `moduleName` 另设。根项目显示名 `spark-milvus`（等于仓库目录），坐标仍是 `com.zilliz:spark-connector`。sbt 的 project id 不能带点，所以命令行是 `spark40` 而目录是 `spark-4.0`。
13. 第 2 层不用 Spark 的 Logging，用 core 的 `com.zilliz.milvus.storage.Logging`（slf4j，provided）。约束 1 的扫描会先去掉注释，注释里提 org.apache.spark 是合法的。
14. core 读写存储只经 `io.ObjectStore`，源码里不出现 `org.apache.hadoop`。唯一实现是 `io.NativeObjectStore`，走 C 的 `loon_filesystem_*`；`io.hadoop` 已删除。`hadoop-common` 仍在 core 的编译依赖里，但不是给我们的代码用的——parquet-mr 的 `ParquetReader.Builder` 签名里有 `org.apache.hadoop.fs.Path`，类得在编译类路径上。测试复用 core 测试源码里的 `LocalObjectStore` 和 `FailingObjectStore`，不需要原生库；compat 与 spark40 通过 `test->test` 依赖取得这些测试实现，其他 Spark 线及生产依赖不受影响。executor 上拿到的是可序列化的 `ObjectStoreFactory`（一组配置字符串），不是活的 `Configuration`。
15. milvus-proto 的生成分两处：不带 service 的 `common.proto`、`schema.proto` 在 core 生成（`grpc = false`），带 service 的五个在 client 生成（`grpc = true`），靠 include 路径引用 core 的产物，同一份 .proto 不生成两遍。core 用得上它们，是因为 Milvus 的存储格式本身由 protobuf 定义：快照里嵌着 CollectionSchema，Manifest 的字段描述来自 schema.proto，core 不另建一套 schema 模型。

补充（2026-09-14）：`checkCapabilityIndex` 只从 `package.scala` 的 `Capabilities: …（see docs/design/capabilities.md）` 这一句里读编号，正文里的「Storage V2」「DataSource V2」不再算认领；一个只有 `package.scala` 的目录不能认领任何编号，编号必须写进 capabilities.md 第 11 节直到代码落地。

## 5 1.x 到 2.0 的迁移对照

41 个 1.x 源文件已经全部离开 `src/`，该目录不再存在。这一轮只做归属，不改语义：
文件搬到它该在的模块，包名跟目录对齐，调用点直接改指新位置，不留转发壳子。
读路径列式化、写路径提交、DataSource 拆分这些是下一步的重构，不在本表内。

| 1.x 文件 | 2.0 位置 | 状态 |
|---|---|---|
| read/MilvusSnapshotReader.scala | core.snapshot.json | 已迁。92 行 Spark 类型转换切成 spark-base 的 SnapshotSparkSchema；JSON 形状类进 core.snapshot.json 并改名（SnapshotJson、CollectionSchemaJson、FieldJson…），V2SegmentInfo 并入 Segment（`Segment.v2` 构造，`columnGroups`/`deltaLogs`/`dedupColumnGroupsBySlot` 是 Segment 的方法），文件不再存在 |
| read/SegmentManifestReader.scala、V3ManifestReader.scala | core.manifest | 已迁 |
| read/DeltaLogReader.scala、DeletePlan.scala | core.delete、core.codec | 已迁。issue #125 将 DeltaLogReader 中共同的 Milvus envelope 与 Parquet payload 解析提取为 BinlogCodec；删除和 MilvusIndexFileDecoder 共用，ObjectStore 仍是唯一文件入口。SegmentIndexQuery 按段物理行号生成此次搜索的删除/过滤位图 |
| src/main/resources/milvus-segment-manifest*.avsc | core 的 resources | 已迁。资源必须跟代码走，留在原处解码器会报 not found on classpath，而失败形式是返回 Left 不是抛异常 |
| serde/SparkTypes.scala、SchemaUtil.scala | core.schema、spark.types | 已迁。core.schema 得到 MilvusTypes、ArrowTypes、SchemaMapper、FieldMetadata；Spark 那一半是 spark.types 的 SparkTypes 与 SparkSchemaMapper（文件名已改成对象名） |
| MilvusUtil.scala 的 FloatConverter、SparseFloatVectorConverter | core.codec | 已迁。文档原来写「MilvusUtil 整个进 apps.legacy」，不成立：627 行里只有 307 行是 FieldData 打包，两个转换器是纯 JVM 的列值编解码，被 ArrowConverter 和读路径用着 |
| MilvusUtil.scala 的 IntConverter | 删除 | 已删，全仓零引用 |
| Exception.scala | core 与 client | 已迁。DataParseException、DataTypeException 进 core；三个 RPC 异常进 client |
| read/FooterV2SegmentResolver.scala | compat.v2 | 已迁。resolvePath 与 readAllBytes 先下沉到 core 的 path 与 io.hadoop，否则 core 的两个 Manifest 解析器要反向依赖 compat |
| read/ParquetFooterReader.scala | compat 根包 | 已迁。v2 和 backup 都要用它 |
| read/BackupMetaReader.scala | compat.backup | 已迁 |
| MilvusClient.scala | client.api、client.grpc | 已迁。重试拦截器拆进 client.grpc；收 MilvusOption 的工厂删掉，改由 MilvusOption.connectionParams 产出连接参数；Catalog 的 ListDatabases、ShowCollections、collection create/drop 与 vector index create 均经 client.api 接入并校验响应状态 |
| sources/MilvusDataSource.scala（2880 行） | spark.sources、spark.table、spark.read、spark.options | 已拆成 14 个文件，最大 550 行。`sources` 只留 TableProvider（FQN 被 apps 和用户作业按字符串引用，不能动）；MilvusTable→spark.table；ScanBuilder、Scan、四个规划入口（ClientSnapshotPlanner、LegacyClientPlanner、OptionSnapshotPlanner、BackupPlanner）、SnapshotPartitions、DeletePlanning、ClientReadSnapshot→spark.read（四个规划入口后来在 #04 全部变成 SnapshotSource，见 snapshot.html 第二节）；桶判定与 Hadoop 配置翻译（StorageOptions）、备份集合选取（BackupSelection）、ReadMode→spark.options。规划逻辑下沉 core.read.plan 未做，SnapshotPartitions.build 是要下沉的那部分 |
| MilvusOption.scala、loon/Properties.scala | spark.options | 已搬。MilvusOption 在 spark.options；MilvusOption 是混的，存储配置下沉 core.credential 是重构，未做。`loon/Properties.FsConfig` 的每个常量都是 core.credential.StorageProperties 的别名，调用方已全部改为直接用 StorageProperties，2026-09-14 连同 PropertiesTest 一起删除，`loon` 包不再存在；`loon/HadoopStorageKeys` 已搬到 spark.options，和 StorageOptions 是同一件事的两半 |
| read/MilvusV3PartitionReader.scala、MilvusPartitionReaderFactory.scala、MilvusInputPartition.scala、MilvusV2PartitionReader.scala | spark.read | 已搬。开段下沉 core.read.exec 的注册表；#06 两个行式 reader 合成 `MilvusRowPartitionReader`，两条线的列名规则归 `ColumnBinding`，向量检索拆成 `SegmentVectorSearch`；列式出口是 `MilvusColumnarPartitionReader` |
| serde/ArrowConverter.scala、ArrowAllocator.scala | spark.types | 已搬到 spark.types：它做的是 Arrow 值与 Spark InternalRow 的双向转换，就是 types 的职责。读路径由 ColumnVector 取代、写路径重写进 core.write.exec 是重构，未做 |
| filter/VectorBruteForceSearch.scala | spark-base | 遗留 DataFrame/UDF 入口保留，最终迁移等决策 16；SegmentVectorSearch 已改调 core.index.BruteForceSearch，不再调用此处的 JVM 距离计算 |
| issue #125 持久化向量查询 | core.index、core.expr、core.read.exec、spark.read、native-vector | SegmentIndexQuery 负责共同执行与过滤，IndexFileCodec 支持 Milvus envelope/切片及 CARD 流，PersistedIndexSearch 使用上游索引；SegmentReader.take 回表，SegmentIndexSearch 适配 Spark 行，MilvusSearch 构造全局 TopK。索引任务独占，无跨任务缓存；本次上游 storage JNI 组合已通过真实 Cardinal HNSW 查询，验收边界见 [存储 I/O 状态](storage-io.html#state)，Knowhere 上游 DiskANN 测试失败记录仍保留 |
| write/MilvusV3Writer.scala、MilvusV2Writer.scala | spark.write → core.write.exec | 已搬；#07 把 native 调用剥进 core.write.exec（V3SegmentWriter、V2SegmentWriter、ManifestTransaction），spark.write 的两个类只剩行到 Arrow 批和 Spark 接口；暂存路径由 StagingLayout 定；#08 起 MilvusV3BatchWrite 的 commit/abort 调 core.write.commit 的 Committer |
| write/MilvusWriteBuilder.scala、MilvusBatchWriter.scala、MilvusDataWriterFactory.scala、MilvusInsertDataWriter.scala、MilvusFieldData.scala（原 MilvusUtil.scala） | 删除 | 2026-09-14 删除：gRPC Insert 是 1.x 的写路径（W7），2.0 不支持；MilvusFieldData 只剩集成测试造数据用，搬到 integration-4.0 的 testkit |
| write/MilvusSparkNativeImportWriter.scala | 删除 | 已删，全仓零引用 |
| operations/backfill/* | apps.backfill | 已迁，包名从 operations.backfill 改成 apps.backfill |
| expressions/、extensions/ | apps.search | 已迁 |
| tools/* | apps.tools | 已迁 |
| src/test/**、src/it/** | 各模块的 src/test | 已迁。core 32、compat 43、client 20、spark-4.0 261、apps-4.0 212 个用例；integration-4.0 收 3 个集成用例 |
| milvus-storage/java 的 Java/Scala 绑定 | native-storage | 2026-09-16 改为编译固定子模块源码，支持 Scala 2.12/2.13；自有 StorageNative、C++ JNI 和加载器移除。filesystem、逐批读取/take、计数、manifest 释放及 transaction 扩展归上游。本次源码交叉编译、单元测试及真实运行结果见 [存储 I/O 验收状态](storage-io.html#state)。storage-access 原需求 7 已被此决策取代 |

### 5.1 搬运中暴露的事实

1. 六个文件只因为 `org.apache.spark.internal.Logging` 才算 Spark 代码，其中三个继承了但一次都没调用，全仓实际日志调用只有 7 处。core 因此有了自己的 `Logging`（slf4j，provided）。
2. 1.x 的 `private[read]` 跨模块之后失效，14 个成员被迫改成 public。1.x 的封装边界是按目录划的，不是按职责划的。
3. 1.x 的 main 与 test 在 Spark 3.5.5、4.0.0、4.2.0 上都编得过，所以存量代码进 spark-base 让四条线各编一遍没有兼容风险。这是 spark-base 而不是单条线的依据。
4. parquet-hadoop 传递进来的 jackson-databind 比 jackson-module-scala 新，跨版本直接抛 JsonMappingException。第 2 层因此钉死 jackson；Spark 线不能钉，Spark 自带的是一套自洽的更新版本。
5. 挡住纯搬运的反向引用一共 6 处，全部是文件放错位置，不是真的循环依赖。
