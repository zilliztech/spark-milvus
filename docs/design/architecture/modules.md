# 2.0 模块、包与目录 `[草稿]`

依赖只能向下：apps → spark-`<line>`（源码来自 spark-base）→ compat、client → core → native-storage、native-vector。核心层没有 Spark，原生层没有业务逻辑，对外的入口和遗留代码只在 apps。

核心层不依赖 Spark 换来三件事：它在四条 Spark 线上只编一次、只出一个产物；它的测试不用拉起 SparkSession；边界由编译期检查守着。换不来的是跨语言复用 —— Ray 是 Python，依赖不了 JVM 的 jar。

跨语言共用的只在第 1 层，而且只有一半：
1. storage 那半不用我们操心。上游 milvus-storage 自带 Python 绑定（`python/` 目录），Ray 直接用上游的包，和我们的 `native-storage` 无关。
2. vector 那半是我们的。knowhere 既没有 C 接口也没有 Python 绑定，`native-vector/src/main/cpp` 里那层 `mv_*` C shim 是唯一的跨语言资产 —— Ray 要用 knowhere，消费的是它编出的 `.so` 加 C 头文件，不是 JNI 那半。所以 shim 的接口设计要按「会有第二个调用方」来定：只传地址和长度，配置用 JSON，不出现任何 JVM 的概念。功能编号见 [capabilities.md](../capabilities.md)，名词沿用 [总体设计](../README.md) 第 0 节。

## 1 模块

| 模块 | 层 | 包名 | 依赖 | 产物 |
|---|---|---|---|---|
| native-storage | 第 1 层 | `com.zilliz.milvus.jni.storage` | milvus-storage 的 C 接口 | `com.zilliz:spark-milvus-native-storage`，jar 内 `native/{os}-{arch}/` 平铺 .so |
| native-vector | 第 1 层 | `com.zilliz.milvus.jni.vector` | knowhere 的 C shim | `com.zilliz:spark-milvus-native-vector`，同上布局。C shim 的 `.so` 与头文件另出一份，供 Ray 之类的非 JVM 调用方用 |
| core | 第 2 层 | `com.zilliz.milvus.storage` | native-storage、native-vector、Arrow（provided）、milvus-proto 的消息类；`hadoop-common` 只为 parquet-mr 的签名在编译类路径上，代码里不用 | `com.zilliz:spark-milvus-core_<scala>` |
| compat | 第 2 层 | `com.zilliz.milvus.storage.compat` | core | `com.zilliz:spark-milvus-compat_<scala>` |
| client | 第 2 层 | `com.zilliz.milvus.client` | core、ScalaPB、gRPC；带 service 的 proto 在这里生成 | `com.zilliz:spark-milvus-client_<scala>` |
| spark-base | 第 3 层 | `com.zilliz.spark.connector` | 不是 sbt project，是四条线引用的源码目录 | 无 |
| spark-3.5 / 4.0 / 4.1 / 4.2 | 第 3 层 | 同上 | spark-base 的源码 + 本线专属目录；core、compat、client；本线 Spark 为 provided | `com.zilliz:spark-milvus-<line>_<scala>` |
| apps | 第 4 层 | `com.zilliz.spark.connector.apps` | 一条线的 spark 模块 | `com.zilliz:spark-milvus-apps-<line>_<scala>`：fat jar。只在云上跑的那条线上建；只有一个消费者，源码直接放在这条线的目录里，不设共享 base |
| integration | 测试 | | 一条线的 spark 与 apps 模块；需 MinIO 和 Milvus | 不发布。Spark 线跑一条，存储后端做成参数化的 fixture 整体重跑（本地、MinIO、S3、OSS、COS、OBS） |

展开后 11 个 sbt project：native 两个、core、compat、client、spark 四条线、apps 一个、integration 一个。交叉编译由 `crossScalaVersions` 控制，不增加 project 数。

只有 spark 必须按线拆：接口差异（Spark 4.0 才有的 ProcedureCatalog）、Arrow、antlr、Java 目标版本都是按线定的。fat jar 不单独成模块，assembly 是 spark-`<line>` 上的一个任务；apps 和 integration 各只建一个，加线是加一行配置。

Scala：3.5 线出 2.12 和 2.13，4.x 线只出 2.13；core、compat、client、spark-base 交叉编译两个版本；apps 和 integration 跟随所在线。

1.x 的坐标 `com.zilliz:spark-connector_2.13` 在 2.0 之后不再更新，1.x 的修复仍发到它。

## 2 包

### 2.1 core `com.zilliz.milvus.storage`

对外只暴露 `snapshot`、`read`、`write`、`index`、`schema`、`expr` 六个包的公开类型；其余是实现细节。

| 包 | 职责 | 主要类型 |
|---|---|---|
| `snapshot` | 列快照目录，选快照，解析 JSON 和 Avro 成对象；分发 SnapshotSource | SnapshotCatalog、Snapshot、Segment、SnapshotSource、SnapshotSourceRegistry |
| `manifest` | 一个段的 Manifest：列组、删除文件、统计、索引登记 | Manifest、ColumnGroup、ManifestReader |
| `schema` | 字段 id、名字、Milvus 类型、Arrow 类型的唯一映射；不含 Spark 类型 | SchemaMapper、MilvusTypes、ArrowTypes、FieldMetadata |
| `path` | 三种路径形态到 (bucket, key) | StoragePath、Located |
| `io` | 对象存储读写的最小接口和它唯一的实现（走 C 的 `loon_filesystem_*`） | ObjectStore、ObjectStoreFactory、NativeObjectStore、FileInfo |
| `codec` | 列值的字节编解码，读写共用 | FloatConverter、SparseFloatVectorConverter |
| `credential` | 对象存储凭证的取用和下发 | Credentials、CredentialSource |
| `expr` | 中间表示、Milvus 文法解析器、列批求值器、反向打印器 | Expr、PlanParser、Evaluator、ExprPrinter、Bitmap |
| `delete` | 删除文件解码，按行号置位 | DeleteBitset、DeltaLogDecoder |
| `stats` | 段统计和 row group 统计的读取与剪枝 | SegmentStats、Pruner |
| `read.plan` | 分区规划，纯 JVM，可序列化 | InputSpec、SegmentLayout、DeleteSource、ReadPlan。Partitioner 待 R19（决策 19）与 R16 定了再加，一段一分区之外还没有第二种切法 |
| `read.exec` | 批读取、行号取列、出口；碰 native | SegmentReader、SegmentReaderRegistry。ColumnBatch 与 Take 未写：列式出口的 Spark 侧是 Spark 类型，归第 3 层，进 core 的仍是 VectorSchemaRoot |
| `write.exec` | 段写出、暂存布局；碰 native | SegmentWriter、StagingLayout |
| `write.commit` | 作业清单、提交、幂等 | JobManifest、Committer |
| `index` | 索引文件编解码、来源、缓存、写出 | IndexFileCodec、IndexSource、IndexCache、IndexWriter |

### 2.2 compat `com.zilliz.milvus.storage.compat`

三个适配器实现 core 的接口，由 spark 层在启动时注册进 core 的注册表；core 源码不出现 compat 的包名。

| 包 | 职责 |
|---|---|
| `v2packed` | Storage V2 packed 段的 SegmentReader |
| `offline` | 1.x 离线 option 的段列表转 Snapshot，实现 SnapshotSource |
| `backup` | milvus-backup 导出目录转 Snapshot，实现 SnapshotSource |

### 2.3 native-storage `com.zilliz.milvus.jni.storage`

| 包 | 职责 |
|---|---|
| `jni.storage` | StorageNative：每个 loon_* 一个 native 方法，句柄是 long，结果码转异常 |
| `jni.storage.loader` | 按 os 和 arch 解压 .so 到带版本号的目录后 System.load |

包名用 `jni` 而不是 `native`：`native` 是 Java 的保留字，不能做包名。

Arrow 过界的三条约定：
1. 两个出口。给 JVM 的返回 Arrow Java 的 reader；给原生消费者的只交 `long` 地址，签名不出现 Arrow 的 Java 类型 —— 类型化参数会把调用方绑死在我们 classloader 里的那个 Arrow 版本，而 C Data Interface 的 ABI 跨版本稳定。
2. 导出前严格比对。原生 schema 与分区声明的 schema 按字段名、顺序、类型、可空性递归比对，对不上就整个分区拒绝、退回列式 reader，不做部分修补。
3. 关闭顺序固定且幂等：release 回调（对已被接管的做幂等判断）→ 释放结构体 → 关 reader → 关句柄，异常用 `addSuppressed` 累积。

### 2.4 native-vector `com.zilliz.milvus.jni.vector`

| 包 | 职责 |
|---|---|
| `jni.vector` | VectorNative：mv_* 的 native 方法 |

C shim（mv_* 包 knowhere::Index、BruteForce、BinarySet、Version，以及 DiskANN 的本地 FileManager）是 C 源码，在 `src/main/cpp`。

### 2.5 client `com.zilliz.milvus.client`

| 包 | 职责 |
|---|---|
| `grpc` | ScalaPB 生成的 stub，重试拦截器 |
| `api` | MilvusClient：DDL、describe、Delete、快照、索引、load、release、flush、compact、BatchUpdateManifest、RegisterSegments（待 Milvus 提供）；proto 的 DataType 与 core 的 MilvusType 互转 |

### 2.6 spark `com.zilliz.spark.connector`

| 包 | 职责 | 按线 |
|---|---|---|
| `catalog` | MilvusCatalog：TableCatalog、SupportsNamespaces、loadTable 的快照重载 | 主体在 base，按线只留一个工厂方法 |
| `table` | MilvusTable：schema、能力集、元数据列、统计、DeleteV2 | 否 |
| `read` | ScanBuilder、Scan、Batch、InputPartition、ColumnarPartitionReader、ColumnVector 实现。包名与 `write` 和 `core.read` 对称，类名沿用 Spark 的 Scan | 否 |
| `read.plan` | 过渡包：四个 PartitionPlanner、SnapshotPartitions、DeletePlanning、ClientReadSnapshot。全靠 Spark 类型，进不了第 2 层；#03–#05 每换一个入口删一个，最后 SnapshotPartitions.build 变成 `core.read.plan`，这个包随之消失 | 否 |
| `expr` | DataSource V2 Predicate 到 IR 的翻译 | 否 |
| `types` | Arrow 类型到 Spark 类型的映射，向量列的 Spark 表示 | 否 |
| `write` | WriteBuilder、BatchWrite、DataWriterFactory、DataWriter；truncate、overwrite、backfill 模式 | 否 |
| `options` | option 名、别名、校验；1.x 名字的映射和告警；把 compat 的实现注册进 core；`fs.*` 到桶、Hadoop 配置和 driver 侧 ObjectStore 的翻译（StorageOptions、HadoopStorageConfig） | 否 |
| `sources` | 只有 MilvusDataSource，`format("milvus")` 的 TableProvider。留在这个包名下是因为 apps 和用户作业按字符串引用它的全名 | 否 |
| `procedure` | CALL 的语义：逻辑计划节点、物理节点、planner 策略 | 否 |
| `extensions` | SparkSessionExtensions、SQL 解析器扩展、优化规则 | parser、AstBuilder、SessionExtensions 三个壳按线 |

按线的还有 `META-INF/services` 资源。

CALL 走语法扩展，不走 `ProcedureCatalog`：后者是 Spark 4.0 才有的接口，用它就要为 3.5 再写一套函数入口，同一批动作两份实现。改成自己的 SQL 语法加逻辑节点加物理节点加一个 planner 策略后，语义一份代码覆盖四条线，按线只剩 parser 壳。

版本漂移的隔离按一条判据分两种做法：变的是方法体，用模板方法（基类在 base，按线子类只覆写变的那一处）；变的是接口的方法集本身（如 `ParserInterface` 在 Spark 4 多了一个方法），抽基类会被新方法打穿，只能整份复制。

### 2.7 apps `com.zilliz.spark.connector.apps`

第 4 层：对外的入口和遗留代码。名字不用 ops，内部已有一个叫 OPS 的系统。

| 包 | 内容 |
|---|---|
| `backfill` | BackfillApp、配置、join 键、列映射、merge 模式、结果 JSON |
| `tools` | ListV2SegmentsApp、ReadSourceOnlyApp |
| `search` | VectorBruteForceSearch、SQL 函数扩展 |
| `legacy` | gRPC Insert 的 TableProvider、DataSource V2 写栈、`format("milvus")` 短名注册 |

四个包互不依赖，各自是独立入口。`format("milvus")` 的短名归 apps 之后，只有加载 apps jar 才能用旧写法；三段名 `milvus.db.coll` 不需要 apps。

## 3 目录

```
spark-milvus/
  build.sbt                        聚合、版本、发布
  project/                         插件、依赖版本、Spark 线与 Scala 版本矩阵
  native-storage/
    src/main/java/                 com.zilliz.milvus.jni.storage
    src/main/cpp/                  JNI 源码
    build/                         构建与 patchelf 脚本
  native-vector/
    src/main/java/                 com.zilliz.milvus.jni.vector
    src/main/cpp/                  C shim 与 JNI
  core/
    src/main/scala/com/zilliz/milvus/storage/{snapshot,manifest,schema,path,credential,expr,delete,stats,read,write,index}
    src/main/antlr4/               表达式文法（见第 4 节第 8 条）
    src/main/resources/            段清单的 Avro schema
  compat/src/main/scala/com/zilliz/milvus/storage/compat/{v2packed,offline,backup}
  client/
    src/main/scala/com/zilliz/milvus/client/{grpc,api}
    src/main/protobuf/             milvus-proto 子模块的引用
  spark-base/src/main/scala/com/zilliz/spark/connector/{sources,table,read,expr,types,write,options}
  spark-3.5/src/main/{scala,resources}/  catalog、functions、extensions、META-INF/services
  spark-4.0/  spark-4.1/  spark-4.2/     catalog、procedure、extensions、META-INF/services
  apps-4.0/src/main/{scala,resources}/   com.zilliz.spark.connector.apps.{backfill,tools,search,legacy}
  integration-4.0/src/test/scala/  需要 MinIO 与 Milvus
  src/                             1.x 的代码，按模块逐个迁走
  docs/design/                     设计文档
  docs/                            用户文档
```

目录全部平铺，不用分组目录：`native/`、`spark/` 这类分组目录本身不是 sbt project，在 IDE 里只是普通文件夹，和真模块混在一起看不出区别。

单测在各模块的 `src/test/scala`；需要 .so 的单测标 tag，CI 在有 native 产物的 job 里跑。

## 4 构建约束

1. core、compat、client 的依赖里没有 spark-*；用 sbt 任务扫描源码，出现 `org.apache.spark` 即编译失败。
2. native-* 的 C 头文件不出现 JNI 类型；JNI 只在 `jni` 包。原生库只在 executor 加载：`core.read.exec`、`core.write.exec`、`core.index` 之外的 core 包不得调用 native，driver 侧要读的 Manifest 字段由纯 JVM 解析器读。
3. Arrow 版本由 spark-`<line>` 钉，与本线 Spark 自带的对齐（3.5 用 12.0.1，4.0 用 18.1.0，4.1 用 18.3.0，4.2 用 19.0.0）；core 只按接口编译，`arrow-vector`、`arrow-memory-core`、`arrow-c-data`、`arrow-format` 全标 provided，实现由运行时的 Spark 提供，版本按 4.0 线取。Spark 的 patch 版取每条线最低的维护版，编译版本就是兼容下限。
4. Java 目标版本按线：core、compat、client、native-* 钉 `-release 11`；spark-`<line>`、apps 按本线（3.5 用 11，4.x 用 17）。
5. 交叉编译的模块统一 `import scala.jdk.CollectionConverters._`，加 `scala-collection-compat` 为 2.12 补齐，禁止 `scala.collection.JavaConverters`。
6. fat jar 只 relocate protobuf 和 guava；`com.zilliz.milvus.native.**` 和 `org.apache.arrow.**` 不 relocate，JNI 的导出符号已按包名编进 .so；`META-INF/services` 用 merge 策略。
7. apps 的每个包能单独删除而不影响编译。
8. SQL 扩展的语法文件放共享源码目录，每条线用本线的 antlr 版本各生成一份，antlr 运行时标 provided 用 Spark 自带的。core 里的 Milvus 表达式解析器不用 antlr：core 是跨线单产物，生成的解析器在 3.5 的 4.9.3 和 4.x 的 4.13.1 之间不通用。
9. 打开段只有一个入口，凭证刷新在那里做；`core.read.exec` 与 `core.write.exec` 不得绕过它直接开文件。
10. 共享源码只能用各条线都有的 Spark API。这条由 spark-3.5 兜住：它用最低的那条线编译共享源码，用了高版本才有的 API，它先编译失败。
11. 目录镜像包名。1.x 的 41 个文件不是这样（文件在 `src/main/scala/read/`，包是 `com.zilliz.spark.connector.read`），迁移时一并对齐。
12. 模块的显示名跟目录走，发布坐标用 `moduleName` 另设。根项目显示名 `spark-milvus`（等于仓库目录），坐标仍是 `com.zilliz:spark-connector`。sbt 的 project id 不能带点，所以命令行是 `spark40` 而目录是 `spark-4.0`。
13. 第 2 层不用 Spark 的 Logging，用 core 的 `com.zilliz.milvus.storage.Logging`（slf4j，provided）。约束 1 的扫描会先去掉注释，注释里提 org.apache.spark 是合法的。
14. core 读写存储只经 `io.ObjectStore`，源码里不出现 `org.apache.hadoop`。唯一实现是 `io.NativeObjectStore`，走 C 的 `loon_filesystem_*`；`io.hadoop` 已删除。`hadoop-common` 仍在 core 的编译依赖里，但不是给我们的代码用的——parquet-mr 的 `ParquetReader.Builder` 签名里有 `org.apache.hadoop.fs.Path`，类得在编译类路径上。测试用 core 测试源码里的 `LocalObjectStore`，只读本地盘，不需要原生库。executor 上拿到的是可序列化的 `ObjectStoreFactory`（一组配置字符串），不是活的 `Configuration`。
15. milvus-proto 的生成分两处：不带 service 的 `common.proto`、`schema.proto` 在 core 生成（`grpc = false`），带 service 的五个在 client 生成（`grpc = true`），靠 include 路径引用 core 的产物，同一份 .proto 不生成两遍。core 用得上它们，是因为 Milvus 的存储格式本身由 protobuf 定义：快照里嵌着 CollectionSchema，Manifest 的字段描述来自 schema.proto，core 不另建一套 schema 模型。

补充（2026-09-14）：`checkCapabilityIndex` 只从 `package.scala` 的 `Capabilities: …（see docs/design/capabilities.md）` 这一句里读编号，正文里的「Storage V2」「DataSource V2」不再算认领；一个只有 `package.scala` 的目录不能认领任何编号，编号必须写进 capabilities.md 第 11 节直到代码落地。

## 5 1.x 到 2.0 的迁移对照

41 个 1.x 源文件已经全部离开 `src/`，该目录不再存在。这一轮只做归属，不改语义：
文件搬到它该在的模块，包名跟目录对齐，调用点直接改指新位置，不留转发壳子。
读路径列式化、写路径提交、DataSource 拆分这些是下一步的重构，不在本表内。

| 1.x 文件 | 2.0 位置 | 状态 |
|---|---|---|
| read/MilvusSnapshotReader.scala | core.snapshot | 已迁。92 行 Spark 类型转换切成 spark-base 的 SnapshotSparkSchema，切完这个文件就是纯 JVM |
| read/MilvusSegmentManifestReader.scala、MilvusStorageV3ManifestReader.scala | core.manifest | 已迁 |
| read/MilvusDeltaLogReader.scala、MilvusDeletePlan.scala | core.delete | 已迁。改按行号位图是重构，未做 |
| src/main/resources/milvus-segment-manifest*.avsc | core 的 resources | 已迁。资源必须跟代码走，留在原处解码器会报 not found on classpath，而失败形式是返回 Left 不是抛异常 |
| serde/DataTypeUtil.scala、SchemaUtil.scala | core.schema、spark.types | 已迁。core.schema 得到 MilvusTypes、ArrowTypes、SchemaMapper、FieldMetadata；Spark 那一半是 spark.types 的 DataTypeUtil 与 MilvusSchemaUtil（文件名已改成对象名） |
| MilvusUtil.scala 的 FloatConverter、SparseFloatVectorConverter | core.codec | 已迁。文档原来写「MilvusUtil 整个进 apps.legacy」，不成立：627 行里只有 307 行是 FieldData 打包，两个转换器是纯 JVM 的列值编解码，被 ArrowConverter 和读路径用着 |
| MilvusUtil.scala 的 IntConverter | 删除 | 已删，全仓零引用 |
| Exception.scala | core 与 client | 已迁。DataParseException、DataTypeException 进 core；三个 RPC 异常进 client |
| read/V2SegmentLoader.scala | compat.v2packed | 已迁。resolvePath 与 readAllBytes 先下沉到 core 的 path 与 io.hadoop，否则 core 的两个 Manifest 解析器要反向依赖 compat |
| read/MilvusParquetFooterReader.scala | compat 根包 | 已迁。v2packed 和 backup 都要用它 |
| read/BackupMetaReader.scala | compat.backup | 已迁 |
| MilvusClient.scala | client.api、client.grpc | 已迁。重试拦截器拆进 client.grpc；收 MilvusOption 的工厂删掉，改由 MilvusOption.connectionParams 产出连接参数 |
| sources/MilvusDataSource.scala（2880 行） | spark.sources、spark.table、spark.read、spark.options | 已拆成 14 个文件，最大 550 行。`sources` 只留 TableProvider（FQN 被 apps 和用户作业按字符串引用，不能动）；MilvusTable→spark.table；ScanBuilder、Scan、四个规划入口（ClientSnapshotPlanner、LegacyClientPlanner、OptionSnapshotPlanner、BackupPlanner）、SnapshotPartitions、DeletePlanning、ClientReadSnapshot→spark.read；桶判定与 Hadoop 配置翻译（StorageOptions）、备份集合选取（BackupSelection）、ReadMode→spark.options。规划逻辑下沉 core.read.plan 未做，SnapshotPartitions.build 是要下沉的那部分 |
| MilvusOption.scala、loon/Properties.scala | spark.options | 已搬。MilvusOption 在 spark.options；MilvusOption 是混的，存储配置下沉 core.credential 是重构，未做。`loon/Properties.FsConfig` 的每个常量都是 core.credential.StorageProperties 的别名，调用方已全部改为直接用 StorageProperties，2026-09-14 连同 PropertiesTest 一起删除，`loon` 包不再存在；`loon/HadoopStorageConfig` 已搬到 spark.options，和 StorageOptions 是同一件事的两半 |
| read/MilvusLoonPartitionReader.scala、MilvusPartitionReaderFactory.scala、MilvusInputPartition.scala、MilvusPackedV2PartitionReader.scala | spark-base | 已搬。两个 reader 已改调 native-storage 的 JNI，不再经上游绑定；分发与出口下沉 core.read.exec、重写为列式仍是重构，未做 |
| serde/ArrowConverter.scala、ArrowAllocator.scala | spark.types | 已搬到 spark.types：它做的是 Arrow 值与 Spark InternalRow 的双向转换，就是 types 的职责。读路径由 ColumnVector 取代、写路径重写进 core.write.exec 是重构，未做 |
| filter/VectorBruteForceSearch.scala | spark-base | 已搬。它是从 MilvusLoonPartitionReader 的读路径里调的，不是 app；最终形态等决策 16 |
| write/MilvusLoonWriter.scala、MilvusV2BinlogWriter.scala | spark-base | 已搬，且已改调 native-storage 的 JNI（决策 14 选了自己封）。下沉 core.write.exec 仍是重构，未做 |
| write/MilvusWriteBuilder.scala、MilvusBatchWriter.scala、MilvusDataWriterFactory.scala、MilvusInsertDataWriter.scala、MilvusFieldData.scala（原 MilvusUtil.scala） | spark.write | 已搬到 spark.write，MilvusFieldData 在内（它只被 MilvusInsertDataWriter 用）。整条 `format("milvus")` 写链是一个整体，上半截是 DataSource V2 的接口实现；下放 apps.legacy 要先有 W7 的注册表，那是重构 |
| write/MilvusSparkNativeImportWriter.scala | 删除 | 已删，全仓零引用 |
| operations/backfill/* | apps.backfill | 已迁，包名从 operations.backfill 改成 apps.backfill |
| expressions/、extensions/ | apps.search | 已迁 |
| tools/* | apps.tools | 已迁 |
| src/test/**、src/it/** | 各模块的 src/test | 已迁。core 32、compat 43、client 20、spark-4.0 261、apps-4.0 212 个用例；integration-4.0 收 3 个集成用例 |
| milvus-storage/java 的 Java 绑定 | native-storage | 已完成。读写两侧的 loon_* 入口都由 native-storage 封装，unmanaged jar 与 Modules.legacyJni 一并移除，3.5 线的 Scala 2.12 因此解锁（core、compat、client、spark35 的 main 与 test 在 ++2.12.20 下实测编过）。欠一笔：per-batch reader 是过渡实现，结束条件是上游导出 loon_record_batch_reader_*，见 storage-access 需求 7 |

### 5.1 搬运中暴露的事实

1. 六个文件只因为 `org.apache.spark.internal.Logging` 才算 Spark 代码，其中三个继承了但一次都没调用，全仓实际日志调用只有 7 处。core 因此有了自己的 `Logging`（slf4j，provided）。
2. 1.x 的 `private[read]` 跨模块之后失效，14 个成员被迫改成 public。1.x 的封装边界是按目录划的，不是按职责划的。
3. 1.x 的 main 与 test 在 Spark 3.5.5、4.0.0、4.2.0 上都编得过，所以存量代码进 spark-base 让四条线各编一遍没有兼容风险。这是 spark-base 而不是单条线的依据。
4. parquet-hadoop 传递进来的 jackson-databind 比 jackson-module-scala 新，跨版本直接抛 JsonMappingException。第 2 层因此钉死 jackson；Spark 线不能钉，Spark 自带的是一套自洽的更新版本。
5. 挡住纯搬运的反向引用一共 6 处，全部是文件放错位置，不是真的循环依赖。
