# 2.0 模块、包与目录 `[草稿]`

依赖只能向下：ops → spark-`<line>`（源码来自 spark-base）→ compat、client → core → native-storage、native-vector。核心层没有 Spark，原生层没有业务逻辑，场景和遗留代码只在 ops。功能编号见 [capabilities.md](capabilities.md)，名词沿用 README 第 0 节。

## 1 模块

| 模块 | 层 | 包名 | 依赖 | 产物 |
|---|---|---|---|---|
| native-storage | 第 1 层 | `com.zilliz.milvus.native.storage` | milvus-storage 的 C 接口 | `com.zilliz:spark-milvus-native-storage`，jar 内 `native/{os}-{arch}/` 平铺 .so |
| native-vector | 第 1 层 | `com.zilliz.milvus.native.vector` | knowhere 的 C shim | `com.zilliz:spark-milvus-native-vector`，同上布局 |
| core | 第 2 层 | `com.zilliz.milvus.storage` | native-storage、native-vector、Arrow C Data Interface、对象存储 SDK | `com.zilliz:spark-milvus-core_<scala>` |
| compat | 第 2 层 | `com.zilliz.milvus.storage.compat` | core | `com.zilliz:spark-milvus-compat_<scala>` |
| client | 第 2 层 | `com.zilliz.milvus.client` | core、ScalaPB、gRPC | `com.zilliz:spark-milvus-client_<scala>` |
| spark-base | 第 3 层 | `com.zilliz.spark.connector` | 不是 sbt project，只是各线引用的源码目录 | 无 |
| spark-3.5 / 4.0 / 4.1 / 4.2 | 第 3 层 | 同上 | spark-base 的源码 + 本线专属目录；core、compat、client；本线 Spark 为 provided | `com.zilliz:spark-milvus-<line>_<scala>` |
| bundle-`<line>` | 打包 | | 本线的 spark 模块 | `com.zilliz:spark-milvus-bundle-<line>_<scala>`：fat jar |
| ops-`<line>` | 第 4 层 | `com.zilliz.spark.connector.ops` | 本线的 spark 模块 | `com.zilliz:spark-milvus-ops-<line>_<scala>`：fat jar |
| it-`<line>` | 测试 | | 本线的 spark 与 ops 模块；需 MinIO 和 Milvus | 不发布 |

展开后约 17 个 sbt project：native 两个、core、compat、client、spark 四条线、bundle 四条线、ops 五个（3.5 的 2.12 和 2.13，4.x 三条线）、it 同 ops。交叉编译由 `crossScalaVersions` 控制。

Scala：3.5 线出 2.12 和 2.13，4.x 线只出 2.13；core、compat、client、spark-base、ops 交叉编译两个版本，bundle 和 it 跟随所在线。

1.x 的坐标 `com.zilliz:spark-connector_2.13` 在 2.0 之后不再更新，1.x 的修复仍发到它。

## 2 包

### 2.1 core `com.zilliz.milvus.storage`

对外只暴露 `snapshot`、`read`、`write`、`index`、`schema`、`expr` 六个包的公开类型；其余是实现细节。

| 包 | 职责 | 主要类型 |
|---|---|---|
| `snapshot` | 列快照目录，选快照，解析 JSON 和 Avro 成对象；分发 SnapshotSource | SnapshotCatalog、Snapshot、Segment、SnapshotSource、SnapshotSourceRegistry |
| `manifest` | 一个段的 Manifest：列组、删除文件、统计、索引登记 | Manifest、ColumnGroup、ManifestReader |
| `schema` | 字段 id、名字、Milvus 类型、Arrow 类型的唯一映射；不含 Spark 类型 | SchemaMapper、MilvusType、ArrowTypes |
| `path` | 三种路径形态到 (bucket, key) | StoragePath、Located |
| `credential` | 对象存储凭证的取用和下发 | Credentials、CredentialSource |
| `expr` | 中间表示、Milvus 文法解析器、列批求值器、反向打印器 | Expr、PlanParser、Evaluator、ExprPrinter、Bitmap |
| `delete` | 删除文件解码，按行号置位 | DeleteBitset、DeltaLogDecoder |
| `stats` | 段统计和 row group 统计的读取与剪枝 | SegmentStats、Pruner |
| `read.plan` | 分区规划，纯 JVM，可序列化 | Partitioner、ReadPlan、InputSpec |
| `read.exec` | 批读取、行号取列、出口；碰 native | SegmentReader、SegmentReaderRegistry、ColumnBatch、Take |
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

### 2.3 native-storage `com.zilliz.milvus.native.storage`

| 包 | 职责 |
|---|---|
| `jni` | StorageNative：每个 loon_* 一个 native 方法，句柄是 long，结果码转异常 |
| `arrow` | ArrowArray、ArrowSchema、ArrowArrayStream 三个 C 结构体的分配与 release |
| `loader` | 按 os 和 arch 解压 .so 到带版本号的目录后 System.load |

### 2.4 native-vector `com.zilliz.milvus.native.vector`

| 包 | 职责 |
|---|---|
| `jni` | VectorNative：mv_* 的 native 方法 |
| `shim` | C 源码：mv_* 包 knowhere::Index、BruteForce、BinarySet、Version；DiskANN 的本地 FileManager |

### 2.5 client `com.zilliz.milvus.client`

| 包 | 职责 |
|---|---|
| `grpc` | ScalaPB 生成的 stub，重试拦截器 |
| `api` | MilvusClient：DDL、describe、Delete、快照、索引、load、release、flush、compact、BatchUpdateManifest、RegisterSegments（待 Milvus 提供）；proto 的 DataType 与 core 的 MilvusType 互转 |

### 2.6 spark `com.zilliz.spark.connector`

| 包 | 职责 | 按线 |
|---|---|---|
| `catalog` | MilvusCatalog：TableCatalog、SupportsNamespaces、loadTable 的快照重载 | createTable 的接口差异按线 |
| `table` | MilvusTable：schema、能力集、元数据列、统计、DeleteV2 | 否 |
| `scan` | ScanBuilder、Scan、Batch、InputPartition、ColumnarPartitionReader、ColumnVector 实现 | 否 |
| `expr` | DataSource V2 Predicate 到 IR 的翻译 | 否 |
| `types` | Arrow 类型到 Spark 类型的映射，向量列的 Spark 表示 | 否 |
| `write` | WriteBuilder、BatchWrite、DataWriterFactory、DataWriter；truncate、overwrite、backfill 模式 | 否 |
| `options` | option 名、别名、校验；1.x 名字的映射和告警；把 compat 的实现注册进 core | 否 |
| `procedure` | Spark 4 的 ProcedureCatalog 实现 | 是 |
| `functions` | Spark 3.5 上同一批动作的函数入口 | 是 |
| `extensions` | SparkSessionExtensions、SQL 解析器扩展、优化规则 | 是 |

按线的还有 `META-INF/services` 资源。

### 2.7 ops `com.zilliz.spark.connector.ops`

| 包 | 内容 |
|---|---|
| `backfill` | BackfillApp、配置、join 键、列映射、merge 模式、结果 JSON |
| `tools` | ListV2SegmentsApp、ReadSourceOnlyApp |
| `search` | VectorBruteForceSearch、SQL 函数扩展 |
| `legacy` | gRPC Insert 的 TableProvider、DataSource V2 写栈、`format("milvus")` 短名注册 |

四个包互不依赖，各自是独立入口。`format("milvus")` 的短名归 ops 之后，只有加载 ops jar 才能用旧写法；三段名 `milvus.db.coll` 不需要 ops。

## 3 目录

```
spark-milvus/
  build.sbt                        聚合、版本、发布
  project/                         插件、依赖版本、Spark 线与 Scala 版本矩阵
  native/
    storage/
      src/main/java/               com.zilliz.milvus.native.storage
      src/main/cpp/                JNI 源码
      build/                       构建与 patchelf 脚本
    vector/
      src/main/java/               com.zilliz.milvus.native.vector
      src/main/cpp/                C shim 与 JNI
  core/
    src/main/scala/com/zilliz/milvus/storage/{snapshot,manifest,schema,path,credential,expr,delete,stats,read,write,index}
    src/main/antlr4/               Plan.g4
    src/main/resources/            段清单的 Avro schema
  compat/src/main/scala/com/zilliz/milvus/storage/compat/{v2packed,offline,backup}
  client/
    src/main/scala/com/zilliz/milvus/client/{grpc,api}
    src/main/protobuf/             milvus-proto 子模块的引用
  spark/
    base/src/main/scala/com/zilliz/spark/connector/{table,scan,expr,types,write,options}
    3.5/src/main/{scala,resources}/  catalog、functions、extensions、META-INF/services
    4.0/  4.1/  4.2/                 catalog、procedure、extensions、META-INF/services
    bundle-3.5/ bundle-4.0/ bundle-4.1/ bundle-4.2/
  ops/
    base/src/main/{scala,resources}/ com.zilliz.spark.connector.ops.{backfill,tools,search,legacy}
    3.5/  4.0/  4.1/  4.2/           各引 base 的源码，依赖本线 spark 模块
  it/
    base/src/test/scala/             集成测试
    3.5/  4.0/  4.1/  4.2/
  docs/design/                     设计文档
  docs/                            用户文档
```

单测在各模块的 `src/test/scala`；需要 .so 的单测标 tag，CI 在有 native 产物的 job 里跑。

## 4 构建约束

1. core、compat、client 的依赖里没有 spark-*；用 sbt 任务扫描源码，出现 `org.apache.spark` 即编译失败。
2. native-* 的 C 头文件不出现 JNI 类型；JNI 只在 `jni` 包。原生库只在 executor 加载：`core.read.exec`、`core.write.exec`、`core.index` 之外的 core 包不得调用 native，driver 侧要读的 Manifest 字段由纯 JVM 解析器读。
3. Arrow 版本由 spark-`<line>` 钉（3.5 用 15，4.0 用 18.1，4.1 用 18.3，4.2 按本线）；core 只按 Arrow C Data Interface 编译，`arrow-c-data`、`arrow-format` 标 provided，不依赖 `arrow-memory-*`。
4. Java 目标版本按线：core、compat、client、native-* 钉 `-release 11`；spark-`<line>`、bundle、ops 按本线（3.5 用 11，4.x 用 17）。
5. 交叉编译的模块统一 `import scala.jdk.CollectionConverters._`，加 `scala-collection-compat` 为 2.12 补齐，禁止 `scala.collection.JavaConverters`。
6. bundle 只 relocate protobuf 和 guava；`com.zilliz.milvus.native.**` 和 `org.apache.arrow.**` 不 relocate，JNI 的导出符号已按包名编进 .so；`META-INF/services` 用 merge 策略。
7. ops 的每个包能单独删除而不影响编译。

## 5 1.x 到 2.0 的迁移对照

41 个 1.x 源文件的去向。

| 1.x 文件 | 2.0 位置 | 处理 |
|---|---|---|
| sources/MilvusDataSource.scala（2879 行） | spark.catalog、table、scan；规划逻辑进 core.snapshot、core.read.plan | 拆分重写 |
| MilvusOption.scala、loon/Properties.scala | spark.options；fs.* 归一到 core.credential | 合并重写 |
| MilvusClient.scala | client.grpc、client.api | 迁入，删 mock 和无调用的接口 |
| MilvusUtil.scala（627 行，值打成 gRPC FieldData） | ops.legacy | 迁入，只有 gRPC Insert 用 |
| Exception.scala | core 定义异常基类，各层派生 | 重写 |
| read/MilvusSnapshotReader.scala | core.snapshot | 迁入，去 Spark 依赖 |
| read/MilvusStorageV3ManifestReader.scala、MilvusSegmentManifestReader.scala | core.manifest | 迁入 |
| read/MilvusDeltaLogReader.scala、MilvusDeletePlan.scala | core.delete | 迁入，改按行号位图 |
| read/V2SegmentLoader.scala、MilvusPackedV2PartitionReader.scala、MilvusParquetFooterReader.scala | compat.v2packed | 迁入 |
| read/BackupMetaReader.scala | compat.backup | 迁入 |
| read/MilvusLoonPartitionReader.scala、MilvusPartitionReaderFactory.scala、MilvusInputPartition.scala | spark.scan、core.read | 重写为列式 |
| serde/DataTypeUtil.scala、SchemaUtil.scala | core.schema（Milvus 与 Arrow）、spark.types（Arrow 与 Spark） | 合并为两份，去 Spark 依赖 |
| serde/ArrowConverter.scala（897 行行式转换） | 删除 | 读路径由 ColumnVector 取代，写路径的 Spark 到 Arrow 重写进 core.write.exec |
| write/MilvusLoonWriter.scala、MilvusV2BinlogWriter.scala | core.write | 决策 14 定为复用时迁入，事务提交移到 Committer；定为重写时删除 |
| write/MilvusInsertDataWriter.scala、MilvusWriteBuilder.scala、MilvusBatchWriter.scala、MilvusDataWriterFactory.scala | ops.legacy | 随 W7 的 `format("milvus")` 入口整体迁入，先修 abort |
| write/MilvusSparkNativeImportWriter.scala | 删除 | 无调用 |
| filter/、expressions/、extensions/ | ops.search | 迁入 |
| operations/backfill/* | ops.backfill | 迁入，内部改用 W2 |
| tools/* | ops.tools | 迁入 |
| src/main/resources/milvus-segment-manifest*.avsc | core 的 resources | 迁入 |
| milvus-storage/java 的 Java 绑定 | native-storage | 替换为自有 JNI |
