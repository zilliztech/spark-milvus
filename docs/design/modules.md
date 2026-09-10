# 2.0 模块、包与目录 `[草稿]`

八个 sbt 模块，依赖只能向下：ops → spark-<line> → client、compat、core → native-storage、native-vector。核心层没有 Spark，原生层没有业务逻辑，场景和遗留代码只在 ops。功能编号见 capabilities.md。

## 1 模块

| 模块 | 层 | 包名 | 依赖 | 产物 |
|---|---|---|---|---|
| native-storage | 第 1 层 | `com.zilliz.milvus.native.storage` | milvus-storage 的 C 接口 | jar 内 `native/{os}-{arch}/` 平铺 .so |
| native-vector | 第 1 层 | `com.zilliz.milvus.native.vector` | knowhere 的 C shim | 同上 |
| core | 第 2 层 | `com.zilliz.milvus.storage` | native-storage、native-vector、Arrow、对象存储 SDK | 无 Spark 依赖的 jar |
| compat | 兼容入口 | `com.zilliz.milvus.storage.compat` | core | 三个适配器，产出 core 的 Snapshot 或 Reader |
| client | Milvus 服务客户端 | `com.zilliz.milvus.client` | ScalaPB、gRPC | DDL、Delete、快照、索引、登记的调用 |
| spark-base | 第 3 层共享源码 | `com.zilliz.spark.connector` | core、compat、client | 不发布 |
| spark-3.5 / 4.0 / 4.1 / 4.2 | 第 3 层，每条 Spark 线一个 | 同上 | spark-base 的源码加本线专属目录；本线 Spark 为 provided | `spark-milvus-<line>_<scala>` |
| bundle-<line> | 打包 | | spark-<line> | `spark-milvus-bundle-<line>_<scala>`：shaded fat jar，含 core、compat、client、native |
| ops | 场景与遗留 | `com.zilliz.spark.connector.ops` | spark-<line> | 每条线一个 fat jar |
| it | 集成测试 | | spark-<line>、ops；需 MinIO 和 Milvus | 不发布 |

Scala：3.5 线出 2.12 和 2.13，4.x 线只出 2.13；core、compat、client、ops 按两个版本交叉编译。

## 2 包

### 2.1 core `com.zilliz.milvus.storage`

| 包 | 职责 | 主要类型 | 功能 |
|---|---|---|---|
| `snapshot` | 列快照目录，选快照，解析 JSON 和 Avro 成对象 | SnapshotCatalog、Snapshot、Segment、SnapshotSource（接口，compat 实现） | R2、R3、R13、R16、C3 |
| `manifest` | 一个段的 Manifest：列组、删除文件、统计、索引登记 | Manifest、ColumnGroup、ManifestReader（经 native） | R4、R5 |
| `schema` | 字段 id、名字、Milvus 类型、Arrow 类型的唯一映射 | SchemaMapper、MilvusType、ArrowTypes | R15、C3 |
| `path` | 三种路径形态到 (bucket, key) | StoragePath、Located | R3 |
| `credential` | 对象存储凭证的取用和下发 | Credentials、CredentialSource | R3、W1 |
| `expr` | 中间表示、Milvus 文法解析器、列批求值器 | Expr（IR）、PlanParser（ANTLR）、Evaluator、Bitmap | R6、R7 |
| `delete` | 删除文件解码，按行号置位 | DeleteBitset、DeltaLogDecoder | R8 |
| `stats` | 段统计和 row group 统计的读取与剪枝 | SegmentStats、Pruner | R9、R10 |
| `read` | 分区规划、批读取、行号取列、出口 | Partitioner、ReadPlan、SegmentReader、ColumnBatch（地址、length、offset）、Take | R4、R14、R17 |
| `write` | 段写出、暂存布局、作业清单、提交 | SegmentWriter、StagingLayout、JobManifest、Committer | W1、W2、W3 |
| `index` | 索引文件编解码、来源、缓存、写出 | IndexFileCodec、IndexSource、IndexCache、IndexWriter | V2、V3、V4 |

core 对外只暴露 `snapshot`、`read`、`write`、`index`、`schema`、`expr` 六个包的公开类型；其余是实现细节。

### 2.2 compat `com.zilliz.milvus.storage.compat`

| 包 | 职责 | 功能 |
|---|---|---|
| `v2packed` | Storage V2 packed 段的 reader，实现 core.read 的 SegmentReader | K1 |
| `offline` | 1.x 离线 option 的段列表转 Snapshot，实现 SnapshotSource | K2 |
| `backup` | milvus-backup 导出目录转 Snapshot，实现 SnapshotSource | K3 |

### 2.3 native-storage `com.zilliz.milvus.native.storage`

| 包 | 职责 |
|---|---|
| `jni` | Java 类 StorageNative：每个 loon_* 一个 native 方法，句柄是 long，结果码转异常 |
| `arrow` | ArrowArray、ArrowSchema、ArrowArrayStream 三个 C 结构体的分配与 release |
| `loader` | 按 os 和 arch 解压 .so 到带版本号的目录后 System.load |

C 源码在 `native/storage/src/main/cpp`；构建产物按 `native/{os}-{arch}/` 进 jar。

### 2.4 native-vector `com.zilliz.milvus.native.vector`

| 包 | 职责 |
|---|---|
| `jni` | Java 类 VectorNative：mv_* 的 native 方法 |
| `shim` | C 源码：mv_* 包 knowhere::Index、BruteForce、BinarySet、Version；DiskANN 的本地 FileManager |

### 2.5 client `com.zilliz.milvus.client`

| 包 | 职责 |
|---|---|
| `grpc` | ScalaPB 生成的 stub，重试拦截器 |
| `api` | MilvusClient：数据库和 collection 的 DDL、describe、Delete、快照、索引、load、release、flush、compact、BatchUpdateManifest、RegisterSegments（待 Milvus 提供） |

### 2.6 spark `com.zilliz.spark.connector`

| 包 | 职责 | 功能 |
|---|---|---|
| `catalog` | MilvusCatalog：TableCatalog、SupportsNamespaces、loadTable 的快照重载 | R1、R2、C1、C2 |
| `table` | MilvusTable：schema、能力集、元数据列、统计、DeleteV2 | R12、R13、W5 |
| `scan` | ScanBuilder、Scan、Batch、InputPartition、ColumnarPartitionReader、ColumnVector 实现 | R4、R5、R11、R16 |
| `expr` | DataSource V2 Predicate 到 IR 的翻译；Arrow 类型到 Spark 类型的映射 | R6、R15 |
| `write` | WriteBuilder、BatchWrite、DataWriterFactory、DataWriter；truncate、overwrite、backfill 模式 | W1 到 W4 |
| `procedure` | Spark 4 的 ProcedureCatalog 实现；本线专属目录 | P1 到 P5 |
| `functions` | Spark 3.5 上同一批动作的函数入口；本线专属目录 | P1 到 P5 |
| `options` | option 名、别名、校验；1.x 名字的映射和告警 | K4 |

版本专属目录只放 `procedure`（4.x）和 `functions`（3.5）以及 createTable 的重载。

### 2.7 ops `com.zilliz.spark.connector.ops`

| 包 | 内容 | 功能 |
|---|---|---|
| `backfill` | BackfillApp、配置、join 键、列映射、merge 模式、结果 JSON | O1 |
| `tools` | ListV2SegmentsApp、ReadSourceOnlyApp | O2 |
| `search` | VectorBruteForceSearch、SQL 函数扩展 | O3 |
| `legacy` | gRPC Insert 写入器 | W7 |

四个包互不依赖，各自是独立入口。

## 3 目录

```
spark-milvus/
  build.sbt                        聚合、版本、发布
  project/                         插件、依赖版本、Spark 线和 Scala 版本矩阵
  native/
    storage/
      src/main/java/               com.zilliz.milvus.native.storage
      src/main/cpp/                JNI 源码
      src/main/resources/native/   {os}-{arch}/ 下的 .so，构建时放入
      build/                       构建和 patchelf 脚本
    vector/
      src/main/java/               com.zilliz.milvus.native.vector
      src/main/cpp/                C shim 与 JNI
  core/src/main/scala/com/zilliz/milvus/storage/{snapshot,manifest,schema,path,credential,expr,delete,stats,read,write,index}
  core/src/main/antlr4/            Plan.g4
  compat/src/main/scala/com/zilliz/milvus/storage/compat/{v2packed,offline,backup}
  client/src/main/scala/com/zilliz/milvus/client/{grpc,api}
  client/src/main/protobuf/        milvus-proto 子模块的引用
  spark/
    base/src/main/scala/com/zilliz/spark/connector/{catalog,table,scan,expr,write,options}
    3.5/src/main/scala/com/zilliz/spark/connector/{functions,catalog}
    4.0/src/main/scala/com/zilliz/spark/connector/procedure
    4.1/  4.2/                      同 4.0，无差异时为空目录加 build 声明
    bundle-3.5/ bundle-4.0/ bundle-4.1/ bundle-4.2/
  ops/src/main/scala/com/zilliz/spark/connector/ops/{backfill,tools,search,legacy}
  it/src/test/scala/               集成测试
  docs/design/                     设计文档
  docs/                            用户文档
```

每个模块的单测在各自的 `src/test/scala`；需要 .so 的单测标 tag，CI 在有 native 产物的 job 里跑。

## 4 构建约束

1. core、compat、client 的依赖里没有 spark-*；编译期用 scalac 的 import 检查（或 sbt 任务扫描源码）拒绝 `org.apache.spark`。
2. native-* 的 C 头文件不出现 JNI 类型；JNI 只在 `jni` 包。
3. ops 的每个包能单独删除而不影响编译。
4. 版本：`2.0.0-{branch}-{arch}-SNAPSHOT`；正式版按 `2.0.0` 加 Spark 线和 Scala 后缀发布。
5. 每条 Spark 线钉自己的 Spark 补丁版；Arrow 版本由 core 钉（arrow-c-data 和 arrow-memory），spark 模块不引 arrow-vector。

## 5 1.x 到 2.0 的迁移对照

| 1.x 文件 | 2.0 位置 | 处理 |
|---|---|---|
| sources/MilvusDataSource.scala（2879 行） | spark.catalog、table、scan；规划逻辑进 core.snapshot、core.read | 拆分重写 |
| MilvusOption.scala、loon/Properties.scala | spark.options；fs.* 归一到 core.credential | 合并重写 |
| MilvusClient.scala | client.grpc、client.api | 迁入，删 mock 和无调用的接口 |
| read/MilvusSnapshotReader.scala | core.snapshot | 迁入，去 Spark 依赖 |
| read/MilvusStorageV3ManifestReader.scala、MilvusSegmentManifestReader.scala | core.manifest | 迁入 |
| read/MilvusDeltaLogReader.scala、MilvusDeletePlan.scala | core.delete | 迁入，改按行号位图 |
| read/V2SegmentLoader.scala、MilvusPackedV2PartitionReader.scala、MilvusParquetFooterReader.scala | compat.v2packed | 迁入 |
| read/BackupMetaReader.scala | compat.backup | 迁入 |
| read/MilvusLoonPartitionReader.scala、MilvusPartitionReaderFactory.scala、MilvusInputPartition.scala | spark.scan 加 core.read | 重写为列式 |
| serde/DataTypeUtil.scala、SchemaUtil.scala、ArrowConverter.scala | core.schema；Spark 映射进 spark.expr | 合并为一份 |
| write/MilvusLoonWriter.scala、MilvusV2BinlogWriter.scala | core.write | 迁入，事务提交移到 Committer |
| write/MilvusInsertDataWriter.scala 等 gRPC 写 | ops.legacy | 迁入 |
| write/MilvusSparkNativeImportWriter.scala | 删除 | 无调用 |
| filter、expressions、extensions | ops.search | 迁入 |
| operations/backfill/* | ops.backfill | 迁入，改用 W2 |
| tools/* | ops.tools | 迁入 |
| milvus-storage/java 的 Java 绑定 | native-storage | 替换为自有 JNI |
