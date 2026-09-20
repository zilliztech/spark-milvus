# Milvus Spark Connector 参数参考文档

本文档详细说明了 Milvus Spark Connector 的所有参数配置。

## 向量搜索（refactor/v2）

`com.zilliz.spark.connector.read.MilvusSearch.search` 输入一组查询，返回每条查询
跨段全局 TopK 的 DataFrame，调用 `collect`、`show` 等 action 时才执行：

```scala
val queries = Seq((1L, Array(0.1f, 0.2f)), (2L, Array(0.3f, 0.4f)))
  .toDF("query_id", "vector")
val hits = MilvusSearch.search(
  spark, options, queries, "embedding", 10, "COSINE",
  mode = "index",
  searchParameters = Map("ef" -> "256"),
  filter = Some("category == \"documents\" and rating >= 2.0"),
  outputColumns = Seq("id", "title")
)
hits.orderBy("query_id", "rank").show(false)
```

`options` 复用普通读取的快照与存储参数。查询集有两列：`query_id` 是非空且唯一的
BIGINT，`vector` 的类型随字段——FloatVector、Float16Vector、BFloat16Vector 用
`ARRAY<FLOAT>`，Int8Vector 用取值在 −128 到 127 的 `ARRAY<SMALLINT>`，
BinaryVector 用 `BINARY`。另有一个以 `queryVector: Array[Float]` 代替查询集的
重载，它构造只有一行、`query_id = 0` 的查询集。

结果列依次是 `query_id`、`rank`（从 1 开始）、`_score`、`_segment_id`、
`_row_offset`，后接 `outputColumns`。每条查询最多 K 行，行之间的先后以 `rank`
为准；DataFrame 本身无序，按 `query_id`、`rank` 排序后阅读。COSINE 与 IP 分数大的
排前，L2 分数小的排前，同分按段 ID 和段内物理行号排序。`_score` 保留 Knowhere
返回值；包含向量量化的索引（如 Cardinal 的 RBQ）可能返回近似分数，Connector 不读
原始向量重算。

`mode = "index"` 用快照钉住的持久化索引，能加载 HNSW 家族（`HNSW`、`HNSW_SQ`、
`HNSW_PQ`、`HNSW_PRQ`，含 Cardinal 构建写出的 HNSW）、IVF 家族（`IVF_FLAT`、
`IVF_SQ8`、`IVF_PQ`、`BIN_IVF_FLAT`）以及 `FLAT`、`BIN_FLAT`，元素类型随列，查询
metric 要与索引一致。nullable 列只对有值的行建索引，索引文件里的 `valid_data` 位图
给出行号对应关系，没有这份位图时报错。DiskANN、稀疏索引、GPU 索引和加密索引在规划时
报错。`mode = "exact"` 逐条算距离，支持全部稠密向量类型，二值向量用 HAMMING 或
JACCARD。

`filter` 在搜索前应用。支持标量比较（`==`、`!=`、`<`、`<=`、`>`、`>=`）、
`in`、`not in`、`is null`、`is not null`、`and`、`or`、`not` 和括号。
未知字段、错误类型和不支持的语法在执行前报错。对结果 DataFrame 再调用 `filter`
是过滤已选中的结果，不能替代这里的条件。表达式暂不支持 JSON、数组和函数。

缺失索引元数据、索引的 metric 或行数与段不符，在规划时报错并列出所有这样的段；
损坏文件、格式不兼容在任务加载索引时报错。只有快照明确表示该字段在这个段上没有索引
时，`allowUnindexed = true` 才让这个段改用精确扫描，默认 `false`。索引由每个任务
独占并关闭，不跨任务缓存。搜索参数按索引家族给：HNSW 家族用整数 `ef`，不得小于 K，默认 `max(64, K)`；IVF 家族
用正整数 `nprobe`，默认 16；FLAT 不接受参数。加密索引和
nullable 向量行号映射暂不支持。Cardinal `_mem.index.bin` 要求启用 Cardinal 的固定
版本原生产物，见[构建说明](contributing.md#knowhere-library-loading)。

三个选项决定作业规模。`milvus.search.queries.max.bytes`（默认 1 GiB）是查询集走
广播的字节上限，超过就随 shuffle 下发，两条路结果相同；
`milvus.search.group.max.bytes`（默认 512 MiB）是一个任务一次回答多少查询，按
「查询数 ×（维度 × 元素宽度 + K × 28 字节）」计；
`milvus.search.vectors.max.bytes`（默认 2 GiB）是一个任务同时留在内存里的向量字节
上限，也决定一个任务读多少个段。

每次搜索注册一组累加器，在 Spark 的 stage 页面可见：`milvus.search.segments`、
`milvus.search.read.bytes` 与 `milvus.search.read.nanos`、
`milvus.search.index.bytes` 与 `milvus.search.index.load.nanos`、
`milvus.search.bitmap.nanos`、`milvus.search.knowhere.calls` 与
`milvus.search.knowhere.nanos`、`milvus.search.candidates`、
`milvus.search.take.rows` 与 `milvus.search.take.nanos`。Knowhere 在自己的线程池里
计算，这部分耗时看 `milvus.search.knowhere.nanos`，不计入任务的 CPU 时间。

## 版本兼容性

**此连接器需要 Milvus 2.6 或更高版本**（Storage V2）。

如需支持 Milvus 2.5 及更早版本，请使用 `legacy` 分支（不再积极维护）。

## 概述

Milvus Spark Connector 提供了 **`milvus`** 数据源格式，用于 Milvus 数据的读写操作。

此外，还提供了一个便捷的 `MilvusDataReader` 工具类，用于简化集合数据的读取操作。

## Catalog 目录、表 DDL 与快照时间旅行

注册一次 `MilvusCatalog` 后，Milvus collection 可作为 Spark 三段表名使用。Spark 会移除
`spark.sql.catalog.milvus.` 前缀，再把其余连接、对象存储和读取选项交给 Connector。

```scala
spark.conf.set(
  "spark.sql.catalog.milvus",
  "com.zilliz.spark.connector.catalog.MilvusCatalog"
)
spark.conf.set("spark.sql.catalog.milvus.milvus.uri", "http://localhost:19530")
spark.conf.set("spark.sql.catalog.milvus.milvus.token", "your-token")
spark.conf.set("spark.sql.catalog.milvus.fs.address", "s3.us-west-2.amazonaws.com")
spark.conf.set("spark.sql.catalog.milvus.fs.bucket_name", "your-milvus-bucket")
spark.conf.set("spark.sql.catalog.milvus.fs.root_path", "files")
spark.conf.set("spark.sql.catalog.milvus.fs.cloud_provider", "aws")
spark.conf.set("spark.sql.catalog.milvus.fs.region", "us-west-2")
spark.conf.set("spark.sql.catalog.milvus.fs.use_ssl", "true")
spark.conf.set("spark.sql.catalog.milvus.fs.use_iam", "true")

val latest = spark.table("milvus.default.products")
val named = spark.sql(
  "SELECT * FROM milvus.default.products VERSION AS OF 'release-2026-09'"
)
val atTime = spark.sql(
  "SELECT * FROM milvus.default.products TIMESTAMP AS OF '2026-09-16 08:00:00'"
)
```

同一个 Catalog 通过 Spark 的目录命令列出 Milvus database 与 collection：

```sql
SHOW NAMESPACES IN milvus;
SHOW TABLES IN milvus.default;
SHOW TABLES IN milvus.default LIKE 'product*';
```

一个 Milvus database 对应一层 Spark namespace，其中的 collection 对应 table；不支持嵌套
namespace。因此 `SHOW TABLES` 必须显式指定 database；`SHOW TABLES IN milvus` 不会隐式选择
`default`，也不会合并多个 database 的 collection。已存在但没有 collection 的 database 返回空结果。
collection 直接按 Milvus 元数据列出，即使它还没有可读快照也会出现。可选的 `LIKE` 模式由 Spark
在目录结果上执行。名称保持 Milvus 返回的原样，结果顺序不作保证。

标识符必须恰好包含一个 database 和一个 collection；这两个名字会覆盖 Catalog 配置里的
`milvus.database.name` 与 `milvus.collection.name`。普通加载取最新快照，`VERSION AS OF`
按快照名精确匹配，`TIMESTAMP AS OF` 取 Milvus HybridTS 边界不晚于 Spark 解析后时刻的最新快照。
时间字面量先按 `spark.sql.session.timeZone` 解析，Catalog 收到 UTC epoch 微秒。返回的 Table 与后续
Scan 始终使用本次解析出的同一个固定快照。对象存储配置需按部署替换；不能使用 IAM 时，以
`fs.access_key_id` 和 `fs.access_key_value` 替代 `fs.use_iam=true`。

时间旅行选择的是快照元数据，不承诺历史数据保留。Connector 不负责保留旧段文件；compaction
或垃圾回收可能使已经选中的旧快照无法读取。

### CREATE 与 DROP TABLE

`CREATE TABLE` 先创建一个 Milvus collection，再为每个向量字段创建索引。主键、有歧义的 Milvus
字段类型、类型参数和向量索引都通过明确的表属性传入：

```sql
CREATE TABLE milvus.default.products (
  id BIGINT NOT NULL,
  title STRING,
  embedding ARRAY<FLOAT>
)
TBLPROPERTIES (
  'milvus.primary.key' = 'id',
  'milvus.field.title.data_type' = 'varchar',
  'milvus.field.title.max_length' = '512',
  'milvus.field.embedding.data_type' = 'float_vector',
  'milvus.field.embedding.dim' = '768',
  'milvus.index.embedding' =
    '{"index_type":"HNSW","metric_type":"COSINE","M":16,"efConstruction":200}'
);

DROP TABLE milvus.default.products;
```

| 属性 | 取值与约束 |
|---|---|
| `milvus.primary.key` | 必填的 schema 字段；必须非空，且映射成 Milvus Int64 或 VarChar；不支持 AutoID。 |
| `milvus.field.<field>.data_type` | Spark String、Array、Binary、Map 字段必填。值是大小写不敏感的 snake_case：`varchar`、`text`、`json`、`array`、`float_vector`、`float16_vector`、`bfloat16_vector`、`int8_vector`、`binary_vector`、`sparse_float_vector`。 |
| `milvus.field.<field>.max_length` | VarChar 与 `Array<String>` 必填的正整数。 |
| `milvus.field.<field>.max_capacity` | Array 必填的正整数。 |
| `milvus.field.<field>.dim` | 所有稠密向量必填的正整数；BinaryVector 的维度还必须是 8 的倍数。 |
| `milvus.index.<field>` | 每个向量字段必填的 JSON object。`index_type` 与 `metric_type` 是必填字符串，`index_name` 是可选字符串；其他键作为 Milvus index params，值只能是字符串、数字或布尔值。 |

这里的 `milvus.index.<field>` 是 Catalog 表属性，值是一个用于在线 CreateIndex 的完整 JSON object；
它与段内索引写出所用的 DataFrame write option 相互独立。

Spark Boolean、Byte、Short、Int、Long、Float、Double 分别直接映射为 Milvus Bool、Int8、Int16、
Int32、Int64、Float、Double，这些字段反而不能再写 `data_type`。String 只接受 `varchar`、`text`、
`json`；Boolean、Short、Int、Long、Float、Double、String 元素的标量 Array 可写 `array`，
`Array<Float>` 还可写 `float_vector`、`float16_vector`、`bfloat16_vector`，`Array<Short>` 可写
`int8_vector`。Catalog 拒绝 `Array<Byte>`：现有读链会把 Milvus Int8 Array 固定呈现为
`Array<Short>`，接受它会导致第一次快照后的 Spark schema 改变。Binary 只接受 `binary_vector`；只有精确的
`Map<Long, Float>` 接受 `sparse_float_vector`。

Catalog 接受 Spark SQL 默认的 `ArrayType.containsNull=true` 与 `MapType.valueContainsNull=true` schema
标记，因为 SQL DDL 无法可靠表达元素级 NOT NULL，而且这两个标记不能证明实际数据一定含 null 元素。
字段本身的 nullable 仍会保留并约束主键。元素级 null 不属于 Catalog DDL 合同，调用方不能由这两个标记
推断后续写入支持 null 元素。

属性名里的字段引用区分大小写，只有 `data_type` 值不区分大小写。正整数属性必须是 Int 范围内的规范
十进制 `[1-9][0-9]*`。索引 JSON 拒绝重复键、尾随内容、null、array、嵌套 object、空的必填值和
重复的显式 index name。table `comment` 成为 collection description，column comment 成为 field
description；存在 `provider` 时只能是 `milvus`，Spark 的 `owner` 属性忽略。字段无效、Milvus 或
bookkeeping 属性未知、类型不兼容、列特性不支持或包含 partition transform，都会在发出任何 RPC 前失败。

创建 collection 和创建索引是分开的 Milvus 操作，不是一个事务。如果 collection 创建成功后某个索引
请求失败，collection 和此前成功的索引会保留。`CREATE TABLE` 不创建 Connector 快照；只有 Milvus
产生快照以后，Catalog 才能读取这张表。CTAS 当前不能完成：collection 创建后没有可供 Spark 写入的
Connector 快照，命令会失败并可能留下需要显式删除的空 collection；collection 创建、段写入和段登记
本身也没有共同事务。

CREATE 预检确认 database 不存在时抛 `NoSuchNamespaceException`，确认 collection 已存在时抛
`TableAlreadyExistsException`。`DROP TABLE` 在确认 database 或 collection 任一不存在时返回 `false`；
认证、授权、传输和服务故障仍然报错。存在性
检查与变更是分开的远端调用，因此条件 DDL 可能与其他客户端竞争，最终以 Milvus 对变更请求的响应为准。

Catalog 表和目录发现只支持 client 模式，必须配置 `milvus.uri`。目录发现只访问 Milvus 服务，
不读取快照元数据或对象存储。确认不存在的 database 按 Spark 的 namespace 不存在错误返回。认证、
授权、网络、超时、限流及其他服务故障原样报错；空结果只来自成功的目录响应。离线的
`milvus.snapshot.path` 与 `milvus.backup.dir` 仍通过 `format("milvus")` 读取。namespace 的
CREATE/ALTER/DROP 与 table 的 ALTER/RENAME 仍不支持。table CREATE/DROP 不支持 AutoID、dynamic
field、partition key、table constraint、generated/default/identity column 或 Spark partition transform。

## 1. `MilvusDataReader` 便捷读取方法

`MilvusDataReader` 提供了一个便捷的方法来读取集合数据。

```scala
import com.zilliz.spark.connector.{MilvusDataReader, MilvusDataReaderConfig, MilvusOption}

// 基本用法
val milvusDF = MilvusDataReader.read(
  spark,
  MilvusDataReaderConfig(
    uri = "http://localhost:19530",
    token = "your-token",
    collectionName = "your_collection"
  )
)

// 带额外配置的用法
val milvusDFWithOptions = MilvusDataReader.read(
  spark,
  MilvusDataReaderConfig(
    uri = "http://localhost:19530",
    token = "your-token",
    collectionName = "your_collection",
    options = Map(
      MilvusOption.MilvusDatabaseName -> "your_database",
      MilvusOption.MilvusPartitions -> "100,101",
      MilvusOption.MilvusSegments -> "2001,2002"
    )
  )
)
```

### 1.1 MilvusDataReaderConfig 参数说明

| 参数名 | 类型 | 必需 | 描述 |
|--------|------|------|------|
| `uri` | String | 是 | Milvus 服务器连接 URI |
| `token` | String | 是 | Milvus 认证令牌 |
| `collectionName` | String | 是 | 集合名称 |
| `options` | Map[String, String] | 否 | 额外的配置选项，支持以下参数 |

### 1.2 支持的 options 参数

**基本连接参数：**
- `MilvusOption.MilvusDatabaseName` - 数据库名称
- `MilvusOption.MilvusPartitions` - 逗号分隔的数值分区 ID
- `MilvusOption.MilvusSegments` - 逗号分隔的数值段 ID

**S3 存储参数：**
- `MilvusOption.S3Endpoint` - S3 服务端点
- `MilvusOption.S3BucketName` - S3 存储桶名称
- `MilvusOption.S3RootPath` - S3 根路径
- `MilvusOption.S3AccessKey` - S3 访问密钥
- `MilvusOption.S3SecretKey` - S3 秘密密钥
- `MilvusOption.S3UseSSL` - 是否使用 SSL 连接（"true"/"false"）
- `MilvusOption.S3PathStyleAccess` - 是否使用路径样式访问（"true"/"false"）
  - **注意**：如果使用阿里云 OSS，需要手动设置为 "false"
  - 其他 S3 兼容存储通常不需要设置此参数

### 1.3 S3 配置示例

```scala
// 阿里云 OSS 配置示例
val ossOptions = Map(
  MilvusOption.S3Endpoint -> "oss-cn-hangzhou.aliyuncs.com",
  MilvusOption.S3BucketName -> "your-bucket-name",
  MilvusOption.S3RootPath -> "your-root-path",
  MilvusOption.S3AccessKey -> "your-access-key",
  MilvusOption.S3SecretKey -> "your-secret-key",
  MilvusOption.S3UseSSL -> "true",
  MilvusOption.S3PathStyleAccess -> "false",  // OSS 需要设置为 false
  MilvusOption.MilvusDatabaseName -> "default"
)

// AWS S3 配置示例
val s3Options = Map(
  MilvusOption.S3Endpoint -> "s3.amazonaws.com",
  MilvusOption.S3BucketName -> "your-bucket-name",
  MilvusOption.S3RootPath -> "your-root-path",
  MilvusOption.S3AccessKey -> "your-access-key",
  MilvusOption.S3SecretKey -> "your-secret-key",
  MilvusOption.S3UseSSL -> "true",
  MilvusOption.MilvusDatabaseName -> "default"
  // S3PathStyleAccess 通常不需要设置
)
```

### 1.4 工作原理

表加载（`getTable` 或 Catalog 的 `loadTable`）只解析一次不可变快照。schema、选中的段、统计和扫描计划都来自同一个
`Snapshot`，扫描期间不会再向服务或对象存储查询更新的视图。driver 用于物化快照元数据的
每个对象存储句柄都会在成功或失败后关闭，不会进入 executor 任务。每个 Spark 分区读取一个数据段：
`storage_version = 3` 由 executor 打开钉住版本的段 Manifest；`storage_version = 2`
由 executor 打开任务中列出的列组 parquet，两条线共用同一个行式 reader。driver 只下发删除文件描述，不下发已解码的主键
Map；executor 读取并关闭适用于本段的删除文件，再按主键和时间戳剔除行。删除文件不可读，或段实际
输出行数少于声明值时，task 直接失败，不返回不完整数据。

### 1.5 指标

每次读写都把它在 C/JVM 边界上的开销作为任务指标报到 Spark SQL 页的 scan 或 write 节点上，不用打开任何开关。scan 报 `milvus.jni.calls`、`milvus.jni.nanos`、`milvus.arrow.batches`、`milvus.arrow.bytes`（过界的 Arrow 字节）、`milvus.copies` 与 `milvus.copied.bytes`（原生侧因批被切片而拷贝的列数与字节数）、`milvus.rows.materialized`（转成 Spark 行的行数，列式路径为 0）、`milvus.arrow.allocated.max`（Arrow allocator 峰值，跨任务取最大）。write 报前四个和峰值。段数据从对象存储读了多少字节不在其中：那次读取发生在 milvus-storage 内部。

## 2. `milvus` 格式参数

### 2.1 连接参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.MilvusUri` | String | 条件 | - | Milvus 服务器连接 URI，格式：`http://host:port` 或 `https://host:port`。客户端模式必需；snapshot/backup 模式（离线读）不要求。。client 模式还要设 `fs.root_path` 为 Milvus 的 `minio.rootPath`（Zilliz Cloud 上是实例 id，自建 Milvus 默认 `files`）：快照目录是 `<root>/snapshots/<collection id>/metadata/` |
| `MilvusOption.MilvusToken` | String | 否 | "" | Milvus 服务器认证令牌 |
| `MilvusOption.MilvusDatabaseName` | String | 否 | "" | 数据库名称，默认为 default 数据库 |

### 2.2 SSL/TLS 配置参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.MilvusServerPemPath` | String | 否 | "" | 服务器证书文件路径（单向 TLS） |
| `MilvusOption.MilvusClientKeyPath` | String | 否 | "" | 客户端私钥文件路径（双向 TLS） |
| `MilvusOption.MilvusClientPemPath` | String | 否 | "" | 客户端证书文件路径（双向 TLS） |
| `MilvusOption.MilvusCaPemPath` | String | 否 | "" | CA 证书文件路径（双向 TLS） |

### 2.3 数据操作参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.MilvusCollectionName` | String | 条件 | - | 集合名称。客户端模式必需；backup 模式仅当备份含多个集合时必需。 |
| `MilvusOption.MilvusCollectionID` | String | 否 | "" | 集合 ID，通常自动获取 |
| `MilvusOption.MilvusPartitions` (`milvus.partitions`) | String | 否 | 未设置 | 逗号分隔的数值分区 ID。任意快照来源解析完成后统一应用；每个 ID 都必须存在。重复值会去重，并保留第一次出现的顺序。 |
| `MilvusOption.MilvusSegments` (`milvus.segments`) | String | 否 | 未设置 | 逗号分隔的数值段 ID。可与 `milvus.partitions` 同时使用，此时读取二者交集；每个 ID 都必须存在。 |
| `MilvusOption.ReaderFieldIDs` (`fieldIDs`) | String | 否 | 未设置 | 逗号分隔的数值字段 ID，在 schema 推导和 Table 构建时都生效。每个 ID 都必须存在于快照 schema，Spark 投影还可在此基础上继续裁剪。外部 `.schema()` 的非元数据字段必须恰好选中这些 ID，字段名和 Spark 类型也必须与快照一致。 |
| `MilvusOption.MilvusExtraColumns` (`milvus.extra.columns`) | String | 否 | "" | 逗号分隔的元数据列，只支持 `_segment_id`、`_row_offset`、`_timestamp`，见第 4 节。 |
| `MilvusOption.MilvusFilter` (`milvus.filter`) | String | 否 | 未设置 | 普通表读取使用的 Milvus 标量表达式。规划期按固定快照完成解析与校验，行式和列式 reader 都执行，并在 Limit 前与 Spark 谓词、删除共同生效。向量搜索的过滤条件由 `MilvusSearch.search` 的 `filter` 参数给出。 |
| `MilvusOption.ReadApplyDeletes` (`milvus.read.apply.deletes`) | Boolean | 否 | true | 应用固定快照可见的段内删除、本分区 L0 删除与全 collection L0 删除。设为 `false` 是显式关闭；只要显式提供，除 `true`、`false` 外的值（包括空白值）都会报错。 |
| `milvus.read.vector.raw` | Boolean | 否 | false | 向量列的输出类型。默认 false，向量转成 Spark 原生类型（`FloatVector`/`Float16Vector`/`BFloat16Vector` → `ArrayType(FloatType)`，`Int8Vector` → `ArrayType(ShortType)`，`SparseFloatVector` → `MapType(LongType, FloatType)`）。设为 true 时向量列输出 `BinaryType`，字节按存储原样给出，由调用方自己按 `dim` 与元素类型解析；这条路径不做逐元素转换，适合把字节直接交给下游原生库的批量作业 |
| `milvus.read.columnar` | Boolean | 否 | true | 读出口形态。默认 true，整批交付（`ColumnarBatch`），直接包住原生 buffer 不拷贝，向量列按 `milvus.read.vector.raw` 决定的类型呈现；有删除的批按存活行下标映射交付，同样不拷贝。设为 false 逐行交给 Spark。行式和列式 reader 共用同一套预期行数校验。 |
| `milvus.read.batch.max.rows` | Int | 否 | 8192 | 每个 milvus-storage record batch 请求的最大正行数，映射为 `reader.record_batch_max_rows`。 |
| `milvus.read.batch.max.bytes` | Long | 否 | 33554432 | 每个原生 record batch 的正目标字节上限，映射为 `reader.record_batch_max_size`；当前上游最大值为 4294967296（4 GiB）。 |
| `milvus.read.arrow.max.bytes` | Long | 否 | 9223372036854775807 | 每个 Spark read task 独占的 Arrow child allocator 正硬上限。覆盖行式、列式与向量路径导入/读取的 Arrow buffer，不包含 milvus-storage 独立的 native 内存池。 |

显式提供的选择器列表不接受空白值、空项或非数值。布尔读选项（`milvus.snapshot.mode`、
`milvus.read.apply.deletes`、`milvus.read.vector.raw`、`milvus.read.columnar`）只接受
不区分大小写的 `true` 或 `false`；空白值和拼写错误都会直接报错，不会回退到默认值。
正整数/正 Long option 不接受空白、零、负数、非十进制和溢出值，错误同时给出键与原始值。
显式提供的 `milvus.snapshot.max.json.bytes` 必须是正整数。
`topK` 必须是正整数；任一显式提供的向量搜索选项都不能是空白值，缺项或格式错误都在规划期失败。
既有 connector 值遵循同一规则：`milvus.insertMaxBatchSize`（默认 5000）、
`milvus.retry.count`（3）、`milvus.retry.interval`（1000）、`s3.maxConnections`（32）和
`s3.preloadPoolSize`（4）都是正 Int；`s3.useSSL` 与 `s3.pathStyleAccess` 是严格 Boolean。

#### Spark 谓词下推

Spark SQL 与 DataFrame 的 `where` 条件通过 DataSource V2 下推。Bool 字段支持等于、不等于、null-safe
等值（`<=>`）、`IN` 和空值判断；数值字段支持六种比较、`<=>`、`IN` 和空值判断；String、
VarChar、Text 还支持前缀与后缀条件。行式与列式读取都保持 Spark SQL 的三值 NULL 语义。

连接器只在整棵谓词树都受支持时接受它。未支持的操作符、cast、嵌套引用，以及 JSON、Array、
Geometry、向量和合成元数据列上的谓词留在 Spark 计划中，由 Spark 求值。只被谓词引用的列会在
内部读取，不会出现在结果 schema。要在向量搜索前过滤，使用
`MilvusSearch.search(..., filter = ...)`。DataSource V1 Filter 接口不支持。

#### Milvus 标量过滤

普通表读取可以使用同一套 Milvus 标量表达式子集：

```scala
spark.read
  .format("milvus")
  .options(readOptions)
  .option("milvus.filter", "category == \"documents\" and rating >= 2.0")
  .load()
```

支持 Bool、整数、Float、Double、String、VarChar 与 Text 字段；语法包括 `==`、`!=`、`<`、`<=`、
`>`、`>=`、`IN`、`NOT IN`、`IS NULL`、`IS NOT NULL`、`AND`、`OR`、`NOT`；Bool 比较只支持
`==` 与 `!=`。只用于过滤的字段会在
内部读取，但不会进入结果 schema。若同时下推 Spark `where`，两者都必须通过；随后应用删除，Limit 只统计
最终存活行。空白或错误表达式、未知字段、不兼容字面量都在规划期报错，不会被忽略。

`milvus.filter` 只用于普通扫描；向量搜索在 TopK 前用 `MilvusSearch.search` 的 `filter` 参数。
当前标量子集不含 JSON path、Array 谓词和 `json_contains`。


### 2.4 写入参数

`df.write.format("milvus").mode("append")` 把 DataFrame 直接写成 Milvus 段到对象存储，不经 Milvus
服务，也不做登记。表的解析和读一样，collection schema 来自三者之一：`milvus.uri` 加 collection
名（取该 collection 最新的快照）、`milvus.snapshot.path`、或 `milvus.snapshot.schema.bytes`（只给
schema，没有任何快照时用）。字段 id 和向量维度都从这份 schema 取。

当前暂存段还不包含 Milvus 系统字段 RowID（字段 0）和 Timestamp（字段 1），因此暂时不能登记到
Milvus，也不能由 Milvus 加载。append 登记仍受设计决策 22 和 Milvus `RegisterSegments` API 阻塞。

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `fs.root_path` | String | 否 | `files` | 作业写到 `{root}/staging/{job-id}/` 下。task 启动前先写 collection 所有权 `owner.json`，每 60 秒刷新 `_heartbeat`；commit 再写带所有权的 `manifest.json` 与 `_committed`。 |
| `MilvusOption.MilvusInsertMaxBatchSize` | Int | 否 | 5000 | 交给原生 writer 的每个 Arrow 批的正行数。 |
| `milvus.writer.variableWidthBytesPerValue` | Double | 否 | 32.0 | 变长列（字符串、JSON、二进制）每个值预留的初始字节数。 |
| `milvus.write.file.rolling.bytes` | Long | 否 | 2147483648 | V2/V3 原生 writer 共用的未压缩字节滚动正阈值，映射为 `writer.file_rolling.size`。它控制列组文件滚动，不是对象存储上传大小，也不保证最终 Parquet 文件的精确大小。 |

写之前在 driver 上校验：DataFrame 的每一列都是 collection 的字段，Spark 类型与读出来的一致（向量列也接受
`BinaryType` 原始字节）；除 Milvus function 输出外每个字段都要给（nullable 字段给一列 null）；collection
不能用 `autoID`，不能有 partition key。`Array<Int8>`、`Array<Int16>` 的元素要在该类型的取值范围内，与 Milvus proxy 对 insert
的检查一致；越界的值让任务失败。只支持 `mode("append")`。`overwrite` 有意不支持：它会让 collection 的全部旧数据不可见，且 Milvus 没有回滚，
所以 Spark 在分析阶段拒绝它，数据不动；全量刷新请在 Milvus 侧 drop 并重建 collection，或用 SDK 全表 delete，再 append。
`errorIfExists`/`ignore` 对这类数据源 Spark 不支持。

### 2.5 离线备份读取参数

读取 milvus-backup 导出的 **binlog 格式**备份，无需任何 Milvus client 连接。已发布版本（v0.5.x）的 `milvus-backup create` 默认即 binlog 格式；`--format binlog` 仅存在于 milvus-backup master 分支（面向 Milvus 3.x，默认 snapshot）。完整设计见 `docs/backup-datasource-design.md`。

| 参数 | 类型 | 必填 | 默认 | 说明 |
|-----------|------|----------|---------|-------------|
| `MilvusOption.BackupDir` | String | 否 | "" | `milvus.backup.dir` — 备份目录，如 `s3a://bucket/backup/<name>`。**仅支持 S3**（`s3://` 自动归一化为 `s3a://`）；本地/`file://` 目录在规划期被拒绝（packed reader 需要 S3）。 |
| `MilvusOption.MilvusDatabaseName` | String | 否 | "" | collection 所在库。传 `"default"` 选择默认库的 collection（匹配 meta 记录为 `""` 或 `"default"`）；留空则走单候选/歧义判定——当同时存在 `default.orders` 与 `db2.orders` 时，需传 `"default"`（或 `"db2"`）消除歧义。 |
| `MilvusOption.MilvusCollectionName` | String | 条件 | - | 备份内的 collection 名（与库名联合匹配，不用 `.head`）。备份含多个 collection 时必须指定。 |
| `MilvusOption.SnapshotPath` | String | 否 | - | `milvus.snapshot.path` — 快照目录里的一个快照 JSON：authority 为桶名的对象存储 URI（`s3a://bucket/files/snapshots/<coll>/metadata/<id>.json`，`s3://`、`gs://`、`oss://` 同理），相对 `fs.bucket_name` 的 key，或 Milvus CreateSnapshot 返回的 `s3_location` 形式 `https://<endpoint>/bucket/files/...`（host 是配置的 endpoint 且 `fs.bucket_name` 是该桶时接受）。位置里的桶与读取绑定的桶不同、存储是本地后端却写了桶、以及其他协议，一律报错。不经 Milvus 服务：schema、分区、段全部来自这个文件。不能与 `milvus.snapshot.manifests` 同时给。 |
| `MilvusOption.ClientSnapshotName` | String | 否 | 最新 | `milvus.client.snapshot.name` — 只作用于配有 `milvus.uri` 的 `format("milvus")` 读取：按名字取快照，而不是最新快照。Catalog 忽略此 option，按名字读取请用 `VERSION AS OF`。读取不会自动建快照；先用 Milvus 或 `CALL milvus.system.create_snapshot(...)` 建。 |
| `MilvusOption.SnapshotMaxJsonBytes` | Long | 否 | 67108864 | `milvus.snapshot.max.json.bytes` — 快照 JSON 或 backup `full_meta.json` 的正整数大小上限。 |

读取 schema 从备份 meta 推导，也可用 `.schema()` 指定；meta 读不到时两种情况都直接失败。读取动态集合（`enable_dynamic_field=true`）要求备份 meta 记录 `$meta` 字段——仅当 milvus-backup 带 etcd 访问（`--backup_index_extra`）且 **≥ v0.5.13** 时才捕获。跨多个 binlog 文件的 column group 已支持（milvus-storage#657 已修复每文件行范围编码）。含 struct-array 字段（`struct_array_fields`）的集合仍会在规划期中止读取。S3 凭证复用现有 `fs.*` 选项（`fs.address`、`fs.access_key_id`、`fs.access_key_value` ...）；桶取自 `milvus.backup.dir` URI。

S3 兼容存储以 `fs.address` 为规范端点选项；DataFrame option
`fs.s3a.endpoint` 和 `s3.endpoint` 依此为别名。已有 Spark/Hadoop 配置中的
`fs.s3a.*`、`fs.oss.*` 也会被翻译，桶级优先：端点、region、角色与静态密钥，
`connection.ssl.enabled`（OSS 为 `connection.secure.enabled`）翻成 `fs.use_ssl`
（没设时与 Hadoop 一样为 true，端点自带 `http://` 或 `https://` 时以端点为准），
以及显式设置过的 `path.style.access`。用哪套凭证由有效的 credential provider 决定，与
Hadoop 一致，按链的顺序：provider 是 `SimpleAWSCredentialsProvider` 的桶用它自己的密钥，不用全局配置的角色；
链的第一项是静态密钥且已设，就用密钥。原生层每个桶只接受一种身份，照 Hadoop 的方式表达不了的链直接报错，要求改用 `fs.*` 选项：
角色与其他来源混用；环境类来源排在已设的静态密钥之前；AssumeRole 由静态密钥签名（`fs.s3a.assumed.role.credentials.provider`
含 `SimpleAWSCredentialsProvider`，这是它的默认值）；连接器不认识的 provider 类；`fs.oss.credentials.provider` 写成列表。
两级都没配 provider 时，角色和静态密钥同时出现也报错。选项里给了静态密钥（`fs.access_key_id` 加 `fs.access_key_value`）或
`fs.role_arn` 时，身份由选项决定，不再判断 Hadoop 的链；`fs.use_iam=true` 只在会话链只有 AssumedRole 时保留它，其他链换成默认链。
读 S3 桶时会话里的 OSS 键、读 OSS 桶时的 S3A 键，没有端点、角色或密钥就跳过。与 driver 的 `AWS_*` 环境变量相同的静态密钥
可以为 AssumeRole 签名，因为原生默认链读的就是这组变量。
显式的 `fs.*` 选项总是优先于翻译结果。
临时凭证（密钥加 `fs.s3a.session.token` 或 `fs.oss.securityToken`）翻不过去，原生存储层没有
session token 属性。三个值正好是 driver 进程的 `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、
`AWS_SESSION_TOKEN` 时（Spark 会把这三个环境变量抄进这几个键），密钥不下传，原生默认链读同一组环境变量；
其他临时凭证直接报错，改设 `fs.use_iam=true`，或用 `fs.access_key_id`、`fs.access_key_value` 给长期密钥。
翻译得到的端点也是识别 Milvus 产生的 `https://<endpoint>/<bucket>/<key>` 位置时用的端点，只由 Hadoop 配置给出的端点同样认得。
Milvus 产生的元数据可能把对象写成
`s3://endpoint:port/bucket/key`。连接器只在解析 Milvus 元数据时识别这种形式：authority
显式带端口，或 authority host 与已配置 endpoint host 精确匹配时，才把后续第一段作为桶；最终与
`s3a://bucket/key` 归一成同一 `(bucket, key)`。用户直接传入的标准 S3 URI 始终把
authority 当作桶。路径样式访问依次读取 `fs.s3a.path.style.access`、
`s3.pathStyleAccess`、`fs.use_virtual_host` 的反值，三者都按严格布尔值解析。
快照 JSON、Avro、V2 footer、V3 Manifest、数据文件与删除文件必须全部归一到
快照的同一个桶；跨桶引用会在规划期失败。

## 3. 使用示例

### 3.1 读取数据

```scala
val df = spark.read
  .format("milvus")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusToken, "your-token")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option(MilvusOption.MilvusDatabaseName, "your_database")
  .option(MilvusOption.MilvusPartitions, "100,101")
  .option(MilvusOption.MilvusSegments, "2001,2002")
  .option(MilvusOption.ReaderFieldIDs, "100,101")  // 只读取指定 collection 字段
  .option(MilvusOption.MilvusExtraColumns, "_segment_id,_row_offset,_timestamp")
  .load()
```

### 3.2 写入数据

```scala
df.write
  .format("milvus")
  .mode("append")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusToken, "your-token")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option("fs.root_path", "files")           // 再加 fs.* 存储选项
  .save()
```

段落在 `files/staging/<job-id>/` 下并带作业清单；登记之后才进入 collection（后续步骤，见
`docs/design/capabilities.md` 的 A4）。已提交过的作业重跑不会再写。

### 3.3 把 backfill 登记给 Milvus

backfill 作业（`MilvusBackfill.run`）给每个已有段写一个新列组，并在
`{stagingRoot}/staging/{jobId}/` 下提交作业清单（`BackfillConfig.stagingRoot`、`jobId`；前缀由
`BackfillResult.stagingPrefix` 带回）。把新的 manifest 版本交给 Milvus 是单独一步：

```scala
import com.zilliz.spark.connector.procedure.Register

Register.run(
  Map(
    MilvusOption.MilvusUri -> "http://localhost:19530",
    MilvusOption.MilvusToken -> "your-token",
    MilvusOption.MilvusCollectionName -> "your_collection",
    "fs.bucket_name" -> "milvus-bucket"          // 再加其余 fs.* 存储选项
  ),
  stagingPrefix = result.stagingPrefix
)
```

它读作业清单，用每个段的 id 和新 manifest 版本调 Milvus 的 `BatchUpdateManifest`，然后给作业打上已登记标记，
第二次调用不再发送。只有写进已有段的作业能这样登记；新建段的作业（`df.write`）会被拒绝，要等 Milvus 的
`RegisterSegments`。

同一个动作的 SQL 形式。先给会话开连接器的 SQL 扩展：

```
--conf spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusSparkSessionExtensions
```

然后：

```sql
CALL milvus.system.register('your_db.your_collection',
  staging          => 'files/staging/backfill-1789478390101',
  `milvus.uri`     => 'http://localhost:19530',
  `milvus.token`   => 'your-token',
  `fs.bucket_name` => 'milvus-bucket',
  `fs.address`     => 's3.us-west-2.amazonaws.com',
  `fs.use_iam`     => 'true')
```

第一个参数是 collection：可以写 `'db.coll'`；只写 `'coll'` 时使用 `milvus.database.name`，未提供则使用 `default`。`staging` 是作业暂存前缀，相对桶的 key。
其余参数都是连接或存储选项，键和 DataFrame 读时 `.option()` 的键一样，因为带点所以用反引号包住，值也一样。
值只能是常量。结果是一张表，每段一行：`job_id`、`segment_id`、`manifest_version`、`status`（`registered`，
作业此前已登记过则是 `already_registered`）。过程名不存在、缺参数、多参数、类型不对，都在解析时拒绝并列出参数表。
不以 `CALL milvus.` 开头的语句不受影响，扩展可以常开。Spark 3.5 和 4.x 行为一样。

建向量索引也走同一个前端：

```sql
CALL milvus.system.build_index('your_db.your_collection',
  field            => 'embedding',
  output           => 'files/built-index',
  index_type       => 'HNSW',
  metric           => 'COSINE',
  params           => 'M=16,efConstruction=200',
  `milvus.snapshot.path` => 'https://.../metadata/4691.json',
  `fs.bucket_name` => 'milvus-bucket',
  `fs.address`     => 's3.us-west-2.amazonaws.com',
  `fs.use_iam`     => 'true')
```

它按 option 选中的固定快照规划，每段一个 Spark 任务读回向量列建索引，按 Milvus 的命名把索引对象写到
`output` 前缀下，并在 `output/staging/<job>/manifest.json` 记录每段的索引。`index_type` 默认 `HNSW`、
`metric` 默认 `COSINE`，`params` 是 `name=value` 列表；`build_id`、`index_version`、`store_path_version`
可选，默认分别是当前毫秒、1、0。结果每段一行：`segment_id`、`partition_id`、`row_count`、`objects`、
`bytes`、`build_id`、`job_id`。

再调一次写出描述这批索引的快照：

```sql
CALL milvus.system.write_snapshot('your_db.your_collection',
  job              => 'index-1789478390101',
  input            => 'files/built-index',
  `milvus.snapshot.path` => 'https://.../metadata/4691.json',
  `fs.bucket_name` => 'milvus-bucket',
  `fs.address`     => 's3.us-west-2.amazonaws.com',
  `fs.use_iam`     => 'true')
```

段来自 option 选中的快照，也就是 `build_index` 规划的那一份；索引记录来自 `input` 下那个作业的清单。
它在 `output/snapshots/<collection>/` 下写出每段一个 Avro 清单和一份快照 JSON，`output` 不给时用 `input`；
`snapshot_id` 默认当前毫秒，`snapshot_name` 默认 `<collection>-<snapshot_id>`。结果一行：`snapshot`（快照 JSON 的 key）、
`snapshot_id`、`snapshot_name`、`segments`、`indexes`、`bytes`。这份快照可以用 `milvus.snapshot.path` 直接被本连接器读回；
把它恢复成 Milvus 的 collection 尚未实现。段必须是 storage version 3 且带行数，V2 段直接报错，不按猜测写出。

### 3.4 用 `CALL` 管理 Milvus

管理过程沿用上面的 SQL 扩展和参数规则。会调用 Milvus 的过程要显式提供 `milvus.uri` 和所需认证选项；
`cleanup_staging` 只打开语句给出的 `fs.*` 存储，不连接 Milvus。
目标可以写成 `db.collection`；只写 `collection` 时先使用 `milvus.database.name`，
未提供该选项才使用 `default`。

| 过程 | 必填参数 | 可选参数 | 结果 |
|------|----------|----------|------|
| `create_snapshot` | `collection`、`name` | `description`；`compaction_protection_seconds`（默认 `0`） | 一行 `database`、`collection`、`snapshot`、`description`、`partition_names`、`create_ts`、`s3_location` |
| `drop_snapshot` | `collection`、`name` | — | 一行，`status = dropped` |
| `list_snapshots` | `collection` | — | 每个快照名称一行；没有快照时返回空表 |
| `describe_snapshot` | `collection`、`name` | — | 与 `create_snapshot` 相同的快照元数据列 |
| `create_index` | `collection`、`field`、`index_name` | `index_type`（默认 `AUTOINDEX`）、`metric_type`（默认 `L2`）、`params`、`wait`、`timeout_seconds` | 一行字段、索引名和状态；不等待时为 `submitted`，等待成功时为 `Finished` |
| `drop_index` | `collection`、`index_name` | — | 一行，`status = dropped` |
| `load` | `collection` | `wait`、`timeout_seconds` | 一行；不等待时 `state = submitted`，等待成功时为 `LoadStateLoaded` |
| `release` | `collection` | — | 一行，`status = released` |
| `flush` | `collection` | — | 一行，`status = submitted`；只表示 Milvus 已接受请求，不表示持久化已经完成 |
| `compact` | `collection` | `wait`、`timeout_seconds` | compaction ID、计划数、状态及各计划状态计数；只提交不等待时，各状态计数为 NULL |
| `describe` | `collection` | — | collection ID、持久段数量、加载状态，以及每个 schema 字段与索引组合一行；字段没有索引时索引列为 NULL |
| `cleanup_staging` | `collection` | `retention_seconds`（默认 `604800`，最小 `300`）、`dry_run`（默认 `true`） | `{fs.root_path}/staging` 每个子目录一行：所有权、写模式、动作与原因、最后心跳、候选/已删文件数、残留目录数和 `prefix_deleted` |

`create_index`、`load`、`compact` 默认只提交任务并立即返回。设置 `wait => true` 才轮询完成状态；
等待时 `timeout_seconds` 默认 600，且必须是正数。未设置 `wait => true` 却提供 timeout 会报错。
每次轮询 RPC 都以当时剩余的整体等待时间为 deadline，不允许单次状态请求越过设定的超时时间。
Milvus 返回失败状态或等待超时时，整条语句失败，不返回看似成功的结果行。

`register` 仍只用于已经提交、且只更新已有段 manifest 的 backfill 作业，不能登记 `df.write`
新建的段。

`cleanup_staging` 只选择版本化 owner 与目标 collection 精确一致、模式明确为 `append`、没有
`_registered`，且心跳与前缀内每个文件修改时间都早于保留期截止点的作业。存在 manifest 或 commit
标记时还会校验其一致性，删除前把完整状态再读一次。缺失、旧格式、损坏、发生变化、属于其它
collection、仍活跃、已经登记的作业，以及所有 backfill 作业，都逐项返回 `preserved`；一个坏作业
不会阻止或授权删除其它作业。例如：

```sql
CALL milvus.system.cleanup_staging('your_db.your_collection',
  retention_seconds => 604800,
  dry_run            => true,
  `fs.bucket_name`   => 'milvus-bucket',
  `fs.address`       => 's3.us-west-2.amazonaws.com',
  `fs.use_iam`       => 'true')
```

先审查 dry-run 的结果，再显式设置 `dry_run => false`。当前固定的原生文件系统只暴露文件删除，没有
递归目录删除；实际执行会删除合格的文件对象，但会报告对象存储目录标记或本地空目录仍在，
`prefix_deleted` 在 milvus-storage 补出对应 API 前始终为 `false`。


## 4. 数据模式

### 4.1 输出模式

`milvus` 格式的输出模式取决于 Milvus 集合的 schema，包含：

- Spark 投影与 `fieldIDs` 共同选出的 collection 字段。每个字段都保留快照中的名称、字段 ID、
  Milvus 类型、nullable、键标记与向量维度，并写入 Spark metadata。
- collection schema 明确记录 `$meta` 的真实字段 ID、JSON 类型和动态字段标记时输出该列。
  如果已开启动态字段但这份定义缺失或不一致，规划会直接失败；连接器不会猜测物理字段 ID。
- 按需请求的元数据列，无论请求顺序如何，都按以下固定顺序追加：
  - `_segment_id`（`LongType`，不可空）：数据段 ID。
  - `_row_offset`（`LongType`，不可空）：删除过滤前，该行在段内的物理位置。
  - `_timestamp`（`LongType`，nullability 取自快照）：字段 ID 为 `1` 的 Milvus 存储系统时间戳，
    从数据文件读取，不是运行时合成值。

连接器不提供 `partition` 元数据列。分区与段的选择分别使用 `milvus.partitions` 和
`milvus.segments`。

## 5. 注意事项

1. **版本要求**：此连接器需要 Milvus 2.6+ 和 Storage V2
2. **SSL/TLS 配置**：支持单向和双向 TLS 认证，根据需要配置相应的证书文件
5. **参数常量**：建议使用 `MilvusOption` 类中定义的常量，避免字符串拼写错误

## 6. 支持的数据类型

### 6.1 标量类型
- Bool（`BooleanType`）
- Int8、Int16、Int32、Int64（`ByteType`、`ShortType`、`IntegerType`、`LongType`）
- Float、Double（`FloatType`、`DoubleType`）
- String、VarChar、Text、JSON（`StringType`）

### 6.2 向量类型
- FloatVector
- Float16Vector
- BFloat16Vector
- BinaryVector
- Int8Vector
- SparseFloatVector

### 6.3 复合类型
- Array，元素可为 Bool、Int8、Int16、Int32、Int64、Float、Double、String 或 VarChar

以上是封闭的支持列表。Geometry、Timestamptz、ArrayOfVector/struct-array 与未来
未知 Milvus 类型不会静默降级为二进制或 null；schema 解析或值转换会直接失败，并报告不支持的类型。

## 原生依赖资源包

构建时使用 `-Dmilvus.native.bundle=/absolute/path/to/platform.jar`，将两个 JNI
及统一的动态依赖打入 Connector JAR。运行时每个类加载器只校验、解压一次。
此构建选项与 `knowhere.native.jar` 互斥；指向其他库的显式
`knowhere.native.path` 会被拒绝。参见[构建与验收](contributing.md#unified-native-bundle)。
