# Milvus Spark Connector 参数参考文档

本文档详细说明了 Milvus Spark Connector 的所有参数配置。

## 版本兼容性

**此连接器需要 Milvus 2.6 或更高版本**（Storage V2）。

如需支持 Milvus 2.5 及更早版本，请使用 `legacy` 分支（不再积极维护）。

## 概述

Milvus Spark Connector 提供了 **`milvus`** 数据源格式，用于 Milvus 数据的读写操作。

此外，还提供了一个便捷的 `MilvusDataReader` 工具类，用于简化集合数据的读取操作。

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
      MilvusOption.MilvusPartitionName -> "your_partition"
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
- `MilvusOption.MilvusPartitionName` - 分区名称

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

每个 Spark 分区是一个段。executor 经 milvus-storage 的 C 接口打开它、拉取 Arrow 批：`storage_version = 3` 的段从段清单打开，`storage_version = 2` 的段从列组 parquet 文件打开，两条线用同一个 reader。executor 把该段的删除文件读成删除计划，按主键和时间戳跳过已删行。

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
| `MilvusOption.MilvusPartitionName` | String | 否 | "" | 分区名称，为空时操作所有分区 |
| `MilvusOption.MilvusCollectionID` | String | 否 | "" | 集合 ID，通常自动获取 |
| `MilvusOption.MilvusPartitionID` | String | 否 | "" | 分区 ID，通常自动获取 |
| `MilvusOption.MilvusSegmentID` | String | 否 | "" | 段 ID，用于精确读取特定段 |
| `MilvusOption.ReaderFieldIDs` | String | 否 | "" | 字段ID列表，逗号分隔，用于只读取部分字段，可以有效减少数据获取时间 |
| `milvus.read.vector.raw` | Boolean | 否 | false | 向量列的输出类型。默认 false，向量转成 Spark 原生类型（`FloatVector`/`Float16Vector`/`BFloat16Vector` → `ArrayType(FloatType)`，`Int8Vector` → `ArrayType(ShortType)`，`SparseFloatVector` → `MapType(LongType, FloatType)`）。设为 true 时向量列输出 `BinaryType`，字节按存储原样给出，由调用方自己按 `dim` 与元素类型解析；这条路径不做逐元素转换，适合把字节直接交给下游原生库的批量作业 |
| `milvus.read.columnar` | Boolean | 否 | true | 读出口形态。默认 true，整批交付（`ColumnarBatch`），直接包住原生 buffer 不拷贝，向量列按 `milvus.read.vector.raw` 决定的类型呈现；有删除的批按存活行下标映射交付，同样不拷贝。设为 false 逐行交给 Spark。带 `vector.search.*` 的读一律走行式，因为那一步要逐行算距离 |


### 2.4 写入参数

`df.write.format("milvus").mode("append")` 把 DataFrame 直接写成 Milvus 段到对象存储，不经 Milvus
服务，也不做登记。表的解析和读一样，collection schema 来自三者之一：`milvus.uri` 加 collection
名（取该 collection 最新的快照）、`milvus.snapshot.path`、或 `milvus.snapshot.schema.bytes`（只给
schema，没有任何快照时用）。字段 id 和向量维度都从这份 schema 取。

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `fs.root_path` | String | 否 | `files` | 作业写到 `{root}/staging/{job-id}/` 下：每个 Spark 分区一个段目录，然后是 `manifest.json`（每个段的路径、manifest 版本、行数）和标记文件 `_committed`。 |
| `MilvusOption.MilvusInsertMaxBatchSize` | Int | 否 | 1000 | 交给原生 writer 的每个 Arrow 批的行数。 |
| `milvus.writer.variableWidthBytesPerValue` | Double | 否 | 32.0 | 变长列（字符串、JSON、二进制）每个值预留的初始字节数。 |

写之前在 driver 上校验：DataFrame 的每一列都是 collection 的字段，Spark 类型与读出来的一致（向量列也接受
`BinaryType` 原始字节）；除 Milvus function 输出外每个字段都要给（nullable 字段给一列 null）；collection
不能用 `autoID`，不能有 partition key。只支持 `mode("append")`。`overwrite` 有意不支持：它会让 collection 的全部旧数据不可见，且 Milvus 没有回滚，
所以 Spark 在分析阶段拒绝它，数据不动；全量刷新请在 Milvus 侧 drop 并重建 collection，或用 SDK 全表 delete，再 append。
`errorIfExists`/`ignore` 对这类数据源 Spark 不支持。

### 2.5 离线备份读取参数

读取 milvus-backup 导出的 **binlog 格式**备份，无需任何 Milvus client 连接。已发布版本（v0.5.x）的 `milvus-backup create` 默认即 binlog 格式；`--format binlog` 仅存在于 milvus-backup master 分支（面向 Milvus 3.x，默认 snapshot）。完整设计见 `docs/backup-datasource-design.md`。

| 参数 | 类型 | 必填 | 默认 | 说明 |
|-----------|------|----------|---------|-------------|
| `MilvusOption.BackupDir` | String | 否 | "" | `milvus.backup.dir` — 备份目录，如 `s3a://bucket/backup/<name>`。**仅支持 S3**（`s3://` 自动归一化为 `s3a://`）；本地/`file://` 目录在规划期被拒绝（packed reader 需要 S3）。 |
| `MilvusOption.MilvusDatabaseName` | String | 否 | "" | collection 所在库。传 `"default"` 选择默认库的 collection（匹配 meta 记录为 `""` 或 `"default"`）；留空则走单候选/歧义判定——当同时存在 `default.orders` 与 `db2.orders` 时，需传 `"default"`（或 `"db2"`）消除歧义。 |
| `MilvusOption.MilvusCollectionName` | String | 条件 | - | 备份内的 collection 名（与库名联合匹配，不用 `.head`）。备份含多个 collection 时必须指定。 |
| `MilvusOption.SnapshotPath` | String | 否 | - | `milvus.snapshot.path` — 快照目录里的一个快照 JSON（`s3a://bucket/files/snapshots/<coll>/metadata/<id>.json` 或相对 `fs.bucket_name` 的 key）。不经 Milvus 服务：schema、分区、段全部来自这个文件。不能与 `milvus.snapshot.manifests` 同时给。 |
| `MilvusOption.ClientSnapshotName` | String | 否 | 最新 | `milvus.client.snapshot.name` — 配合 `milvus.uri`：读该 collection 快照目录里这个名字的快照，而不是最新的。连接器自己不建快照，先用 Milvus 或 `CALL create_snapshot` 建。 |
| `MilvusOption.SnapshotMaxJsonBytes` | Long | 否 | 67108864 | `milvus.snapshot.max.json.bytes` — backup `full_meta.json` 大小上限。 |

读取 schema 从备份 meta 推导，也可用 `.schema()` 指定；meta 读不到时两种情况都直接失败。读取动态集合（`enable_dynamic_field=true`）要求备份 meta 记录 `$meta` 字段——仅当 milvus-backup 带 etcd 访问（`--backup_index_extra`）且 **≥ v0.5.13** 时才捕获。两种备份形态会在规划期中止读取：跨多个 binlog 文件的 column group（未修复的 milvus-storage bug，见设计文档）与含 struct-array 字段（`struct_array_fields`）的集合。S3 凭证复用现有 `fs.*` 选项（`fs.address`、`fs.access_key_id`、`fs.access_key_value` ...）；桶取自 `milvus.backup.dir` URI。

## 3. 使用示例

### 3.1 读取数据

```scala
val df = spark.read
  .format("milvus")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusToken, "your-token")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option(MilvusOption.MilvusDatabaseName, "your_database")
  .option(MilvusOption.ReaderFieldIDs, "1,2,100,101")  // 只读取指定字段
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
`RegisterSegments`。SQL 形式的 `CALL milvus.system.register(...)` 还没有。


## 4. 数据模式

### 4.1 输出模式

`milvus` 格式的输出模式取决于 Milvus 集合的 schema，包含：

- 用户定义的字段（根据集合 schema）
- `$meta` (StringType) - 动态字段（如果启用）

## 5. 注意事项

1. **版本要求**：此连接器需要 Milvus 2.6+ 和 Storage V2
2. **SSL/TLS 配置**：支持单向和双向 TLS 认证，根据需要配置相应的证书文件
5. **参数常量**：建议使用 `MilvusOption` 类中定义的常量，避免字符串拼写错误

## 6. 支持的数据类型

### 6.1 标量类型
- Bool
- Int8, Int16, Int32, Int64
- Float, Double
- String, VarChar
- JSON

### 6.2 向量类型
- FloatVector
- Float16Vector
- BFloat16Vector
- BinaryVector
- Int8Vector
- SparseFloatVector

### 6.3 复合类型
- Array（支持标量元素类型）
