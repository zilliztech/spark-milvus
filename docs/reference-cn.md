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

`MilvusDataReader` 使用 Storage V2 (Loon) FFI 接口读取数据。删除操作由存储层内部处理，无需单独的删除日志处理。

## 2. `milvus` 格式参数

### 2.1 连接参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.MilvusUri` | String | 条件 | - | Milvus 服务器连接 URI，格式：`http://host:port` 或 `https://host:port`。客户端模式必需；snapshot/backup 模式（离线读）不要求。 |
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

### 2.4 写入参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.MilvusInsertMaxBatchSize` | Int | 否 | 5000 | 单次插入的最大批次大小 |
| `MilvusOption.WriterVariableWidthBytesPerValue` | Double | 否 | 32.0 | VARCHAR/JSON/binary 写入列的 Arrow 初始 buffer 密度；宽变长字段可调大。必须是有限正数。 |
| `MilvusOption.MilvusRetryCount` | Int | 否 | 3 | 操作失败时的重试次数 |
| `MilvusOption.MilvusRetryInterval` | Int | 否 | 1000 | 重试间隔时间（毫秒） |

### 2.5 离线备份读取参数

读取 milvus-backup 导出的 **binlog 格式**备份，无需任何 Milvus client 连接。已发布版本（v0.5.x）的 `milvus-backup create` 默认即 binlog 格式；`--format binlog` 仅存在于 milvus-backup master 分支（面向 Milvus 3.x，默认 snapshot）。完整设计见 `docs/backup-datasource-design.md`。

| 参数 | 类型 | 必填 | 默认 | 说明 |
|-----------|------|----------|---------|-------------|
| `MilvusOption.BackupDir` | String | 否 | "" | `milvus.backup.dir` — 备份目录，如 `s3a://bucket/backup/<name>`。**仅支持 S3**（`s3://` 自动归一化为 `s3a://`）；本地/`file://` 目录在规划期被拒绝（packed reader 需要 S3）。 |
| `MilvusOption.MilvusDatabaseName` | String | 否 | "" | collection 所在库。传 `"default"` 选择默认库的 collection（匹配 meta 记录为 `""` 或 `"default"`）；留空则走单候选/歧义判定——当同时存在 `default.orders` 与 `db2.orders` 时，需传 `"default"`（或 `"db2"`）消除歧义。 |
| `MilvusOption.MilvusCollectionName` | String | 条件 | - | 备份内的 collection 名（与库名联合匹配，不用 `.head`）。备份含多个 collection 时必须指定。 |
| `MilvusOption.ReadApplyDeletes` | Boolean | 否 | true | `milvus.read.apply.deletes` — 读取时应用删除日志（L0/L1）。 |
| `MilvusOption.SnapshotMaxJsonBytes` | Long | 否 | 67108864 | `milvus.snapshot.max.json.bytes` — backup `full_meta.json` 大小上限。 |

读取 schema 需通过 `.schema()` 提供，或从备份 meta 推导；未提供 `.schema()` 且 meta 读取失败时读取硬失败。读取动态集合（`enable_dynamic_field=true`）要求备份 meta 记录 `$meta` 字段——仅当 milvus-backup 带 etcd 访问（`--backup_index_extra`）且 **≥ v0.5.13** 时才捕获。两种备份形态会在规划期中止读取：跨多个 binlog 文件的 column group（未修复的 milvus-storage bug，见设计文档）与含 struct-array 字段（`struct_array_fields`）的集合。S3 凭证复用现有 `fs.*` 选项（`fs.address`、`fs.access_key_id`、`fs.access_key_value` ...）；桶取自 `milvus.backup.dir` URI。

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
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusToken, "your-token")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option(MilvusOption.MilvusDatabaseName, "your_database")
  .option(MilvusOption.MilvusInsertMaxBatchSize, "1000")
  .option(MilvusOption.MilvusRetryCount, "5")
  .save()
```

## 4. 数据模式

### 4.1 输出模式

`milvus` 格式的输出模式取决于 Milvus 集合的 schema，包含：

- 用户定义的字段（根据集合 schema）
- `$meta` (StringType) - 动态字段（如果启用）

## 5. 注意事项

1. **版本要求**：此连接器需要 Milvus 2.6+ 和 Storage V2
2. **SSL/TLS 配置**：支持单向和双向 TLS 认证，根据需要配置相应的证书文件
3. **批次大小**：合理设置 `MilvusOption.MilvusInsertMaxBatchSize` 可以优化写入性能
4. **重试机制**：内置重试机制可以提高操作的可靠性
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
