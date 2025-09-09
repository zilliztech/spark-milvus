# Milvus Spark Connector 参数参考文档

本文档详细说明了 Milvus Spark Connector 中两种数据源格式的所有参数配置。

## 概述

Milvus Spark Connector 提供了两种数据源格式：

1. **`milvus`** - 用于 Milvus 数据的读写操作
2. **`milvusbinlog`** - 用于读取 Milvus collection中的单个field二进制日志文件（只读）

此外，还提供了一个便捷的 `MilvusDataReader` 工具类，用于简化集合数据的读取操作。该工具类内部会自动处理插入和删除日志的合并，返回最终的有效数据。

## 1. `MilvusDataReader` 便捷读取方法

`MilvusDataReader` 提供了一个便捷的方法来读取集合数据，它会自动处理插入日志和删除日志的合并，返回最终的有效数据。

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
  MilvusOption.S3FileSystemTypeName -> "s3.fs",
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
  MilvusOption.S3FileSystemTypeName -> "s3.fs",
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

`MilvusDataReader` 内部执行以下步骤：

1. **读取插入日志**：使用 `milvus` 格式读取集合的插入数据
2. **读取删除日志**：使用 `milvusbinlog` 格式读取删除日志
3. **数据合并**：自动处理删除操作，过滤掉已删除的记录
4. **返回结果**：返回最终的有效数据，去除系统字段（如 `row_id`、`timestamp`）

这种方式特别适用于需要获取集合当前有效数据的场景，无需手动处理插入和删除日志的复杂合并逻辑。

## 2. `milvus` 格式参数

### 2.1 连接参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.MilvusUri` | String | 是 | - | Milvus 服务器连接 URI，格式：`http://host:port` 或 `https://host:port` |
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
| `MilvusOption.MilvusCollectionName` | String | 是 | - | 集合名称 |
| `MilvusOption.MilvusPartitionName` | String | 否 | "" | 分区名称，为空时操作所有分区 |
| `MilvusOption.MilvusCollectionID` | String | 否 | "" | 集合 ID，通常自动获取 |
| `MilvusOption.MilvusPartitionID` | String | 否 | "" | 分区 ID，通常自动获取 |
| `MilvusOption.MilvusSegmentID` | String | 否 | "" | 段 ID，用于精确读取特定段 |
| `MilvusOption.ReaderFieldIDs` | String | No | "" | 字段ID列表，逗号分隔，用于只读取部分字段，可以有效减少数据获取时间 |

### 2.4 写入参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.MilvusInsertMaxBatchSize` | Int | 否 | 5000 | 单次插入的最大批次大小 |
| `MilvusOption.MilvusRetryCount` | Int | 否 | 3 | 操作失败时的重试次数 |
| `MilvusOption.MilvusRetryInterval` | Int | 否 | 1000 | 重试间隔时间（毫秒） |


## 3. `milvusbinlog` 格式参数

### 3.1 基本参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.ReaderType` | String | 是 | - | 读取器类型，支持 "insert" 或 "delete" |
| `MilvusOption.ReaderPath` | String | 条件必需 | - | 二进制日志文件路径，当不使用 S3 时必需 |

### 3.2 Milvus 连接参数（用于元数据获取）

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.MilvusUri` | String | 否 | "" | Milvus 服务器 URI，用于获取集合元数据 |
| `MilvusOption.MilvusToken` | String | 否 | "" | Milvus 认证令牌 |
| `MilvusOption.MilvusDatabaseName` | String | 否 | "" | 数据库名称 |
| `MilvusOption.MilvusCollectionName` | String | 否 | "" | 集合名称 |
| `MilvusOption.MilvusPartitionName` | String | 否 | "" | 分区名称 |
| `MilvusOption.MilvusCollectionID` | String | 否 | "" | 集合 ID |
| `MilvusOption.MilvusPartitionID` | String | 否 | "" | 分区 ID |
| `MilvusOption.MilvusSegmentID` | String | 否 | "" | 段 ID |
| `MilvusOption.MilvusFieldID` | String | 否 | "" | 字段 ID，对于 insert 类型必需 |

### 3.3 S3 存储参数

| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|--------|------|------|--------|------|
| `MilvusOption.S3FileSystemTypeName` | String | 否 | "" | S3 文件系统类型标识符，固定值 "s3.fs" |
| `MilvusOption.S3Endpoint` | String | 否 | "localhost:9000" | S3 服务端点 |
| `MilvusOption.S3BucketName` | String | 否 | "a-bucket" | S3 存储桶名称 |
| `MilvusOption.S3RootPath` | String | 否 | "files" | S3 根路径 |
| `MilvusOption.S3AccessKey` | String | 否 | "minioadmin" | S3 访问密钥 |
| `MilvusOption.S3SecretKey` | String | 否 | "minioadmin" | S3 秘密密钥 |
| `MilvusOption.S3UseSSL` | Boolean | 否 | false | 是否使用 SSL 连接 |
| `MilvusOption.S3PathStyleAccess` | Boolean | 否 | true | 是否使用路径样式访问 |

## 4. 使用示例

### 4.1 `milvus` 格式示例

#### 读取数据
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

#### 写入数据
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

### 4.2 `milvusbinlog` 格式示例

#### 读取插入日志
```scala
val insertDF = spark.read
  .format("milvusbinlog")
  .option(MilvusOption.ReaderType, "insert")
  .option(MilvusOption.ReaderPath, "/path/to/insert/logs")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .option(MilvusOption.MilvusFieldID, "100")  // 插入日志需要指定字段ID
  .load()
```

#### 读取删除日志
```scala
val deleteDF = spark.read
  .format("milvusbinlog")
  .option(MilvusOption.ReaderType, "delete")
  .option(MilvusOption.ReaderPath, "/path/to/delete/logs")
  .option(MilvusOption.MilvusUri, "http://localhost:19530")
  .option(MilvusOption.MilvusCollectionName, "your_collection")
  .load()
```

#### 使用 S3 存储的二进制日志
```scala
val df = spark.read
  .format("milvusbinlog")
  .option(MilvusOption.ReaderType, "insert")
  .option(MilvusOption.S3FileSystemTypeName, "s3.fs")
  .option(MilvusOption.S3Endpoint, "s3.amazonaws.com")
  .option(MilvusOption.S3BucketName, "your-bucket")
  .option(MilvusOption.S3RootPath, "milvus-logs")
  .option(MilvusOption.S3AccessKey, "your-access-key")
  .option(MilvusOption.S3SecretKey, "your-secret-key")
  .option(MilvusOption.S3UseSSL, "true")
  .option(MilvusOption.MilvusCollectionID, "123456")
.option(MilvusOption.MilvusFieldID, "100")
  .load()
```

## 5. 数据模式

### 5.1 `milvus` 格式输出模式

`milvus` 格式的输出模式取决于 Milvus 集合的 schema，包含以下系统字段：

- `row_id` (LongType) - 行 ID
- `timestamp` (LongType) - 时间戳
- 用户定义的字段（根据集合 schema）
- `$meta` (StringType) - 动态字段（如果启用）

### 5.2 `milvusbinlog` 格式输出模式

`milvusbinlog` 格式固定输出以下字段：

- `data` (StringType/LongType) - 数据内容，insert 为字段值，delete 为主键值
- `timestamp` (LongType) - 时间戳
- `data_type` (IntegerType) - 数据类型标识

## 6. 注意事项

1. **路径自动构建**：当提供 `MilvusOption.MilvusCollectionID` 等参数时，系统会自动构建二进制日志路径
2. **字段 ID 要求**：对于 `milvusbinlog` 格式的 insert 类型，必须指定 `MilvusOption.MilvusFieldID`
3. **SSL/TLS 配置**：支持单向和双向 TLS 认证，根据需要配置相应的证书文件
4. **批次大小**：合理设置 `MilvusOption.MilvusInsertMaxBatchSize` 可以优化写入性能
5. **重试机制**：内置重试机制可以提高操作的可靠性
6. **参数常量**：建议使用 `MilvusOption` 类中定义的常量，避免字符串拼写错误

## 7. 支持的数据类型

### 7.1 标量类型
- Bool
- Int8, Int16, Int32, Int64
- Float, Double
- String, VarChar
- JSON

### 7.2 向量类型
- FloatVector
- Float16Vector
- BFloat16Vector
- BinaryVector
- Int8Vector
- SparseFloatVector

### 7.3 复合类型
- Array（支持标量元素类型）
