# Milvus Spark Connector 参数参考文档

本文档详细说明了 Milvus Spark Connector 的所有参数配置。

## 版本兼容性

**此连接器需要 Milvus 2.6 或更高版本**（Storage V2）。

如需支持 Milvus 2.5 及更早版本，请使用 `legacy` 分支（不再积极维护）。

## 概述

Milvus Spark Connector 提供了 **`milvus`** 数据源格式，用于 Milvus 数据的读写操作。

此外，还提供了一个便捷的 `MilvusDataReader` 工具类，用于简化集合数据的读取操作。

## Catalog 三段表名与快照时间旅行

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

标识符必须恰好包含一个 database 和一个 collection；这两个名字会覆盖 Catalog 配置里的
`milvus.database.name` 与 `milvus.collection.name`。普通加载取最新快照，`VERSION AS OF`
按快照名精确匹配，`TIMESTAMP AS OF` 取 Milvus HybridTS 边界不晚于 Spark 解析后时刻的最新快照。
时间字面量先按 `spark.sql.session.timeZone` 解析，Catalog 收到 UTC epoch 微秒。返回的 Table 与后续
Scan 始终使用本次解析出的同一个固定快照。对象存储配置需按部署替换；不能使用 IAM 时，以
`fs.access_key_id` 和 `fs.access_key_value` 替代 `fs.use_iam=true`。

时间旅行选择的是快照元数据，不承诺历史数据保留。Connector 不负责保留旧段文件；compaction
或垃圾回收可能使已经选中的旧快照无法读取。

Catalog 表只支持 client 模式，必须配置 `milvus.uri`。离线的 `milvus.snapshot.path` 与
`milvus.backup.dir` 仍通过 `format("milvus")` 读取。当前 Catalog 不支持 namespace/table 列表，
也不支持 CREATE、ALTER、DROP、RENAME。

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
| `MilvusOption.ReadApplyDeletes` (`milvus.read.apply.deletes`) | Boolean | 否 | true | 应用固定快照可见的段内删除、本分区 L0 删除与全 collection L0 删除。设为 `false` 是显式关闭；只要显式提供，除 `true`、`false` 外的值（包括空白值）都会报错。 |
| `milvus.read.vector.raw` | Boolean | 否 | false | 向量列的输出类型。默认 false，向量转成 Spark 原生类型（`FloatVector`/`Float16Vector`/`BFloat16Vector` → `ArrayType(FloatType)`，`Int8Vector` → `ArrayType(ShortType)`，`SparseFloatVector` → `MapType(LongType, FloatType)`）。设为 true 时向量列输出 `BinaryType`，字节按存储原样给出，由调用方自己按 `dim` 与元素类型解析；这条路径不做逐元素转换，适合把字节直接交给下游原生库的批量作业 |
| `milvus.read.columnar` | Boolean | 否 | true | 读出口形态。默认 true，整批交付（`ColumnarBatch`），直接包住原生 buffer 不拷贝，向量列按 `milvus.read.vector.raw` 决定的类型呈现；有删除的批按存活行下标映射交付，同样不拷贝。设为 false 逐行交给 Spark。带 `vector.search.*` 的读一律走行式，因为那一步要逐行算距离。行式和列式 reader 共用同一套预期行数校验。 |

显式提供的选择器列表不接受空白值、空项或非数值。布尔读选项（`milvus.snapshot.mode`、
`milvus.read.apply.deletes`、`milvus.read.vector.raw`、`milvus.read.columnar`）只接受
不区分大小写的 `true` 或 `false`；空白值和拼写错误都会直接报错，不会回退到默认值。
显式提供的 `milvus.snapshot.max.json.bytes` 必须是正整数。只有 `vector.search.query` 与
`vector.search.topK` 同时给出时才启用向量搜索：query 必须是非空 JSON 风格的有限数字数组，
`topK` 必须是正整数；任一显式提供的向量搜索选项都不能是空白值，缺项或格式错误都在规划期失败。


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
| `MilvusOption.SnapshotPath` | String | 否 | - | `milvus.snapshot.path` — 快照目录里的一个快照 JSON：`s3a://bucket/files/snapshots/<coll>/metadata/<id>.json`，相对 `fs.bucket_name` 的 key，或 Milvus CreateSnapshot 返回的 `s3_location` 形式 `https://<endpoint>/bucket/files/...`（host 是配置的 endpoint 时接受）。不经 Milvus 服务：schema、分区、段全部来自这个文件。不能与 `milvus.snapshot.manifests` 同时给。 |
| `MilvusOption.ClientSnapshotName` | String | 否 | 最新 | `milvus.client.snapshot.name` — 只作用于配有 `milvus.uri` 的 `format("milvus")` 读取：按名字取快照，而不是最新快照。Catalog 忽略此 option，按名字读取请用 `VERSION AS OF`。连接器自己不建快照，先用 Milvus 或 `CALL create_snapshot` 建。 |
| `MilvusOption.SnapshotMaxJsonBytes` | Long | 否 | 67108864 | `milvus.snapshot.max.json.bytes` — 快照 JSON 或 backup `full_meta.json` 的正整数大小上限。 |

读取 schema 从备份 meta 推导，也可用 `.schema()` 指定；meta 读不到时两种情况都直接失败。读取动态集合（`enable_dynamic_field=true`）要求备份 meta 记录 `$meta` 字段——仅当 milvus-backup 带 etcd 访问（`--backup_index_extra`）且 **≥ v0.5.13** 时才捕获。跨多个 binlog 文件的 column group 已支持（milvus-storage#657 已修复每文件行范围编码）。含 struct-array 字段（`struct_array_fields`）的集合仍会在规划期中止读取。S3 凭证复用现有 `fs.*` 选项（`fs.address`、`fs.access_key_id`、`fs.access_key_value` ...）；桶取自 `milvus.backup.dir` URI。

S3 兼容存储以 `fs.address` 为规范端点选项；DataFrame option
`fs.s3a.endpoint` 和 `s3.endpoint` 依此为别名。已有 Spark/Hadoop 配置中的
`fs.s3a.endpoint` 也会被翻译为同一个原生属性。
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
`RegisterSegments`。SQL 形式的 `CALL milvus.system.register(...)` 还没有。


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
