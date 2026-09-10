package com.zilliz.milvus.storage

/** 字段 id、名字、Milvus 类型、Arrow 类型的唯一映射；不含 Spark 类型。
  *
  * 主要类型：SchemaMapper、MilvusType、ArrowTypes。
  * 承载的功能：R15、C3（见 docs/design/capabilities.md）。
  */
package object schema
