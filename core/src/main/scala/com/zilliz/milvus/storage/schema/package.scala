package com.zilliz.milvus.storage

/** The single mapping between field id, field name, Milvus type and Arrow type.
  * Carries no Spark types.
  *
  * Main types: SchemaMapper, MilvusTypes, ArrowTypes. Capabilities: R15, C3
  * (see docs/design/capabilities.md).
  */
package object schema
