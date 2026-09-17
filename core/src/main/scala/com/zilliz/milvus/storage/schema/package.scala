package com.zilliz.milvus.storage

/** The single mapping between field id, field name, Milvus type and Arrow type.
  * Carries no Spark types.
  *
  * Planned for R20: the legality rules for external source types, copied from
  * Milvus's `NormalizeExternalArrow`, and the description of each vector layout
  * (element type, dimension, data buffer). `spark.types` and `core.index` both
  * read it; neither converts through a Milvus-internal copy.
  *
  * Main types: SchemaMapper, MilvusTypes, ArrowTypes. Capabilities: R15, C3
  * (see docs/design/capabilities.md).
  */
package object schema
