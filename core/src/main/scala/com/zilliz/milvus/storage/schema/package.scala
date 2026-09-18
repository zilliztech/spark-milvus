package com.zilliz.milvus.storage

/** The single mapping between field id, field name, Milvus type and Arrow type.
  * Carries no Spark types.
  *
  * VectorLayout describes a dense vector column as an element type and a
  * dimension, which is what a computation needs to read the data buffer;
  * `spark.types` and `core.index` both read this package, and neither converts
  * through a Milvus-internal copy.
  *
  * Planned for R20: the legality rules for external source types, copied from
  * Milvus's `NormalizeExternalArrow`.
  *
  * Main types: SchemaMapper, MilvusTypes, ArrowTypes, VectorLayout.
  * Capabilities: R15, C3 (see docs/design/capabilities.md).
  */
package object schema
