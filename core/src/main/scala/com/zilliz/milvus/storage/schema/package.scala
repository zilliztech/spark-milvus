package com.zilliz.milvus.storage

/** The single mapping between field id, field name, Milvus type and Arrow type.
  * Carries no Spark types. SchemaMapper owns the canonical system field ids;
  * HybridTimestamp converts epoch time to Milvus HybridTS for both catalog
  * selection and row timestamps.
  *
  * VectorLayout describes a dense vector column as an element type and a
  * dimension, which is what a computation needs to read the data buffer;
  * `spark.types` and `core.vector` both read this package, and neither converts
  * through a Milvus-internal copy. MetricType is the `metric_type` such a
  * column is searched by and indexed for: the five metrics the connector's own
  * search and index build compute, the direction each ranks in, and which of
  * them an element type takes. A name becomes a MetricType where it enters (a
  * call's argument, a persisted index's parameters); RPCs that only hand a
  * metric to the Milvus service keep the name the call gave.
  *
  * Planned for R20: the legality rules for external source types, copied from
  * Milvus's `NormalizeExternalArrow`.
  *
  * Main types: SchemaMapper, MilvusTypes, ArrowTypes, VectorLayout, MetricType,
  * HybridTimestamp. Capabilities: R15, C3 (see design document
  * capabilities.md).
  */
package object schema
