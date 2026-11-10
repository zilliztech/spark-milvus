package com.zilliz.milvus.storage

/** Milvus and Arrow type contracts, field metadata and timestamps.
  * SchemaMapper, MilvusTypes and ArrowTypes preserve scalar, dense-vector and
  * sparse-vector field representations without a computation engine dependency.
  * FieldMetadata identifies fields and their physical layout; HybridTimestamp
  * preserves timestamp semantics.
  */
package object schema
