package com.zilliz.milvus.storage

/** Byte-level encoding and decoding of Milvus column values. Pure JVM, shared
  * by the read and write paths.
  *
  * Float16 and BFloat16 packing lives here, as does the (index, value) encoding
  * of sparse vectors and the protobuf `ScalarField` an Array value is stored
  * as. The split against the schema package: schema answers "what type is this
  * field", codec answers "what does one value of that type look like in bytes".
  *
  * BinlogCodec owns the common event envelope and flat Parquet payload used by
  * deletion files and persisted index files. It opens no storage itself.
  * Encoding index files for W6 is planned here, beside the decoder, so both
  * directions share one format definition.
  *
  * Main types: FloatConverter, SparseFloatVectorConverter, ArrayCodec,
  * BinlogCodec. Capabilities: R15, W3, V2 (see docs/design/capabilities.md).
  */
package object codec
