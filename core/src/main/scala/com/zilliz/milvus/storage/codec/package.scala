package com.zilliz.milvus.storage

/** Byte-level encoding and decoding of Milvus column values. Pure JVM, shared
  * by the read and write paths.
  *
  * Float16 and BFloat16 packing lives here, as does the (index, value) encoding
  * of sparse vectors and the protobuf `ScalarField` an Array value is stored
  * as. The split against the schema package: schema answers "what type is this
  * field", codec answers "what does one value of that type look like in bytes".
  *
  * Main types: FloatConverter, SparseFloatVectorConverter, ArrayCodec.
  * Capabilities: R15, W3 (see docs/design/capabilities.md).
  */
package object codec
