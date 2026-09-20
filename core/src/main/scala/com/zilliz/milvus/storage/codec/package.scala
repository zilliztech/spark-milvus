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
  * MilvusIndexFileDecoder takes one index object apart and IndexFileCodec
  * assembles the slices of a persisted index and hands the payload to the
  * vector library; both belong to the Milvus format side rather than to the
  * computation (docs/design/architecture/vector-search.html section 2.5).
  * `SegmentIndexObjects` encodes what a build produced into the same files,
  * beside the decoder, so both directions share one format definition (W6).
  *
  * `VectorIndexFamilies` is the one list of index types this connector reads
  * and writes, with the byte markers each family's persisted stream starts
  * with: the types that can be loaded and the markers that identify them are
  * the same fact, and splitting them is how a family ends up declared but
  * unreadable.
  *
  * Main types: FloatConverter, SparseFloatVectorConverter, ArrayCodec,
  * BinlogCodec, MilvusIndexFileDecoder, IndexFileCodec, SegmentIndexObjects,
  * VectorIndexFamilies.
  * Capabilities: R15, W3, V2, W6 (see docs/design/capabilities.md).
  */
package object codec
