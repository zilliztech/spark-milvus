package com.zilliz.milvus.storage

/** Milvus field-value encoding and decoding. FloatConverter handles Float16 and
  * BFloat16 values; SparseFloatVectorConverter handles sparse index/value
  * pairs; ArrayCodec handles protobuf array values, while ArrayEncoder and
  * SparseFloatVectorEncoder encode values for storage writers. BinlogCodec and
  * BinlogFormat decode the common event envelope and Parquet payload used by
  * delete files and downstream format consumers.
  */
package object codec
