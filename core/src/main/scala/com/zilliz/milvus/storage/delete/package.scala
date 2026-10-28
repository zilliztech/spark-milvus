package com.zilliz.milvus.storage

/** Decodes delete files into a bitset indexed by row number.
  * `DeltaLogReader.decodeParquet` accepts bare primary-key/timestamp Parquet
  * bytes and rejects binlog envelopes. Private compatibility readers unwrap
  * historical containers before sharing this decoder and `DeletePlan`.
  *
  * Main types: DeletePlan, DeltaLogReader; DeleteBitset and DeltaLogDecoder are
  * the designed names they grow into.
  */
package object delete
