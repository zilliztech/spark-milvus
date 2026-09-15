package com.zilliz.milvus.storage

/** Decodes delete files into a bitset indexed by row number.
  *
  * Main types: DeletePlan, DeltaLogReader; DeleteBitset and DeltaLogDecoder
  * are the designed names they grow into. Capabilities: R8 (see
  * docs/design/capabilities.md).
  */
package object delete
