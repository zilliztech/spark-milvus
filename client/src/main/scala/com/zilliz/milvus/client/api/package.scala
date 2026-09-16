package com.zilliz.milvus.client

/** MilvusClient: database and collection discovery, DDL, describe, delete,
  * snapshots, indexes, load, release, flush, compact, BatchUpdateManifest, and
  * RegisterSegments once Milvus offers it. Also converts between the protobuf
  * DataType and the core type model. `delete` stays for tests and jobs that
  * prepare data; the connector's SQL surface does not call it (section 10 of
  * capabilities.md).
  *
  * Capabilities: C1, C2, A1 (see docs/design/capabilities.md).
  */
package object api
