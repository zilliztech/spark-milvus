package com.zilliz.milvus.client

/** How a call leaves on gRPC: `RpcRetry`, which sends a read again after a
  * transient failure within one overall deadline.
  *
  * The read RPCs behind C1, C2 and A1 through A5 are retried here; writes are
  * sent once. capabilities.md records those rows against client.api (see
  * docs/design/capabilities.md).
  */
package object grpc
