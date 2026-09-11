package com.zilliz.milvus.storage.read

/** Partition planning. Pure JVM and serializable, so it can be built on the
  * driver and shipped to executors.
  *
  * Main types: Partitioner, ReadPlan, InputSpec. Capabilities: R3, R5, R10 (see
  * docs/design/capabilities.md).
  */
package object plan
