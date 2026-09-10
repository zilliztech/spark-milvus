package com.zilliz.milvus.storage

/** 列快照目录，选快照，解析 JSON 和 Avro 成对象；分发 SnapshotSource。
  *
  * 主要类型：SnapshotCatalog、Snapshot、Segment、SnapshotSource、SnapshotSourceRegistry。
  * 承载的功能：R2、R3、R9、R13、R16、C3（见 docs/design/capabilities.md）。
  */
package object snapshot
