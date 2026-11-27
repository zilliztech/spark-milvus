package com.zilliz.milvus.storage.read

/** Physical segment batch reading through upstream milvus-storage.
  * SegmentReader owns native readers and transfers owned Arrow batches to
  * callers, including selected physical rows through take. It opens manifests
  * with the same backend-relative key as the store and column writer. A
  * projection that depends on stored columns resolves against the already open
  * manifest before the native reader is created. PhysicalColumnGroup describes
  * resolved physical input. DeletePlans resolves segment deletion descriptions
  * and ReadMetrics records read costs.
  */
package object exec
