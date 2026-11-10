package com.zilliz.milvus.storage.read

/** Physical segment batch reading through upstream milvus-storage.
  * SegmentReader owns native readers and transfers owned Arrow batches to
  * callers, including selected physical rows through take. PhysicalColumnGroup
  * describes resolved physical input. DeletePlans resolves segment deletion
  * descriptions and ReadMetrics records read costs. This package contains no
  * vector search computation or persisted-index execution.
  */
package object exec
