package com.zilliz.milvus.storage.read

/** 批读取、行号取列、出口；碰 native。
  *
  * 主要类型：SegmentReader、SegmentReaderRegistry、ColumnBatch、Take。
  * 承载的功能：R3、R4、R14、R17、G3（见 docs/design/capabilities.md）。
  */
package object exec
