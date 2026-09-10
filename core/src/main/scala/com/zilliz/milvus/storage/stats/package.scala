package com.zilliz.milvus.storage

/** 段统计和 row group 统计的读取与剪枝。
  *
  * 主要类型：SegmentStats、Pruner。
  * 承载的功能：R9、R10、R18（见 docs/design/capabilities.md）。
  */
package object stats
