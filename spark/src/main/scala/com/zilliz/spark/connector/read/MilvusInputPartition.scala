package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition

import com.zilliz.milvus.storage.expr.FilterExpr
import com.zilliz.milvus.storage.read.plan.SegmentReadTask

/** One segment read and its parsed Milvus filter. The task's layout is the
  * single description of whether the segment uses a manifest or column groups.
  */
final case class MilvusInputPartition(
    task: SegmentReadTask,
    milvusFilter: Option[FilterExpr] = None
) extends InputPartition
