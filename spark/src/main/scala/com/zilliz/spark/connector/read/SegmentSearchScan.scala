package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.{Batch, Scan}

import com.zilliz.milvus.storage.expr.PredicateExpr
import com.zilliz.milvus.storage.snapshot.SegmentIndexes
import io.milvus.grpc.schema.CollectionSchema

/** A scan whose fixed segments can be searched and addressed by physical row.
  * Input formats resolve their own metadata before implementing this contract.
  * The connector shares exact/indexed execution and take without resolving the
  * input's snapshot or interpreting its format. Describing segments opens no
  * files; planning partitions retains the scan's predicates and runtime
  * pruning.
  */
trait SegmentSearchScan extends Scan with Batch {
  def searchSchema: CollectionSchema
  def searchOptions: Map[String, String]
  def searchPredicate: Option[PredicateExpr]
  def searchSegments: Seq[(Long, SegmentIndexes)]
  def searchPartitions(): Seq[MilvusInputPartition]
}
