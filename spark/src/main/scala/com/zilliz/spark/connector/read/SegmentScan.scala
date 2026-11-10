package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.{Batch, Scan}

import com.zilliz.milvus.storage.expr.PredicateExpr
import com.zilliz.milvus.storage.snapshot.SegmentIndexes
import io.milvus.grpc.schema.CollectionSchema

/** A fixed set of physical segments exposed by a batch scan. Input formats
  * resolve their own schema and metadata. Describing segments opens no files;
  * partition planning retains predicates and runtime pruning. Consumers use
  * these immutable descriptions with the public segment reader.
  */
trait SegmentScan extends Scan with Batch {
  def collectionSchema: CollectionSchema
  def segmentOptions: Map[String, String]
  def segmentPredicate: Option[PredicateExpr]
  def segments: Seq[(Long, SegmentIndexes)]
  def segmentPartitions(): Seq[MilvusInputPartition]
}
