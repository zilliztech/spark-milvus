package com.zilliz.spark.connector.search

import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReaderFactory
}
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.snapshot.SegmentIndexes
import com.zilliz.spark.connector.read.{MilvusInputPartition, SegmentSearchScan}
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class SegmentSearchScanTest extends AnyFunSuite with Matchers {
  test(
    "external scans describe vector fields and indexes without planning or a public snapshot"
  ) {
    val field = FieldSchema(
      fieldID = 101L,
      name = "vector",
      dataType = DataType.FloatVector
    )
    val scan = new SegmentSearchScan {
      override def searchSchema = CollectionSchema(fields = Seq(field))
      override def searchOptions = Map.empty[String, String]
      override def searchPredicate = None
      override def searchSegments = Seq(30L -> SegmentIndexes.Unindexed)
      override def searchPartitions(): Seq[MilvusInputPartition] =
        throw new AssertionError("describing an input must not plan partitions")
      override def readSchema(): StructType = StructType(Seq.empty)
      override def toBatch = this
      override def planInputPartitions(): Array[InputPartition] =
        throw new AssertionError("describing an input must not plan partitions")
      override def createReaderFactory(): PartitionReaderFactory =
        throw new AssertionError("describing an input must not open readers")
    }
    NearestByInput.vectorField(scan, "vector") shouldBe Right(field)
    NearestByInput.vectorField(scan, "unknown").isLeft shouldBe true
    NearestByInput.describeSegments(
      scan,
      "vector",
      MetricType.L2,
      approx = false
    ) shouldBe
      "1 segments scanned exactly"
    NearestByInput.describeSegments(
      scan,
      "vector",
      MetricType.L2,
      approx = true
    ) should include(
      "0 of 1 segments by index, 1 scanned exactly"
    )
  }
}
