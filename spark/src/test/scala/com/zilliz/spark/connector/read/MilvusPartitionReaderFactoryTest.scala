package com.zilliz.spark.connector.read

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.expr.{FieldRef, IsNotNull}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.{
  SegmentLayout,
  Snapshot,
  SnapshotOrigin
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class MilvusPartitionReaderFactoryTest extends AnyFunSuite {
  test(
    "the scan serializes a reader factory without service or storage identities"
  ) {
    val input = Map(
      MilvusOption.ReadVectorRaw -> "true",
      "milvus.token" -> "driver-only-test-token",
      "milvus.client.key" -> "driver-only-client-key",
      "milvus.uri" -> "https://driver-only-service",
      "fs.secret_access_key" -> "partition-only-storage-secret"
    )
    val snapshot = Snapshot(
      name = "t",
      collectionId = 1L,
      createdAt = None,
      schema = milvusSchema,
      partitionIds = Seq.empty,
      segments = Seq.empty,
      origin = SnapshotOrigin.Options,
      bucket = ""
    )
    val scan = new MilvusScan(
      schema,
      new CaseInsensitiveStringMap(input.asJava),
      snapshot
    )
    val factory = scan.createReaderFactory()
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(factory)
    finally output.close()
    val encoded = new String(
      bytes.toByteArray,
      StandardCharsets.ISO_8859_1
    )
    Seq(
      "driver-only",
      "partition-only-storage-secret"
    ).foreach { forbidden =>
      assert(!encoded.contains(forbidden))
    }
    val serialized = new ObjectInputStream(
      new ByteArrayInputStream(bytes.toByteArray)
    )
    val restored =
      try serialized.readObject().asInstanceOf[MilvusPartitionReaderFactory]
      finally serialized.close()
    assert(restored.supportColumnarReads(v3()))
  }

  private val milvusSchema = CollectionSchema(
    name = "t",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(fieldID = 1L, name = "Timestamp", dataType = DataType.Int64)
    )
  )

  private val schema = StructType(Seq(StructField("id", LongType)))

  private def task(layout: SegmentLayout) = SegmentReadTask(
    segmentId = 30L,
    partitionId = 20L,
    layout = layout,
    schemaBytes = milvusSchema.toByteArray,
    properties = Map("fs.storage_type" -> "local")
  )

  private def v3() =
    MilvusInputPartition(task(SegmentLayout.Manifest("files/seg")))

  test("snapshot partitions support only columnar reads") {
    val factory = new MilvusPartitionReaderFactory(schema, rawVectors = false)
    assert(factory.supportColumnarReads(v3()))
    val failure = intercept[UnsupportedOperationException] {
      factory.createReader(v3())
    }
    assert(failure.getMessage.contains("columnar"))
  }

  test("a pushed-down predicate keeps V3 reads columnar") {
    val withPredicate = new MilvusPartitionReaderFactory(
      schema,
      rawVectors = false,
      pushedExpression = Some(IsNotNull(FieldRef(100L, DataType.Int64)))
    )
    assert(withPredicate.supportColumnarReads(v3()))
  }

  test("a reader that fails to open releases what it already took") {
    val allocator = ArrowAllocator.forReadTask(7L, 1024L)
    val failure = new IllegalStateException("open failed")

    MilvusPartitionReaderFactory.closeAfterFailure(allocator, failure)

    assert(allocator.isClosed)
    assert(failure.getSuppressed.isEmpty)
  }

  test("a resource that fails to close reports it on the original failure") {
    val failure = new IllegalStateException("open failed")
    val stubborn = new AutoCloseable {
      override def close(): Unit = throw new IllegalStateException("close")
    }

    MilvusPartitionReaderFactory.closeAfterFailure(stubborn, failure)
    MilvusPartitionReaderFactory.closeAfterFailure(null, failure)

    assert(failure.getSuppressed.map(_.getMessage).toSeq == Seq("close"))
  }
}
