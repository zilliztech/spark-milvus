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

import com.zilliz.milvus.storage.expr.{FieldRef, IsNotNull, PredicateExpr}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class MilvusPartitionReaderFactoryTest extends AnyFunSuite {
  test(
    "serialized reader factories retain read options without service or storage identities"
  ) {
    val input = Map(
      "MILVUS.READ.COLUMNAR" -> "false",
      MilvusOption.ReadVectorRaw -> "true",
      MilvusOption.MilvusExtraColumns -> "_segment_id",
      "milvus.token" -> "driver-only-test-token",
      "milvus.client.key" -> "driver-only-client-key",
      "milvus.uri" -> "https://driver-only-service",
      "fs.secret_access_key" -> "partition-only-storage-secret"
    )
    val retained = MilvusPartitionReaderFactory.readerOptions(input)
    assert(!MilvusOption.readColumnar(retained))
    assert(MilvusOption.readVectorRaw(retained))
    assert(MilvusOption.extraColumns(retained) == Seq("_segment_id"))
    val factory = new MilvusPartitionReaderFactory(StructType(Nil), retained)
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
    assert(!restored.supportColumnarReads(v3()))
  }

  test("requestedExtraColumns normalizes legacy aliases") {
    val requested = MilvusPartitionReaderFactory.requestedExtraColumns(
      Map(
        MilvusOption.MilvusExtraColumns ->
          "$segment_id,segment_id,$row_offset,row_offset,_timestamp"
      )
    )

    assert(
      requested == Set(
        MilvusOption.MilvusExtraColumnSegmentID,
        MilvusOption.MilvusExtraColumnRowOffset,
        MilvusOption.MilvusExtraColumnTimestamp
      )
    )
  }

  test("metadata classification is limited to requested extra columns") {
    val requested = Set(MilvusOption.MilvusExtraColumnSegmentID)

    assert(
      !MilvusPartitionReaderFactory.isMetadataExtraField(
        MilvusOption.MilvusExtraColumnTimestamp,
        requested
      )
    )
    assert(
      MilvusPartitionReaderFactory.isMetadataExtraField(
        MilvusOption.MilvusExtraColumnSegmentID,
        requested
      )
    )
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

  private val options = MilvusOption(
    new CaseInsensitiveStringMap(new java.util.HashMap[String, String]())
  )

  private def task(layout: SegmentLayout) = SegmentReadTask(
    segmentId = 30L,
    partitionId = 20L,
    layout = layout,
    schemaBytes = milvusSchema.toByteArray,
    properties = Map("fs.storage_type" -> "local")
  )

  private def v3(milvusOption: MilvusOption = options) =
    MilvusInputPartition(
      task(SegmentLayout.Manifest("files/seg")),
      milvusOption.milvusFilter
    )

  private def factory(
      columnar: Boolean,
      pushedExpression: Option[PredicateExpr] = None
  ) = new MilvusPartitionReaderFactory(
    schema,
    Map(MilvusOption.ReadColumnar -> columnar.toString),
    pushedExpression
  )

  test(
    "columnar is the default; milvus.read.columnar=false takes the row path"
  ) {
    val byDefault = new MilvusPartitionReaderFactory(schema, Map.empty)
    assert(byDefault.supportColumnarReads(v3()))
    assert(!factory(columnar = false).supportColumnarReads(v3()))
    assert(factory(columnar = true).supportColumnarReads(v3()))
  }

  test("a pushed-down predicate keeps V3 reads columnar") {
    val withPredicate = factory(
      columnar = true,
      Some(IsNotNull(FieldRef(100L, DataType.Int64)))
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
