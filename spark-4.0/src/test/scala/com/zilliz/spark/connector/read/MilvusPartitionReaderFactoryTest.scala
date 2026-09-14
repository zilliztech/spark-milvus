package com.zilliz.spark.connector.read

import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.read.plan.InputSpec
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}
import com.zilliz.milvus.storage.snapshot.SegmentLayout

class MilvusPartitionReaderFactoryTest extends AnyFunSuite {
  test("requestedExtraColumns normalizes legacy aliases") {
    val requested = MilvusPartitionReaderFactory.requestedExtraColumns(
      Map(MilvusOption.MilvusExtraColumns -> "partition,segment_id,row_offset")
    )

    assert(
      requested == Set(
        MilvusOption.MilvusExtraColumnPartition,
        MilvusOption.MilvusExtraColumnSegmentID,
        MilvusOption.MilvusExtraColumnRowOffset
      )
    )
  }

  test("metadata classification is limited to requested extra columns") {
    val requested = Set(MilvusOption.MilvusExtraColumnSegmentID)

    assert(
      !MilvusPartitionReaderFactory.isMetadataExtraField(
        MilvusOption.MilvusExtraColumnPartition,
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

  private def spec(layout: SegmentLayout) = InputSpec(
    segmentId = 30L,
    partitionId = 20L,
    layout = layout,
    schemaBytes = milvusSchema.toByteArray,
    properties = Map("fs.storage_type" -> "local")
  )

  private def v3(
      topK: Option[Int] = None,
      queryVector: Option[Array[Float]] = None
  ) = MilvusStorageV3InputPartition(
    spec(SegmentLayout.Manifest("files/seg")),
    "20",
    options,
    topK = topK,
    queryVector = queryVector
  )

  private def v2() = MilvusPackedV2InputPartition(
    spec(
      SegmentLayout.ColumnGroups(
        Seq(V2ColumnGroup(Seq(100L), Seq("a.parquet"), Seq(10L)))
      )
    ),
    options
  )

  private def factory(
      columnar: Boolean,
      pushedFilters: Array[Filter] = Array.empty
  ) = new MilvusPartitionReaderFactory(
    schema,
    if (columnar) Map(MilvusOption.ReadColumnar -> "true") else Map.empty,
    pushedFilters
  )

  test("columnar is off unless the read asks for it") {
    assert(!factory(columnar = false).supportColumnarReads(v3()))
    assert(factory(columnar = true).supportColumnarReads(v3()))
    assert(factory(columnar = true).supportColumnarReads(v2()))
  }

  // The scan builder keeps the predicates it said it would evaluate, and Spark
  // does not re-apply them. The columnar reader does not evaluate them, so a
  // partition that carries any goes back to the row reader instead of silently
  // returning rows that should have been filtered out.
  test("a pushed-down filter sends the partition back to the row reader") {
    val withFilter =
      factory(columnar = true, Array[Filter](EqualTo("id", 1L)))
    assert(!withFilter.supportColumnarReads(v3()))
    assert(!withFilter.supportColumnarReads(v2()))
  }

  // topK and queryVector make the row reader run a brute-force search. The
  // columnar reader would ignore them and return the whole segment.
  test("vector search parameters send the partition back to the row reader") {
    val f = factory(columnar = true)
    assert(!f.supportColumnarReads(v3(topK = Some(10))))
    assert(
      !f.supportColumnarReads(v3(queryVector = Some(Array(1f, 2f))))
    )
  }
}
