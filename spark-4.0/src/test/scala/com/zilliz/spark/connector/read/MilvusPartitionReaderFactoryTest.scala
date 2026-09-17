package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.expr.{FieldRef, IsNotNull, PredicateExpr}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import com.zilliz.spark.connector.options.{MilvusOption, VectorSearch}
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class MilvusPartitionReaderFactoryTest extends AnyFunSuite {
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
    MilvusV3InputPartition(
      task(SegmentLayout.Manifest("files/seg")),
      "20",
      milvusOption
    )

  private def v2(milvusOption: MilvusOption = options) = MilvusV2InputPartition(
    task(
      SegmentLayout.ColumnGroups(
        Seq(V2ColumnGroup(Seq(100L), Seq("a.parquet"), Seq(10L)))
      )
    ),
    milvusOption
  )

  private val bruteForce = options.copy(
    vectorSearch = Some(
      VectorSearch(
        Array(1f, 2f),
        topK = 10,
        metricType = "L2",
        vectorColumn = "vector"
      )
    )
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
    assert(byDefault.supportColumnarReads(v2()))
    assert(!factory(columnar = false).supportColumnarReads(v3()))
    assert(factory(columnar = true).supportColumnarReads(v3()))
  }

  test("a pushed-down predicate keeps both storage lines columnar") {
    val withPredicate = factory(
      columnar = true,
      Some(IsNotNull(FieldRef(100L, DataType.Int64)))
    )
    assert(withPredicate.supportColumnarReads(v3()))
    assert(withPredicate.supportColumnarReads(v2()))
  }

  // The search runs in the row reader whatever the segment's storage line.
  // Only V3 partitions used to carry it, so a V2 segment of a brute-force read
  // was scanned whole and every row came back as if it were a hit.
  test("a brute-force search sends both storage lines to the row reader") {
    val f = factory(columnar = true)
    assert(!f.supportColumnarReads(v2(bruteForce)))
    assert(!f.supportColumnarReads(v3(bruteForce)))
    assert(f.searchFor(v2(bruteForce)).map(_.topK) == Some(10))
    assert(f.searchFor(v3(bruteForce)).map(_.topK) == Some(10))
    assert(f.searchFor(v2()).isEmpty && f.searchFor(v3()).isEmpty)
  }

  test("row-reader construction failure closes its task allocator") {
    val expression = IsNotNull(FieldRef(100L, DataType.Int64))
    val searchOptions = options.copy(
      vectorSearch = Some(
        VectorSearch(
          Array(1f),
          topK = 1,
          metricType = "L2",
          vectorColumn = "vector",
          mode = "index"
        )
      )
    )
    val partition = MilvusV3InputPartition(
      task(SegmentLayout.Manifest("files/seg")),
      "20",
      searchOptions
    )
    val root = ArrowAllocator.get
    val childrenBefore = root.getChildAllocators.asScala.size

    intercept[IllegalArgumentException] {
      factory(columnar = false, Some(expression)).createReader(partition)
    }

    assert(root.getChildAllocators.asScala.size == childrenBefore)
  }
}
