package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.plan.{InputSpec, SegmentLayout}
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import com.zilliz.spark.connector.options.MilvusOption

/** R13: the statistics Spark uses to pick a join strategy. */
class MilvusScanStatisticsTest extends AnyFunSuite with Matchers {

  private val options = MilvusOption(
    new CaseInsensitiveStringMap(new java.util.HashMap[String, String]())
  )

  private def vector(name: String, kind: String, dim: Long): StructField =
    StructField(
      name,
      ArrayType(FloatType),
      metadata = new MetadataBuilder()
        .putLong(FieldMetadata.MilvusVectorDimensionMetadataKey, dim)
        .putString(FieldMetadata.MilvusDataTypeMetadataKey, kind)
        .build()
    )

  private def v2(rows: Long): InputPartition =
    MilvusPackedV2InputPartition(
      InputSpec(
        segmentId = rows,
        partitionId = 0L,
        layout = SegmentLayout.ColumnGroups(
          Seq(V2ColumnGroup(Seq(100L), Seq("a.parquet"), Seq(rows)))
        ),
        schemaBytes = Array.emptyByteArray,
        properties = Map("fs.storage_type" -> "local")
      ),
      options
    )

  test(
    "row width: a vector is dimension times element width, not defaultSize"
  ) {
    val schema = StructType(
      Seq(
        StructField("id", LongType),
        vector("v", "FloatVector", 768L),
        vector("h", "Float16Vector", 128L),
        vector("b", "BinaryVector", 1024L)
      )
    )
    // 8 + 768*4 + 128*2 + 1024/8
    MilvusScan.estimatedRowWidth(schema) shouldBe 8L + 3072L + 256L + 128L
  }

  test("numRows and sizeInBytes come from the plan") {
    val schema =
      StructType(
        Seq(StructField("id", LongType), vector("v", "FloatVector", 4L))
      )
    val stats = MilvusScan.statisticsFor(Array(v2(10L), v2(20L)), schema)
    stats.numRows().getAsLong shouldBe 30L
    stats.sizeInBytes().getAsLong shouldBe 30L * (8L + 16L)
  }

  // A manifest partition does not know its row count until it is opened. One
  // unknown makes the total unknown; an undercount is worse than none because
  // Spark would broadcast a table that is not small.
  test("one partition with an unknown count makes both statistics unknown") {
    val schema = StructType(Seq(StructField("id", LongType)))
    val v3 = MilvusStorageV3InputPartition(
      InputSpec(
        segmentId = 1L,
        partitionId = 0L,
        layout = SegmentLayout.Manifest("files/seg"),
        schemaBytes = Array.emptyByteArray,
        properties = Map("fs.storage_type" -> "local")
      ),
      "0",
      options
    )
    val stats = MilvusScan.statisticsFor(Array(v2(10L), v3), schema)
    stats.numRows().isPresent shouldBe false
    stats.sizeInBytes().isPresent shouldBe false
  }

  test("no partitions is zero rows, not unknown") {
    val stats = MilvusScan.statisticsFor(Array.empty, StructType(Nil))
    stats.numRows().getAsLong shouldBe 0L
  }
}
