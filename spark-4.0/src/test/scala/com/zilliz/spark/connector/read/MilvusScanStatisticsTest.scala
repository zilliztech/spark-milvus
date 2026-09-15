package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.SparkTypes
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** Scan statistics Spark uses to pick a join strategy. */
class MilvusScanStatisticsTest extends AnyFunSuite with Matchers {

  private val options = MilvusOption(
    new CaseInsensitiveStringMap(new java.util.HashMap[String, String]())
  )

  private def vector(
      name: String,
      kind: MilvusDataType,
      dim: Long
  ): StructField = {
    val field = FieldSchema(
      name = name,
      dataType = kind,
      typeParams = Seq(KeyValuePair(key = "dim", value = dim.toString))
    )
    StructField(
      name,
      SparkTypes.toDataType(field),
      metadata = SparkTypes.metadata(field)
    )
  }

  private def v2(rows: Long): InputPartition =
    MilvusV2InputPartition(
      SegmentReadTask(
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
        vector("f32", MilvusDataType.FloatVector, 768L),
        vector("f16", MilvusDataType.Float16Vector, 128L),
        vector("bf16", MilvusDataType.BFloat16Vector, 64L),
        vector("i8", MilvusDataType.Int8Vector, 32L),
        vector("bits", MilvusDataType.BinaryVector, 1025L)
      )
    )
    // 8 + 768*4 + 128*2 + 64*2 + 32 + ceil(1025/8)
    MilvusScan.estimatedRowWidth(schema) shouldBe
      8L + 3072L + 256L + 128L + 32L + 129L
  }

  test("row width falls back conservatively for absent or unknown type") {
    val dimOnly = StructField(
      "absent",
      ArrayType(FloatType),
      metadata = new MetadataBuilder()
        .putLong(FieldMetadata.MilvusVectorDimensionMetadataKey, 3L)
        .build()
    )
    val unknown = StructField(
      "unknown",
      ArrayType(FloatType),
      metadata = new MetadataBuilder()
        .putLong(FieldMetadata.MilvusVectorDimensionMetadataKey, 5L)
        .putLong(FieldMetadata.MilvusDataTypeMetadataKey, 999L)
        .build()
    )
    val outOfRange = StructField(
      "out_of_range",
      ArrayType(FloatType),
      metadata = new MetadataBuilder()
        .putLong(FieldMetadata.MilvusVectorDimensionMetadataKey, 7L)
        .putLong(
          FieldMetadata.MilvusDataTypeMetadataKey,
          (1L << 32) + MilvusDataType.BinaryVector.value
        )
        .build()
    )

    MilvusScan.estimatedRowWidth(
      StructType(Seq(dimOnly, unknown, outOfRange))
    ) shouldBe 3L * 4L + 5L * 4L + 7L * 4L
  }

  test("numRows and sizeInBytes come from the plan") {
    val schema =
      StructType(
        Seq(
          StructField("id", LongType),
          vector("v", MilvusDataType.FloatVector, 4L)
        )
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
    val v3 = MilvusV3InputPartition(
      SegmentReadTask(
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
