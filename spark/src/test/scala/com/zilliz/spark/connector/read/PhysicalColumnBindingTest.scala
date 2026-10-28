package com.zilliz.spark.connector.read

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.read.exec.PhysicalColumnGroup
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class PhysicalColumnBindingTest extends AnyFunSuite with Matchers {
  private val collection = CollectionSchema(fields =
    Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(fieldID = 101L, name = "value", dataType = DataType.Int64)
    )
  )
  private val physical = SegmentLayout.PhysicalColumnGroups(
    Seq(
      PhysicalColumnGroup(Seq("0", "1", "100", "101"), Seq("data/a"), Seq(2L))
    ),
    SchemaMapper.convertToArrowSchemaWithFieldIdNames(collection).toJson,
    Map(0L -> "0", 1L -> "1", 100L -> "100", 101L -> "101"),
    Map("id" -> "100", "value" -> "101")
  )
  private val task =
    SegmentReadTask(30L, 20L, physical, collection.toByteArray, Map.empty)
  private val output = StructType(Seq(StructField("value", LongType)))

  test(
    "resolved columns drive projection and delete columns without format inference"
  ) {
    val binding = ColumnBinding(
      MilvusInputPartition(
        task.copy(
          deletes =
            DeleteSource.Materialized(DeletePlan.fromLongPks(Map(1L -> 2L)))
        )
      ),
      output
    )
    binding shouldBe a[PhysicalColumnBinding]
    binding.arrowColumnFor("value") shouldBe "101"
    binding.neededColumns shouldBe Seq("101", "100", "1")
    binding.columnNameFor(100L) shouldBe Some("100")
    binding.arrowSchema shouldBe SchemaMapper
      .convertToArrowSchemaWithFieldIdNames(collection)
  }

  test(
    "unknown selected fields or absent physical columns fail before opening data"
  ) {
    val unknown = ColumnBinding(
      MilvusInputPartition(task.copy(neededFieldIds = Seq(500L))),
      output
    )
    an[IllegalArgumentException] should be thrownBy unknown.neededColumns
    val missing = physical.copy(groups =
      Seq(PhysicalColumnGroup(Seq("100"), Seq("data/a"), Seq(2L)))
    )
    val incomplete =
      ColumnBinding(MilvusInputPartition(task.copy(layout = missing)), output)
    an[IllegalArgumentException] should be thrownBy incomplete.neededColumns
    an[IllegalArgumentException] should be thrownBy ColumnBinding(
      MilvusInputPartition(
        task.copy(layout = physical.copy(fieldNames = Map(100L -> "absent")))
      ),
      output
    )
  }
}
