package com.zilliz.milvus.storage.index

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.{
  SegmentIndex,
  SegmentIndexes,
  SegmentLayout
}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class SegmentIndexQueryTest extends AnyFunSuite {
  private val collection = CollectionSchema(
    name = "c",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(
        fieldID = 101L,
        name = "vector",
        dataType = DataType.FloatVector,
        typeParams = Seq(KeyValuePair("dim", "2"))
      ),
      FieldSchema(
        fieldID = 102L,
        name = "category",
        dataType = DataType.Int64,
        nullable = true
      )
    )
  )
  private val schema =
    SchemaMapper.convertToArrowSchemaWithFieldIdNames(collection)
  private val request =
    SegmentIndexQuery.Request("vector", Array(1f, 0f), 2, "COSINE")
  private val task = SegmentReadTask(
    3L,
    2L,
    SegmentLayout.Manifest("must-not-open", 1L),
    collection.toByteArray,
    Map.empty,
    indexes = SegmentIndexes.Unindexed,
    snapshotRows = Some(5L)
  )
  private val index = SegmentIndex(
    1L,
    2L,
    3L,
    101L,
    4L,
    5L,
    "vector",
    Map("index_type" -> "HNSW", "metric_type" -> "COSINE"),
    Vector("must-not-open/HNSW"),
    5L,
    100L,
    1L,
    Some(8),
    Some(0)
  )

  test("only an explicitly unindexed segment can opt into brute force") {
    val allocator = new RootAllocator()
    try {
      def run(value: SegmentReadTask, fallback: Boolean) =
        SegmentIndexQuery.run(
          request.copy(allowUnindexed = fallback),
          value,
          schema,
          id => Some(id.toString),
          allocator
        )
      intercept[IllegalArgumentException](run(task, fallback = false))
      assert(
        run(task, fallback = true).isInstanceOf[SegmentIndexQuery.Unindexed]
      )
      intercept[IllegalArgumentException](
        run(task.copy(indexes = SegmentIndexes.Unknown), fallback = true)
      )
      intercept[IllegalArgumentException](
        run(
          task.copy(layout = SegmentLayout.Manifest("absent", -1)),
          fallback = true
        )
      )
      Seq(
        Vector(index, index.copy(buildId = 6)),
        Vector(index.copy(rowCount = 4)),
        Vector(index.copy(segmentId = 9)),
        Vector(index.copy(partitionId = 9)),
        Vector(
          index.copy(parameters = index.parameters.updated("metric_type", "IP"))
        )
      ).foreach { indexes =>
        intercept[IllegalArgumentException](
          run(
            task.copy(indexes = SegmentIndexes.Available(indexes)),
            fallback = true
          )
        )
      }
    } finally allocator.close()
  }

  test(
    "shared row selection excludes filter nulls and deleted rows before either search path"
  ) {
    val allocator = new RootAllocator()
    val batch = VectorSchemaRoot.create(schema, allocator)
    try {
      val result = SegmentIndexQuery
        .run(
          request.copy(filter = Some("category >= 1"), allowUnindexed = true),
          task.copy(deletes =
            DeleteSource.Materialized(
              DeletePlan.fromLongPks(Map(12L -> 200L, 13L -> 50L))
            )
          ),
          schema,
          id => Some(id.toString),
          allocator
        )
        .asInstanceOf[SegmentIndexQuery.Unindexed]
      assert(result.selection.neededColumns.toSet == Set("102", "100", "1"))
      assert(!result.selection.neededColumns.contains("101"))
      batch.allocateNew()
      (0 until 5).foreach { row =>
        batch
          .getVector("100")
          .asInstanceOf[BigIntVector]
          .setSafe(row, 10L + row)
        batch.getVector("1").asInstanceOf[BigIntVector].setSafe(row, 100L)
        val category = batch.getVector("102").asInstanceOf[BigIntVector]
        if (row == 1) category.setNull(row)
        else category.setSafe(row, if (row == 0) 0L else 1L)
      }
      batch.setRowCount(5)
      assert(
        (0 until 5).map(result.selection.excludes(batch, _)) == Seq(true, true,
          true, false, false)
      )
    } finally { batch.close(); allocator.close() }
  }
}
