package com.zilliz.milvus.storage.read.exec

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.milvus.storage.snapshot.SegmentLayout
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** Which rows a search must not see, decided before any search path runs
  * (docs/design/architecture/vector-search.html sections 2.3 and 2.4).
  */
class RowExclusionsTest extends AnyFunSuite with Matchers {

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

  private val task = SegmentReadTask(
    3L,
    2L,
    SegmentLayout.Manifest("must-not-open", 1L),
    collection.toByteArray,
    Map.empty,
    deletes = DeleteSource.Materialized(
      DeletePlan.fromLongPks(Map(12L -> 200L, 13L -> 50L))
    ),
    snapshotRows = Some(5L)
  )

  test("deletes and the filter exclude rows, the vector column is not read") {
    val exclusions = RowExclusions.of(
      task,
      collection,
      Some(PlanParser.parse("category >= 1")),
      id => Some(id.toString)
    )

    // The filter's column, the primary key and the timestamp, and nothing else:
    // an index probe never opens the vector column to decide this.
    exclusions.neededColumns.toSet shouldBe Set("102", "100", "1")

    val allocator = new RootAllocator()
    val batch = VectorSchemaRoot.create(schema, allocator)
    try {
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

      // Row 0 fails the filter, row 1 is null, row 2 has primary key 12 and a
      // delete timestamped after it; rows 3 and 4 survive.
      (0 until 5).map(exclusions.excludes(batch, _)) shouldBe Seq(
        true, true, true, false, false
      )
    } finally {
      batch.close()
      allocator.close()
    }
  }

  test("without deletes or a filter, nothing is read and nothing is excluded") {
    val exclusions = RowExclusions.of(
      task.copy(deletes = DeleteSource.None),
      collection,
      None,
      id => Some(id.toString)
    )

    exclusions.neededColumns shouldBe empty
    exclusions.expressionFields shouldBe empty
  }
}
