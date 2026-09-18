package com.zilliz.milvus.storage.read.exec

import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.expr.{Evaluator, Expr}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import io.milvus.grpc.schema.{CollectionSchema, FieldSchema}

/** Which rows of a segment a search must not see: the deleted ones and the ones
  * a filter rejects.
  *
  * Deciding this is what the Milvus format does with its delete files and its
  * expression semantics; a computation only reads the bitmap that comes out
  * (docs/design/architecture/vector-search.html sections 2.3 and 2.6).
  */
final class RowExclusions private (
    expression: Option[Expr],
    deletePlan: DeletePlan,
    pkField: Option[FieldSchema],
    columnNameFor: Long => Option[String],
    fieldNameToColumn: Map[String, String]
) {
  val expressionFields: Set[String] =
    expression.map(_.fields).getOrElse(Set.empty)
  private val pkColumn =
    pkField.map(f => columnNameFor(f.fieldID).getOrElse(f.name))
  private val timestampColumn = columnNameFor(1L).getOrElse("Timestamp")
  private def arrowColumn(name: String): String =
    fieldNameToColumn.getOrElse(name, name)

  /** The columns a reader has to deliver for this to be decided at all. */
  val neededColumns: Seq[String] =
    (expressionFields.toSeq.sorted.map(arrowColumn) ++
      (if (deletePlan.isEmpty) Seq.empty
       else Seq(pkColumn.get, timestampColumn))).distinct

  def excludes(batch: VectorSchemaRoot, row: Int): Boolean = {
    val deleted = !deletePlan.isEmpty && DeletePlans.rowDeleted(
      deletePlan,
      pkField.get,
      batch.getVector(pkColumn.get),
      batch.getVector(timestampColumn),
      row
    )
    deleted || expression.exists(e =>
      !Evaluator.matches(e, batch, row, arrowColumn)
    )
  }
}

object RowExclusions {

  def of(
      task: SegmentReadTask,
      collection: CollectionSchema,
      expression: Option[Expr],
      columnNameFor: Long => Option[String]
  ): RowExclusions = {
    val pkField = collection.fields.find(_.isPrimaryKey)
    val deletes = DeletePlans.of(task, pkField)
    require(
      deletes.isEmpty || pkField.nonEmpty,
      "Applying deletes requires a primary key field"
    )
    val names = collection.fields
      .map(field =>
        field.name -> columnNameFor(field.fieldID).getOrElse(field.name)
      )
      .toMap
    new RowExclusions(expression, deletes, pkField, columnNameFor, names)
  }
}
