package com.zilliz.milvus.storage.read.exec

import java.util.BitSet

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
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

  /** The rows of a whole segment a search must not see, one bit per row.
    *
    * An index holds every row of its segment, so probing it needs the whole
    * segment's bitmap before the first query runs. Building it reads the
    * primary key, the timestamp and the filter's columns and nothing else: the
    * vector column is never opened on this path
    * (docs/design/architecture/vector-search.html section 2.4).
    */
  def bitmap(
      task: SegmentReadTask,
      arrowSchema: Schema,
      rows: Long,
      allocator: BufferAllocator,
      reportMetrics: ReadMetrics => Unit = _ => ()
  ): BitSet = {
    require(
      rows > 0 && rows <= Int.MaxValue,
      s"Segment ${task.segmentId} has $rows rows, outside what a bitmap covers"
    )
    val excluded = new BitSet(rows.toInt)
    if (neededColumns.isEmpty) return excluded
    val reader = SegmentReaderRegistry.open(
      task,
      arrowSchema,
      neededColumns,
      columnNameFor,
      allocator
    )
    var offset = 0L
    try {
      var next = reader.next()
      while (next.nonEmpty) {
        val batch = next.get
        try {
          require(
            offset + batch.getRowCount <= rows,
            s"Segment ${task.segmentId} holds more rows than the $rows it declared"
          )
          var row = 0
          while (row < batch.getRowCount) {
            if (excludes(batch, row)) excluded.set((offset + row).toInt)
            row += 1
          }
          offset += batch.getRowCount
        } finally batch.close()
        next = reader.next()
      }
      require(
        offset == rows,
        s"Segment ${task.segmentId} gave $offset of the $rows rows it declared"
      )
    } finally {
      try reader.close()
      finally reportMetrics(reader.metrics)
    }
    excluded
  }

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
