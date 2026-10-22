package com.zilliz.milvus.storage.read.exec

import java.util.BitSet

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.expr.{
  FilterEvaluator,
  FilterExpr,
  PredicateEvaluator,
  PredicateExpr
}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import io.milvus.grpc.schema.{CollectionSchema, FieldSchema}

/** Which rows of a segment a search must not see: the deleted ones, the ones
  * the Milvus filter rejects, and the ones a predicate Spark pushed into the
  * scan does not keep (SQL three-valued logic: only TRUE survives).
  *
  * Deciding this is what the Milvus format does with its delete files and its
  * filter semantics; a computation only reads the bitmap that comes out (design
  * document architecture/vector-search.html sections 2.3 and 2.6).
  */
final class RowExclusions private (
    filter: Option[FilterExpr],
    predicate: Option[PredicateExpr],
    deletePlan: DeletePlan,
    pkField: Option[FieldSchema],
    columnNameFor: Long => Option[String],
    fieldNameToColumn: Map[String, String]
) {
  val filterFields: Set[String] =
    filter.map(_.fields).getOrElse(Set.empty)
  private val pkColumn =
    pkField.map(f => columnNameFor(f.fieldID).getOrElse(f.name))
  private val timestampColumn = columnNameFor(1L).getOrElse("Timestamp")
  private def arrowColumn(name: String): String =
    fieldNameToColumn.getOrElse(name, name)
  private val predicateColumns: Seq[String] =
    predicate.toSeq.flatMap(PredicateExpr.fieldIds).sorted.map { fieldId =>
      columnNameFor(fieldId).getOrElse(
        throw new IllegalArgumentException(
          s"The pushed predicate reads field $fieldId, which this segment does not hold"
        )
      )
    }

  /** The columns a reader has to deliver for this to be decided at all. */
  val neededColumns: Seq[String] =
    (filterFields.toSeq.sorted.map(arrowColumn) ++ predicateColumns ++
      (if (deletePlan.isEmpty) Seq.empty
       else Seq(pkColumn.get, timestampColumn))).distinct

  /** The rows of a whole segment a search must not see, one bit per row.
    *
    * An index holds every row of its segment, so searching it needs the whole
    * segment's bitmap before the first query runs. Building it reads the
    * primary key, the timestamp and the filter's columns and nothing else: the
    * vector column is never opened on this path (design document
    * architecture/vector-search.html section 2.4).
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
    val reader = SegmentReader.open(
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
          val inBatch = this.excluded(batch)
          var row = inBatch.nextSetBit(0)
          while (row >= 0) {
            excluded.set((offset + row).toInt)
            row = inBatch.nextSetBit(row + 1)
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

  /** The rows of one batch a search must not see, one bit per row. A pushed
    * predicate is evaluated over the whole batch at once.
    */
  def excluded(batch: VectorSchemaRoot): BitSet = {
    val rows = batch.getRowCount
    val excluded = new BitSet(math.max(rows, 1))
    val rejected =
      predicate.map(PredicateEvaluator.evaluate(_, batch, columnNameFor))
    var row = 0
    while (row < rows) {
      if (excludes(batch, row) || rejected.exists(_.isExcluded(row))) {
        excluded.set(row)
      }
      row += 1
    }
    excluded
  }

  /** What is decided row by row: a delete, or the Milvus filter. */
  private def excludes(batch: VectorSchemaRoot, row: Int): Boolean = {
    val deleted = !deletePlan.isEmpty && DeletePlans.rowDeleted(
      deletePlan,
      pkField.get,
      batch.getVector(pkColumn.get),
      batch.getVector(timestampColumn),
      row
    )
    deleted || filter.exists(e =>
      !FilterEvaluator.matches(e, batch, row, arrowColumn)
    )
  }
}

object RowExclusions {

  def of(
      task: SegmentReadTask,
      collection: CollectionSchema,
      filter: Option[FilterExpr],
      columnNameFor: Long => Option[String],
      predicate: Option[PredicateExpr] = None
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
    new RowExclusions(
      filter,
      predicate,
      deletes,
      pkField,
      columnNameFor,
      names
    )
  }
}
