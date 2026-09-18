package com.zilliz.spark.connector.read

import scala.collection.mutable

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.index.SegmentIndexQuery
import com.zilliz.milvus.storage.read.exec.{ReadMetrics, SegmentReaderRegistry}
import com.zilliz.milvus.storage.read.exec.RowExclusions
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.spark.connector.options.VectorSearch
import com.zilliz.spark.connector.types.{ArrowConverter, SparkTypes}
import io.milvus.grpc.schema.CollectionSchema

/** Adapts the shared index execution to Spark rows. The vector column is not
  * opened for indexed distance computation; take reads only requested output.
  */
private[read] object SegmentIndexSearch extends Logging {
  private def request(search: VectorSearch): SegmentIndexQuery.Request =
    SegmentIndexQuery.Request(
      search.vectorColumn,
      search.queryVector,
      search.topK,
      search.metricType,
      search.filter,
      search.searchParameters.toMap,
      search.allowUnindexed
    )

  def validate(search: VectorSearch, schema: CollectionSchema): Unit =
    SegmentIndexQuery.validate(request(search), schema)

  /** Fails planning when a segment of the plan cannot serve the search. */
  def checkPlan(
      search: VectorSearch,
      schema: CollectionSchema,
      tasks: Seq[SegmentReadTask]
  ): Unit =
    SegmentIndexQuery.checkPlan(request(search), schema, tasks)

  def run(
      search: VectorSearch,
      schema: StructType,
      setup: ColumnBinding,
      allocator: BufferAllocator,
      reportMetrics: ReadMetrics => Unit
  ): SegmentVectorSearch.Search = {
    val hits = SegmentIndexQuery.run(
      request(search),
      setup.task,
      setup.arrowSchema,
      setup.columnNameFor,
      allocator,
      reportMetrics
    ) match {
      case SegmentIndexQuery.Indexed(found) => found
      case SegmentIndexQuery.Unindexed(selection) =>
        return bruteForce(
          search,
          schema,
          setup,
          allocator,
          selection,
          reportMetrics
        )
    }
    if (hits.isEmpty) return SegmentVectorSearch.Search(Iterator.empty, 0L)
    val offsets = hits.map(_.rowOffset).distinct.sorted.toArray
    val outputColumns = schema.fieldNames.toSeq.map(setup.arrowColumnFor)
    // A zero-column projection still needs row cardinality from one scalar field.
    val projected =
      if (outputColumns.nonEmpty) outputColumns else Seq(setup.pkColumnName)
    val reader = SegmentReaderRegistry.open(
      setup.task,
      setup.arrowSchema,
      projected,
      setup.columnNameFor,
      allocator
    )
    val byOffset =
      mutable.Map.empty[Long, InternalRow]
    try {
      val taken = reader.take(offsets, projected)
      var index = 0
      try {
        var next = taken.next()
        while (next.nonEmpty) {
          val batch = next.get
          try {
            var row = 0
            while (row < batch.getRowCount) {
              require(index < offsets.length, "take returned too many rows")
              byOffset(offsets(index)) = ArrowConverter
                .arrowToInternalRow(batch, row, schema, setup.arrowColumnNames)
                .copy()
              index += 1; row += 1
            }
          } finally batch.close()
          next = taken.next()
        }
        require(
          index == offsets.length,
          "take returned fewer rows than requested"
        )
      } finally taken.close()
    } finally {
      try reader.close()
      finally reportMetrics(reader.metrics)
    }
    SegmentVectorSearch.Search(
      hits.iterator.map(hit =>
        SegmentVectorSearch
          .Result(byOffset(hit.rowOffset), hit.score, hit.rowOffset)
      ),
      offsets.length.toLong
    )
  }

  private def bruteForce(
      search: VectorSearch,
      output: StructType,
      setup: ColumnBinding,
      allocator: BufferAllocator,
      selection: RowExclusions,
      reportMetrics: ReadMetrics => Unit
  ): SegmentVectorSearch.Search = {
    val collection = CollectionSchema.parseFrom(setup.task.schemaBytes)
    val required = selection.expressionFields + search.vectorColumn
    val extra = collection.fields
      .filter(f => required(f.name) && !output.fieldNames.contains(f.name))
      .map(f => SparkTypes.toStructField(f, rawVectors = false))
    val readSchema = StructType(output.fields.toSeq ++ extra)
    val names = (readSchema.fieldNames.toSeq.map(setup.arrowColumnFor) ++
      selection.neededColumns).distinct
    val reader = SegmentReaderRegistry.open(
      setup.task,
      setup.arrowSchema,
      names,
      setup.columnNameFor,
      allocator
    )
    val batches = new Iterator[VectorSchemaRoot] {
      private var pending =
        Option.empty[VectorSchemaRoot]
      def hasNext: Boolean = {
        if (pending.isEmpty) pending = reader.next(); pending.nonEmpty
      }
      def next(): VectorSchemaRoot = {
        if (!hasNext) throw new NoSuchElementException
        val result = pending.get; pending = None; result
      }
    }
    val result =
      try
        SegmentVectorSearch
          .run(
            search,
            readSchema,
            setup.arrowColumnNames,
            batches,
            selection.excludes
          )
      finally {
        try reader.close()
        finally reportMetrics(reader.metrics)
      }
    val projected = result.results.map { hit =>
      val row = InternalRow.fromSeq(
        output.fields
          .map(f => hit.row.get(readSchema.fieldIndex(f.name), f.dataType))
          .toSeq
      )
      SegmentVectorSearch.Result(row, hit.distance, hit.rowOffset)
    }
    SegmentVectorSearch.Search(projected, result.rowsMaterialized)
  }
}
