package com.zilliz.milvus.storage.index

import java.lang.{Float => JavaFloat}
import java.util.BitSet
import scala.util.Try

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema

import com.zilliz.milvus.storage.expr.{Evaluator, Expr, PlanParser}
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.read.exec.{
  ReadMetrics,
  RowExclusions,
  SegmentIndexHandle,
  SegmentReaderRegistry
}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.snapshot.{
  SegmentIndex,
  SegmentIndexes,
  SegmentLayout
}
import com.zilliz.milvus.storage.Logging
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** Selects one persisted index and excludes deleted or nonmatching physical
  * rows before querying it. Output-column retrieval belongs to the caller.
  */
object SegmentIndexQuery extends Logging {
  final case class Request(
      vectorColumn: String,
      query: Array[Float],
      topK: Int,
      metric: String,
      filter: Option[String] = None,
      parameters: Map[String, String] = Map.empty,
      allowUnindexed: Boolean = false
  )

  sealed trait Result
  final case class Indexed(hits: Vector[PersistedIndexSearch.Hit])
      extends Result
  final case class Unindexed(selection: RowExclusions) extends Result

  private final case class Validated(
      field: FieldSchema,
      dimension: Int,
      expression: Option[Expr]
  )

  def validate(request: Request, schema: CollectionSchema): Unit = {
    validated(request, schema)
    ()
  }

  private def validated(
      request: Request,
      schema: CollectionSchema
  ): Validated = {
    require(
      request.query != null && request.query.nonEmpty && request.query.forall(
        JavaFloat.isFinite
      ),
      "Query vector must be nonempty and finite"
    )
    require(request.topK > 0, "topK must be positive")
    require(
      Set("L2", "IP", "COSINE").contains(request.metric),
      "Unsupported vector search metric"
    )
    val field = schema.fields.find(_.name == request.vectorColumn).getOrElse {
      throw new IllegalArgumentException(
        s"Unknown vector field ${request.vectorColumn}"
      )
    }
    require(
      field.dataType == DataType.FloatVector && !field.nullable,
      "Persisted index search currently requires a non-nullable FloatVector field"
    )
    val dimension =
      field.typeParams.find(_.key == "dim").map(_.value.toInt).getOrElse(0)
    require(
      dimension > 0 && request.query.length == dimension,
      s"Query dimension must equal $dimension"
    )
    require(
      request.metric != "COSINE" || request.query.exists(_ != 0.0f),
      "COSINE query must have nonzero norm"
    )
    PersistedIndexSearch.searchEf(request.topK, request.parameters)
    val expression = request.filter.map(PlanParser.parse)
    expression.foreach(Evaluator.validate(_, schema.fields))
    Validated(field, dimension, expression)
  }

  /** The persisted index that serves `fieldId` in `task`'s segment, checked
    * against what the snapshot pinned: `None` only when the snapshot says the
    * segment has no index and the request allows that. Planning checks every
    * task with this before any runs; `run` checks again on the executor.
    */
  def selectIndex(
      request: Request,
      fieldId: Long,
      task: SegmentReadTask
  ): Option[SegmentIndex] = {
    task.layout match {
      case SegmentLayout.Manifest(_, version) =>
        require(
          version >= 0,
          "Persisted index search requires a pinned data manifest version"
        )
      case _ =>
    }
    val selected = task.indexes match {
      case SegmentIndexes.Available(indexes) =>
        val matches = indexes.filter(_.fieldId == fieldId)
        require(
          matches.size <= 1,
          s"Ambiguous index for segment ${task.segmentId}, field $fieldId"
        )
        matches.headOption
      case SegmentIndexes.Unindexed => None
      case SegmentIndexes.Unknown =>
        throw new IllegalArgumentException(
          s"Snapshot has no index metadata for segment ${task.segmentId}"
        )
    }
    require(
      selected.nonEmpty || request.allowUnindexed,
      s"No persisted index for segment ${task.segmentId}, field $fieldId"
    )
    selected.foreach { descriptor =>
      require(
        descriptor.segmentId == task.segmentId && descriptor.partitionId == task.partitionId,
        s"Index identity differs from the pinned segment ${task.segmentId}"
      )
      require(
        descriptor.metricType.exists(_.equalsIgnoreCase(request.metric)),
        s"Query metric differs from the persisted index metric of segment ${task.segmentId}"
      )
      require(
        task.expectedRows.contains(descriptor.rowCount),
        s"Index row count differs from the pinned segment ${task.segmentId}"
      )
      require(
        descriptor.rowCount > 0 && descriptor.rowCount <= Int.MaxValue,
        s"Segment ${task.segmentId} bitmap exceeds supported row count"
      )
    }
    selected
  }

  /** Checks a whole plan before any task runs and names every segment that
    * cannot serve the request.
    */
  def checkPlan(
      request: Request,
      schema: CollectionSchema,
      tasks: Seq[SegmentReadTask]
  ): Unit = {
    val fieldId = validated(request, schema).field.fieldID
    val failures = tasks.flatMap { task =>
      Try(selectIndex(request, fieldId, task)).failed.toOption.map(_.getMessage)
    }
    if (failures.nonEmpty) {
      val shown = failures.take(20).mkString("; ")
      val rest =
        if (failures.size > 20) s"; ${failures.size - 20} more" else ""
      throw new IllegalArgumentException(
        s"Index search cannot run on ${failures.size} of ${tasks.size} segments: $shown$rest"
      )
    }
  }

  def run(
      request: Request,
      task: SegmentReadTask,
      arrowSchema: Schema,
      columnNameFor: Long => Option[String],
      allocator: BufferAllocator,
      reportMetrics: ReadMetrics => Unit = _ => ()
  ): Result = {
    val collection = CollectionSchema.parseFrom(task.schemaBytes)
    val prepared = validated(request, collection)
    val selected = selectIndex(request, prepared.field.fieldID, task)
    val selection =
      RowExclusions.of(task, collection, prepared.expression, columnNameFor)
    if (selected.isEmpty) {
      logWarning(
        s"Explicit unindexed brute-force fallback: segment=${task.segmentId}, field=${prepared.field.fieldID}"
      )
      return Unindexed(selection)
    }
    val descriptor = selected.get
    val excluded = new BitSet(descriptor.rowCount.toInt)
    if (selection.neededColumns.nonEmpty) {
      val reader = SegmentReaderRegistry.open(
        task,
        arrowSchema,
        selection.neededColumns,
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
              offset + batch.getRowCount <= descriptor.rowCount,
              "Filter scan exceeds index row count"
            )
            var row = 0
            while (row < batch.getRowCount) {
              if (selection.excludes(batch, row))
                excluded.set((offset + row).toInt)
              row += 1
            }
            offset += batch.getRowCount
          } finally batch.close()
          next = reader.next()
        }
        require(
          offset == descriptor.rowCount,
          "Filter scan row count differs from index"
        )
      } finally {
        try reader.close()
        finally reportMetrics(reader.metrics)
      }
    }
    val store = NativeObjectStore.Factory(task.properties).open()
    val handle =
      try
        SegmentIndexHandle.open(
          descriptor,
          prepared.dimension,
          prepared.field.nullable,
          store
        )
      finally store.close()
    val loaded = new PersistedIndexSearch(handle)
    val hits =
      try
        loaded.search(
          request.query,
          request.topK,
          offset => excluded.get(offset.toInt),
          request.parameters
        )
      finally loaded.close()
    logInfo(
      s"Persisted index search: segment=${task.segmentId}, build=${descriptor.buildId}, hits=${hits.size}, excluded=${excluded.cardinality()}"
    )
    Indexed(hits)
  }
}
