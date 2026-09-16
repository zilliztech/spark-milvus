package com.zilliz.spark.connector.read

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.expr.{
  Bitmap,
  PredicateEvaluator,
  PredicateExpr
}
import com.zilliz.milvus.storage.read.exec.{ReadMetrics, SegmentReader}
import com.zilliz.spark.connector.metrics.ScanMetrics
import com.zilliz.spark.connector.options.VectorSearch
import com.zilliz.spark.connector.types.{ArrowAllocator, ArrowConverter}

/** The one row reader for both storage lines: Arrow batches from
  * `core.read.exec` become Spark rows. What differs between the lines is how
  * columns are named and which the query needs, and that is the
  * `ColumnBinding`'s; the loop here does not know which line it reads.
  *
  * The loop pulls a batch, hands out its rows one by one, skips a deleted row,
  * evaluates the pushed expression once per Arrow batch, closes the batch and
  * pulls the next. The `SegmentReader` opened by the binding owns the EOF
  * row-count contract, so row and columnar consumers cannot disagree about a
  * short read.
  *
  * A vector search replaces the scan: on the first `next()` the whole segment
  * is scored by `SegmentVectorSearch` and the top-k come out, each row with its
  * distance appended.
  */
class MilvusRowPartitionReader(
    schema: StructType,
    setup: ColumnBinding,
    pushedExpression: Option[PredicateExpr] = None,
    vectorSearch: Option[VectorSearch] = None,
    includeSearchScore: Boolean = true,
    searchScorePosition: Option[Int] = None,
    allocator: BufferAllocator = ArrowAllocator.get,
    taskAllocatorOwner: Option[AutoCloseable] = None
) extends RowOffsetReader
    with Logging {

  require(
    pushedExpression.isEmpty || vectorSearch.isEmpty,
    "predicate pushdown is not defined for vector search"
  )

  private val applyDeletes: Boolean = setup.appliesDeletes
  private val arrowColumnNames: Map[String, String] = setup.arrowColumnNames

  // Native handles start out null so a constructor that throws can release
  // whatever it took; Spark only closes a reader it got back.
  private var segmentReader: SegmentReader = null
  private var allocatorOwner: AutoCloseable = taskAllocatorOwner.orNull
  private var currentBatch: VectorSchemaRoot = null
  private var currentPredicateBitmap: Bitmap = null
  private var currentRowIndex: Int = 0
  private var currentBatchStartRowOffset: Long = 0L
  private var _lastReturnedRowOffset: Long = -1L

  private var searchResults: Iterator[SegmentVectorSearch.Result] = null

  // Rows actually turned into InternalRow. Predicate evaluation stays on Arrow
  // vectors, so neither deleted nor filtered-out rows contribute.
  private var materialized: Long = 0L
  private var finalMetrics: ReadMetrics = ReadMetrics.Zero

  /** Row offset of the row `get()` last returned, for the row-offset metadata
    * column; -1 before the first row.
    */
  def lastReturnedRowOffset: Long = _lastReturnedRowOffset

  try {
    if (!vectorSearch.exists(_.mode == "index")) {
      segmentReader = setup.open(allocator)
      loadNextBatch()
    }
  } catch {
    case e: Throwable =>
      releaseAll()
      throw e
  }

  private def pullNextBatch(): VectorSchemaRoot =
    if (segmentReader == null) null else segmentReader.next().orNull

  /** Keeps a prefetched batch owned by this reader until `next()` transfers it.
    * Query validation may fail without consuming the iterator, in which case
    * `close()` still releases that batch.
    */
  private def remainingBatches(): Iterator[VectorSchemaRoot] =
    new Iterator[VectorSchemaRoot] {
      private var exhausted = currentBatch == null

      override def hasNext: Boolean = {
        if (!exhausted && currentBatch == null) {
          loadNextBatch()
          exhausted = currentBatch == null
        }
        !exhausted
      }

      override def next(): VectorSchemaRoot = {
        if (!hasNext) throw new NoSuchElementException("No remaining batch")
        val batch = currentBatch
        currentBatch = null
        currentPredicateBitmap = null
        batch
      }
    }

  private def loadNextBatch(): Unit = {
    currentBatch = pullNextBatch()
    currentPredicateBitmap =
      if (currentBatch == null) null
      else
        pushedExpression
          .map(
            PredicateEvaluator.evaluate(
              _,
              currentBatch,
              setup.columnNameFor
            )
          )
          .orNull
  }

  private def isDeleted(batch: VectorSchemaRoot, rowIndex: Int): Boolean =
    applyDeletes && !setup.deletePlan.isEmpty && setup.isDeleted(
      batch,
      rowIndex
    )

  private def isExcluded(batch: VectorSchemaRoot, rowIndex: Int): Boolean =
    isDeleted(batch, rowIndex) ||
      (currentPredicateBitmap != null &&
        currentPredicateBitmap.isExcluded(rowIndex))

  override def next(): Boolean = vectorSearch match {
    case Some(search) =>
      if (searchResults == null) {
        val result = if (search.mode == "index") {
          SegmentIndexSearch.run(
            search,
            schema,
            setup,
            allocator,
            metrics => finalMetrics = finalMetrics + metrics
          )
        } else {
          SegmentVectorSearch.run(
            search,
            schema,
            arrowColumnNames,
            remainingBatches(),
            isDeleted
          )
        }
        searchResults = result.results
        materialized += result.rowsMaterialized
      }
      searchResults.hasNext
    case None => nextRow()
  }

  private def nextRow(): Boolean = {
    while (true) {
      // A 0-row batch is not EOF; only a null batch is.
      while (
        currentBatch != null && currentRowIndex >= currentBatch.getRowCount
      ) {
        val exhausted = currentBatch
        currentBatch = null
        currentPredicateBitmap = null
        currentBatchStartRowOffset += exhausted.getRowCount.toLong
        exhausted.close()
        loadNextBatch()
        currentRowIndex = 0
      }
      if (currentBatch == null) {
        return false
      }
      if (isExcluded(currentBatch, currentRowIndex)) {
        currentRowIndex += 1
      } else {
        return true
      }
    }
    false
  }

  override def get(): InternalRow = vectorSearch match {
    case Some(search) =>
      val result = searchResults.next()
      _lastReturnedRowOffset = result.rowOffset
      if (search.mode == "index" && !includeSearchScore) result.row
      else {
        val values = result.row.toSeq(schema)
        val position = searchScorePosition.getOrElse(values.size)
        require(
          position >= 0 && position <= values.size,
          "Invalid search score position"
        )
        InternalRow.fromSeq(values.patch(position, Seq(result.distance), 0))
      }
    case None =>
      if (currentBatch == null) {
        throw new IllegalStateException("No batch loaded")
      }
      _lastReturnedRowOffset = currentBatchStartRowOffset + currentRowIndex
      val row = ArrowConverter.arrowToInternalRow(
        currentBatch,
        currentRowIndex,
        schema,
        arrowColumnNames
      )
      currentRowIndex += 1
      materialized += 1L
      row
  }

  override def close(): Unit = releaseAll()

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val metrics =
      if (segmentReader != null) segmentReader.metrics else finalMetrics
    ScanMetrics.taskValues(metrics, materialized)
  }

  // Each handle is released on its own so one failure does not strand the
  // rest; the null sentinels make a second close a no-op.
  private def releaseAll(): Unit = {
    if (currentBatch != null) {
      try currentBatch.close()
      catch { case e: Throwable => logWarning("close currentBatch failed", e) }
      currentBatch = null
      currentPredicateBitmap = null
    }
    if (segmentReader != null) {
      val owned = segmentReader
      segmentReader = null
      try owned.close()
      catch { case e: Throwable => logWarning("close segmentReader failed", e) }
      try finalMetrics = owned.metrics
      catch { case e: Throwable => logWarning("read final metrics failed", e) }
    }
    if (allocatorOwner != null) {
      val owned = allocatorOwner
      try {
        owned.close()
        allocatorOwner = null
      } catch {
        case e: Throwable => logWarning("close task allocator failed", e)
      }
    }
  }
}
