package com.zilliz.spark.connector.read

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.expr.{
  ExclusionBitmap,
  PredicateEvaluator,
  PredicateExpr
}
import com.zilliz.milvus.storage.read.exec.{ReadMetrics, SegmentReader}
import com.zilliz.spark.connector.metrics.{
  ReadEndReason,
  ReadFailurePhase,
  ReadProgress,
  ScanMetrics
}
import com.zilliz.spark.connector.types.{ArrowAllocator, ArrowConverter}

/** The one row reader for both storage lines: Arrow batches from
  * `core.read.exec` become Spark rows. What differs between the lines is how
  * columns are named and which the query needs, and that is the
  * `ColumnBinding`'s; the loop here does not know which line it reads.
  *
  * The loop pulls a batch, evaluates the pushed predicate once, hands out its
  * rows one by one, skips excluded rows, closes the batch and pulls the next.
  * The `SegmentReader` opened by the binding owns the EOF row-count contract,
  * so row and columnar consumers cannot disagree about a short read.
  */
class MilvusRowPartitionReader(
    schema: StructType,
    setup: ColumnBinding,
    pushedExpression: Option[PredicateExpr] = None,
    allocator: BufferAllocator = ArrowAllocator.get,
    taskAllocatorOwner: Option[AutoCloseable] = None,
    private[read] val preopenedSegmentReader: Option[SegmentReader] = None,
    private[read] val progress: ReadProgress = new ReadProgress
) extends RowOffsetReader
    with Logging {

  private val arrowColumnNames: Map[String, String] = setup.arrowColumnNames

  // Native handles start out null so a constructor that throws can release
  // whatever it took; Spark only closes a reader it got back.
  private var segmentReader: SegmentReader = null
  private var allocatorOwner: AutoCloseable = taskAllocatorOwner.orNull
  private var currentBatch: VectorSchemaRoot = null
  private var currentFilterBitmap: ExclusionBitmap = null
  private var currentRowIndex: Int = 0
  private var selectedRowIndex: Int = -1
  private var returnedRow: InternalRow = null
  private var closed = false
  private var currentBatchStartRowOffset: Long = 0L
  private var _lastReturnedRowOffset: Long = -1L

  // Rows actually turned into InternalRow. Predicate evaluation stays on Arrow
  // vectors, so neither deleted nor filtered-out rows contribute.
  private var materialized: Long = 0L
  private var finalMetrics: ReadMetrics = ReadMetrics.Zero

  /** Row offset of the row `get()` last returned, for the row-offset metadata
    * column; -1 before the first row.
    */
  def lastReturnedRowOffset: Long = _lastReturnedRowOffset

  try {
    segmentReader = progress.inPhase(ReadFailurePhase.Open) {
      preopenedSegmentReader.getOrElse {
        progress.inPhase(ReadFailurePhase.DeleteLoad)(setup.deletePlan)
        setup.open(allocator)
      }
    }
    loadNextBatch()
  } catch {
    case e: Throwable =>
      try releaseAll()
      catch {
        case closeFailure: Throwable =>
          progress.failed(ReadFailurePhase.Close, closeFailure)
          if (closeFailure ne e) e.addSuppressed(closeFailure)
      }
      progress.close()
      throw e
  }

  private def pullNextBatch(): VectorSchemaRoot =
    if (segmentReader == null) null
    else progress.inPhase(ReadFailurePhase.Read)(segmentReader.next().orNull)

  private def loadNextBatch(): Unit = {
    currentBatch = pullNextBatch()
    if (currentBatch == null) progress.end(ReadEndReason.Exhausted)
    else progress.decodedRows(currentBatch.getRowCount.toLong)
    currentFilterBitmap =
      if (currentBatch == null) null
      else
        progress.inPhase(ReadFailurePhase.Filter) {
          pushedExpression
            .map(
              PredicateEvaluator.evaluate(_, currentBatch, setup.columnNameFor)
            )
            .orNull
        }
  }

  private def isExcluded(batch: VectorSchemaRoot, rowIndex: Int): Boolean =
    progress.inPhase(ReadFailurePhase.Filter) {
      if (setup.isDeleted(batch, rowIndex)) {
        progress.excludedRows(1L, 0L)
        true
      } else if (
        currentFilterBitmap != null && currentFilterBitmap.isExcluded(rowIndex)
      ) {
        progress.excludedRows(0L, 1L)
        true
      } else false
    }

  override def next(): Boolean = {
    if (closed) return false
    selectedRowIndex = -1
    returnedRow = null
    nextRow()
  }

  private def nextRow(): Boolean = {
    while (true) {
      // A 0-row batch is not EOF; only a null batch is.
      while (
        currentBatch != null && currentRowIndex >= currentBatch.getRowCount
      ) {
        val exhausted = currentBatch
        currentBatch = null
        currentFilterBitmap = null
        currentBatchStartRowOffset += exhausted.getRowCount.toLong
        progress.inPhase(ReadFailurePhase.Close)(exhausted.close())
        loadNextBatch()
        currentRowIndex = 0
      }
      if (currentBatch == null) {
        return false
      }
      val candidate = currentRowIndex
      currentRowIndex += 1
      if (!isExcluded(currentBatch, candidate)) {
        selectedRowIndex = candidate
        return true
      }
    }
    false
  }

  override def get(): InternalRow = {
    if (closed || currentBatch == null || selectedRowIndex < 0) {
      throw new IllegalStateException(
        "No current row; call next() before get()"
      )
    }
    if (returnedRow != null) return returnedRow
    _lastReturnedRowOffset = currentBatchStartRowOffset + selectedRowIndex
    returnedRow = progress.inPhase(ReadFailurePhase.Convert) {
      ArrowConverter.arrowToInternalRow(
        currentBatch,
        selectedRowIndex,
        schema,
        arrowColumnNames
      )
    }
    materialized += 1L
    returnedRow
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      try progress.inPhase(ReadFailurePhase.Close)(releaseAll())
      finally progress.close()
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val metrics =
      if (segmentReader != null) segmentReader.metrics else finalMetrics
    ScanMetrics.taskValues(metrics, materialized) ++ progress.taskValues
  }

  // Each handle is released on its own so one failure does not strand the
  // rest; the null sentinels make a second close a no-op.
  private def releaseAll(): Unit = {
    var failure: Throwable = null
    def closeResource(action: => Unit): Unit =
      try action
      catch {
        case next: Throwable =>
          progress.failed(ReadFailurePhase.Close, next)
          if (failure == null) failure = next
          else if (failure ne next) failure.addSuppressed(next)
      }
    if (currentBatch != null) {
      closeResource(currentBatch.close())
      currentBatch = null
      currentFilterBitmap = null
    }
    if (segmentReader != null) {
      val owned = segmentReader
      segmentReader = null
      closeResource(owned.close())
      closeResource { finalMetrics = owned.metrics }
    }
    if (allocatorOwner != null) {
      val owned = allocatorOwner
      allocatorOwner = null
      closeResource(owned.close())
    }
    if (failure != null) throw failure
  }
}
