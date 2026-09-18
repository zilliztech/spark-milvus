package com.zilliz.spark.connector.read

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.storage.expr.{Bitmap, Expr, PredicateExpr}
import com.zilliz.milvus.storage.read.exec.{ReadMetrics, SegmentReader}
import com.zilliz.spark.connector.metrics.ScanMetrics
import com.zilliz.spark.connector.types.{ArrowAllocator, ArrowConverter}

/** The one row reader for both storage lines: Arrow batches from
  * `core.read.exec` become Spark rows. What differs between the lines is how
  * columns are named and which the query needs, and that is the
  * `ColumnBinding`'s; the loop here does not know which line it reads.
  *
  * The loop pulls a batch, evaluates table filters once, hands out its rows one
  * by one, skips excluded rows, closes the batch and pulls the next. The
  * `SegmentReader` opened by the binding owns the EOF row-count contract, so
  * row and columnar consumers cannot disagree about a short read.
  */
class MilvusRowPartitionReader(
    schema: StructType,
    setup: ColumnBinding,
    pushedExpression: Option[PredicateExpr] = None,
    allocator: BufferAllocator = ArrowAllocator.get,
    taskAllocatorOwner: Option[AutoCloseable] = None,
    private[read] val preopenedSegmentReader: Option[SegmentReader] = None,
    milvusFilter: Option[Expr] = None
) extends RowOffsetReader
    with Logging {

  private val applyDeletes: Boolean = setup.appliesDeletes
  private val arrowColumnNames: Map[String, String] = setup.arrowColumnNames

  // Native handles start out null so a constructor that throws can release
  // whatever it took; Spark only closes a reader it got back.
  private var segmentReader: SegmentReader = null
  private var allocatorOwner: AutoCloseable = taskAllocatorOwner.orNull
  private var currentBatch: VectorSchemaRoot = null
  private var currentFilterBitmap: Bitmap = null
  private var currentRowIndex: Int = 0
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
    segmentReader = preopenedSegmentReader.getOrElse(setup.open(allocator))
    loadNextBatch()
  } catch {
    case e: Throwable =>
      releaseAll()
      throw e
  }

  private def pullNextBatch(): VectorSchemaRoot =
    if (segmentReader == null) null else segmentReader.next().orNull

  private def loadNextBatch(): Unit = {
    currentBatch = pullNextBatch()
    currentFilterBitmap =
      if (currentBatch == null) null
      else
        BatchFilterEvaluator
          .exclusions(
            currentBatch,
            milvusFilter,
            pushedExpression,
            setup.arrowColumnFor,
            setup.columnNameFor
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
      (currentFilterBitmap != null &&
        currentFilterBitmap.isExcluded(rowIndex))

  override def next(): Boolean = nextRow()

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

  override def get(): InternalRow = {
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
      currentFilterBitmap = null
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
