package com.zilliz.spark.connector.read

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{
  BinaryType,
  BooleanType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StringType,
  StructType
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
  * evaluates the pushed filters on a row when there are any, closes the batch
  * and pulls the next. At EOF, when the layout states how many rows the segment
  * holds (a column-group layout does), the count delivered has to match: a
  * short read is an error, not a short DataFrame. A milvus-storage before #657
  * dropped every file after the first of a column group with no error and no
  * log, and the native cross-group row check cannot see it when every group has
  * the same file split.
  *
  * A vector search replaces the scan: on the first `next()` the whole segment
  * is scored by `SegmentVectorSearch` and the top-k come out, each row with its
  * distance appended.
  */
class MilvusRowPartitionReader(
    schema: StructType,
    setup: ColumnBinding,
    pushedFilters: Array[Filter] = Array.empty[Filter],
    vectorSearch: Option[VectorSearch] = None
) extends RowOffsetReader
    with Logging {

  private val applyDeletes: Boolean = setup.appliesDeletes
  private val arrowColumnNames: Map[String, String] = setup.arrowColumnNames
  private val allocator = ArrowAllocator.get
  private val expectedRows: Option[Long] = setup.task.expectedRows

  // Native handles start out null so a constructor that throws can release
  // whatever it took; Spark only closes a reader it got back.
  private var segmentReader: SegmentReader = null
  private var currentBatch: VectorSchemaRoot = null
  private var currentRowIndex: Int = 0
  private var currentBatchStartRowOffset: Long = 0L
  private var observedRows: Long = 0L
  private var rowCountVerified = false
  private var _lastReturnedRowOffset: Long = -1L

  private var searchResults: Iterator[SegmentVectorSearch.Result] = null

  // Rows turned into InternalRow, for the metric of that name: counted per
  // batch as the rows touched minus the ones skipped as deleted, which never
  // get converted. A filtered-out row was converted to be evaluated.
  private var materialized: Long = 0L
  private var deletedInBatch: Long = 0L
  private var finalMetrics: ReadMetrics = ReadMetrics.Zero

  /** Row offset of the row `get()` last returned, for the row-offset metadata
    * column; -1 before the first row.
    */
  def lastReturnedRowOffset: Long = _lastReturnedRowOffset

  try {
    segmentReader = setup.open(allocator)
    currentBatch = pullNextBatch()
  } catch {
    case e: Throwable =>
      releaseAll()
      throw e
  }

  private def pullNextBatch(): VectorSchemaRoot =
    if (segmentReader == null) null else segmentReader.next().orNull

  /** The batches from the one already pulled onward; each is owned by whoever
    * takes it.
    */
  private def remainingBatches(): Iterator[VectorSchemaRoot] = {
    val first = currentBatch
    currentBatch = null
    Iterator.single(first).filter(_ != null) ++
      Iterator.continually(pullNextBatch()).takeWhile(_ != null)
  }

  private def isDeleted(batch: VectorSchemaRoot, rowIndex: Int): Boolean =
    applyDeletes && !setup.deletePlan.isEmpty && setup.isDeleted(
      batch,
      rowIndex
    )

  override def next(): Boolean = vectorSearch match {
    case Some(search) =>
      if (searchResults == null) {
        val result = SegmentVectorSearch.run(
          search,
          schema,
          arrowColumnNames,
          remainingBatches(),
          isDeleted
        )
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
        currentBatchStartRowOffset += exhausted.getRowCount.toLong
        observedRows += exhausted.getRowCount.toLong
        materialized += exhausted.getRowCount.toLong - deletedInBatch
        deletedInBatch = 0L
        exhausted.close()
        currentBatch = pullNextBatch()
        currentRowIndex = 0
      }
      if (currentBatch == null) {
        verifyRowCount()
        return false
      }
      if (isDeleted(currentBatch, currentRowIndex)) {
        currentRowIndex += 1
        deletedInBatch += 1
      } else if (pushedFilters.isEmpty) {
        return true
      } else {
        val row = ArrowConverter.arrowToInternalRow(
          currentBatch,
          currentRowIndex,
          schema,
          arrowColumnNames
        )
        if (RowFilters.matches(pushedFilters, row, schema)) return true
        currentRowIndex += 1
      }
    }
    false
  }

  override def get(): InternalRow = vectorSearch match {
    case Some(_) =>
      val result = searchResults.next()
      _lastReturnedRowOffset = result.rowOffset
      InternalRow.fromSeq(result.row.toSeq(schema) :+ result.distance)
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
      row
  }

  override def close(): Unit = releaseAll()

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val metrics =
      if (segmentReader != null) segmentReader.metrics else finalMetrics
    ScanMetrics.taskValues(metrics, materialized)
  }

  /** Runs once, at EOF of a scan; a close before EOF skips it on purpose. */
  private def verifyRowCount(): Unit = {
    if (rowCountVerified) return
    rowCountVerified = true
    expectedRows.foreach { expected =>
      if (observedRows != expected) {
        throw new IllegalStateException(
          s"segment ${setup.task.segmentId} delivered $observedRows rows, its layout states $expected " +
            "(sum of per-file row counts); refusing to return a short DataFrame. " +
            "A milvus-storage before milvus-storage#657 drops every file after " +
            "the first of a column group"
        )
      }
    }
  }

  // Each handle is released on its own so one failure does not strand the
  // rest; the null sentinels make a second close a no-op.
  private def releaseAll(): Unit = {
    if (currentBatch != null) {
      // Rows touched in the batch being abandoned, less the deleted ones.
      materialized += currentRowIndex.toLong - deletedInBatch
      deletedInBatch = 0L
      try currentBatch.close()
      catch { case e: Throwable => logWarning("close currentBatch failed", e) }
      currentBatch = null
    }
    if (segmentReader != null) {
      try segmentReader.close()
      catch { case e: Throwable => logWarning("close segmentReader failed", e) }
      finalMetrics = segmentReader.metrics
      segmentReader = null
    }
  }
}

/** Evaluates Spark's DataSource V1 `Filter`s on a row, the way the row reader
  * always has. A filter on a column not in the schema and a filter kind not
  * listed here pass the row; Spark re-evaluates every filter it pushed, so a
  * pass here is never a wrong result. Decision 20 is about replacing these with
  * the V2 predicates.
  */
private[read] object RowFilters {

  def matches(
      filters: Array[Filter],
      row: InternalRow,
      schema: StructType
  ): Boolean =
    filters.forall(evaluate(_, row, schema))

  def evaluate(
      filter: Filter,
      row: InternalRow,
      schema: StructType
  ): Boolean = {
    import org.apache.spark.sql.sources._
    def index(attr: String): Int =
      try schema.fieldIndex(attr)
      catch { case _: IllegalArgumentException => -1 }
    def compareAt(attr: String, value: Any)(test: Int => Boolean): Boolean = {
      val i = index(attr)
      i == -1 || test(compareValues(valueAt(row, i, schema), value))
    }
    filter match {
      case EqualTo(attr, value)            => compareAt(attr, value)(_ == 0)
      case GreaterThan(attr, value)        => compareAt(attr, value)(_ > 0)
      case GreaterThanOrEqual(attr, value) => compareAt(attr, value)(_ >= 0)
      case LessThan(attr, value)           => compareAt(attr, value)(_ < 0)
      case LessThanOrEqual(attr, value)    => compareAt(attr, value)(_ <= 0)
      case In(attr, values) =>
        val i = index(attr)
        i == -1 || {
          val v = valueAt(row, i, schema)
          values.exists(compareValues(v, _) == 0)
        }
      case IsNull(attr) =>
        val i = index(attr)
        i == -1 || row.isNullAt(i)
      case IsNotNull(attr) =>
        val i = index(attr)
        i == -1 || !row.isNullAt(i)
      case And(left, right) =>
        evaluate(left, row, schema) && evaluate(right, row, schema)
      case Or(left, right) =>
        evaluate(left, row, schema) || evaluate(right, row, schema)
      case _ => true
    }
  }

  private def valueAt(row: InternalRow, i: Int, schema: StructType): Any =
    if (row.isNullAt(i)) null
    else
      schema.fields(i).dataType match {
        case LongType    => row.getLong(i)
        case IntegerType => row.getInt(i)
        case ShortType   => row.getShort(i)
        case FloatType   => row.getFloat(i)
        case DoubleType  => row.getDouble(i)
        case BooleanType => row.getBoolean(i)
        case StringType  => row.getUTF8String(i).toString
        case BinaryType  => row.getBinary(i)
        case other       => row.get(i, other)
      }

  private def compareValues(rowValue: Any, filterValue: Any): Int =
    (rowValue, filterValue) match {
      case (null, null)               => 0
      case (null, _)                  => -1
      case (_, null)                  => 1
      case (rv: Long, fv: Long)       => rv.compareTo(fv)
      case (rv: Long, fv: Int)        => rv.compareTo(fv.toLong)
      case (rv: Int, fv: Int)         => rv.compareTo(fv)
      case (rv: Int, fv: Long)        => rv.toLong.compareTo(fv)
      case (rv: Short, fv: Short)     => rv.compareTo(fv)
      case (rv: Short, fv: Int)       => rv.toInt.compareTo(fv)
      case (rv: Float, fv: Float)     => rv.compareTo(fv)
      case (rv: Float, fv: Double)    => rv.toDouble.compareTo(fv)
      case (rv: Double, fv: Double)   => rv.compareTo(fv)
      case (rv: Double, fv: Float)    => rv.compareTo(fv.toDouble)
      case (rv: Boolean, fv: Boolean) => rv.compareTo(fv)
      case (rv: String, fv: String)   => rv.compareTo(fv)
      case (rv: Array[Byte], fv: Array[Byte]) =>
        java.util.Arrays.compare(rv, fv)
      case _ => rowValue.toString.compareTo(filterValue.toString)
    }
}
