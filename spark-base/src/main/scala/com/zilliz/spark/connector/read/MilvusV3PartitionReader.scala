package com.zilliz.spark.connector.read

import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.JavaConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StringType,
  StructField,
  StructType
}

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.read.exec.{
  SegmentReader,
  SegmentReaderRegistry
}
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.schema.SchemaMapper
import com.zilliz.spark.connector.filter.VectorBruteForceSearch
import com.zilliz.spark.connector.types.{ArrowAllocator, ArrowConverter}
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}
import com.zilliz.milvus.storage.snapshot.SegmentLayout

object MilvusV3PartitionReader {
  private[read] val TimestampColumnName = "1"

  private[read] val SystemFieldAliases: Seq[(String, Long)] = Seq(
    "RowID" -> 0L,
    "row_id" -> 0L,
    "rowid" -> 0L,
    "Timestamp" -> 1L,
    "timestamp" -> 1L
  )

  private[read] def buildFieldNameToId(
      milvusSchema: CollectionSchema
  ): Map[String, Long] = {
    val userFieldNames = milvusSchema.fields.map(_.name).toSet
    val systemFields = SystemFieldAliases.filterNot { case (alias, _) =>
      userFieldNames.contains(alias)
    }.toMap
    val userFields = milvusSchema.fields.map { field =>
      field.name -> field.fieldID
    }.toMap
    systemFields ++ userFields
  }

  private case class VectorSearchResult(
      row: InternalRow,
      distance: Double,
      rowOffset: Long
  )

  private[read] def validateVectorSearchField(
      field: StructField,
      metric: String
  ): Unit = {
    field.dataType match {
      case BinaryType
          if field.metadata.contains(
            FieldMetadata.MilvusDataTypeMetadataKey
          ) =>
        field.metadata
          .getLong(FieldMetadata.MilvusDataTypeMetadataKey)
          .toInt match {
          case DataType.BinaryVector.value =>
            throw new IllegalArgumentException(
              s"Vector column '${field.name}' uses BinaryVector storage and does not support vector.search.* dense-float metric '$metric'; use binary search utilities with Hamming/Jaccard instead"
            )
          case _ =>
        }
      case _ =>
    }
  }

  private[read] def decodeBinaryTypeVectorForSearch(
      bytes: Array[Byte],
      field: StructField
  ): Array[Float] = {
    if (!field.metadata.contains(FieldMetadata.MilvusDataTypeMetadataKey)) {
      throw new IllegalArgumentException(
        s"BinaryType vector search requires ${FieldMetadata.MilvusDataTypeMetadataKey} metadata"
      )
    }

    field.metadata
      .getLong(FieldMetadata.MilvusDataTypeMetadataKey)
      .toInt match {
      case DataType.FloatVector.value =>
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        (0 until (bytes.length / 4)).map(_ => buffer.getFloat()).toArray
      case DataType.Float16Vector.value =>
        bytes
          .grouped(2)
          .map(b => FloatConverter.fromFloat16Bytes(b.toSeq))
          .toArray
      case DataType.BFloat16Vector.value =>
        bytes
          .grouped(2)
          .map(b => FloatConverter.fromBFloat16Bytes(b.toSeq))
          .toArray
      case DataType.BinaryVector.value =>
        throw new IllegalArgumentException(
          "BinaryVector does not support vector.search.* dense-float metrics; use binary search utilities with Hamming/Jaccard instead"
        )
      case other =>
        throw new IllegalArgumentException(
          s"BinaryType vector search is unsupported for Milvus data type $other"
        )
    }
  }
}

// for Milvus 2.6+ version data source and milvus lake data
class MilvusV3PartitionReader(
    schema: StructType,
    setup: ColumnBinding,
    milvusSchema: CollectionSchema,
    milvusOption: MilvusOption,
    optionsMap: Map[String, String],
    topK: Option[Int] = None,
    queryVector: Option[Array[Float]] = None,
    metricType: Option[String] = None,
    vectorColumn: Option[String] = None,
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends RowOffsetReader
    with Logging {

  // Load native library

  // Everything storage-facing comes out of the task the driver built: the
  // manifest location, the version pinned at planning time, the validated fs.*
  // map, and the deletes.
  private val (manifestPath: String, readVersion: Long) =
    setup.task.layout match {
      case SegmentLayout.Manifest(basePath, version) => (basePath, version)
      case other =>
        throw new IllegalArgumentException(
          s"V3 reader needs a manifest layout, got $other"
        )
    }
  private val applyDeletes: Boolean = setup.appliesDeletes
  private val deletePlan: DeletePlan = setup.deletePlan

  private val allocator = ArrowAllocator.get

  private val sourceSchema = schema

  // Spark name to Arrow column name. Shared with the columnar reader through
  // the setup so the two cannot name columns differently.
  private val fieldNameToIdString: Map[String, String] = setup.arrowColumnNames

  private val pkField: Option[FieldSchema] =
    milvusSchema.fields.find(_.isPrimaryKey)
  private val pkColumnName = pkField.map(_.fieldID.toString).getOrElse("")

  private val columnNames = setup.neededColumns.toArray

  // Native resource handles. Initialized to safe defaults so a partial-init
  // failure can roll back whatever was allocated so far via releaseAll().
  // Spark only calls close() on a fully-constructed reader, so any throw
  // from the init block below has to release its own resources before
  // bubbling out.
  private var segmentReader: SegmentReader = null
  private var _currentBatch: VectorSchemaRoot = null
  private var _currentRowIndex: Int = 0
  private var _currentBatchStartRowOffset: Long = 0L
  private var _lastReturnedRowOffset: Long = -1L

  /** Row offset of the row `get()` last returned, for the row-offset metadata
    * column. -1 before the first row.
    */
  def lastReturnedRowOffset: Long = _lastReturnedRowOffset

  try {
    segmentReader = setup.open(allocator)
    _currentBatch = pullNextBatch()
  } catch {
    case e: Throwable =>
      releaseAll()
      throw e
  }

  // Pull the next batch as a freshly-owned VectorSchemaRoot, or null on EOF.
  private def pullNextBatch(): VectorSchemaRoot =
    if (segmentReader == null) null else segmentReader.next().orNull

  // Vector search state
  private val vectorSearchEnabled = topK.isDefined && queryVector.isDefined
  private var vectorSearchResults: Iterator[
    MilvusV3PartitionReader.VectorSearchResult
  ] = _
  private var vectorSearchCompleted = false

  override def next(): Boolean = {
    if (vectorSearchEnabled) {
      if (!vectorSearchCompleted) {
        performSegmentVectorSearch()
        vectorSearchCompleted = true
      }
      vectorSearchResults.hasNext
    } else {
      // Loop to find next row that passes filters
      while (true) {
        // Check if we have more rows in current batch
        if (
          _currentBatch != null && _currentRowIndex < _currentBatch.getRowCount
        ) {
          if (
            applyDeletes && !deletePlan.isEmpty && isDeleted(
              _currentBatch,
              _currentRowIndex
            )
          ) {
            _currentRowIndex += 1
          } else
          // If we have filters, check if current row passes
          if (pushedFilters.nonEmpty) {
            val row = ArrowConverter.arrowToInternalRow(
              _currentBatch,
              _currentRowIndex,
              sourceSchema,
              fieldNameToIdString
            )
            _currentRowIndex += 1
            if (applyFilters(row)) {
              // Found a matching row, back up index so get() will return it
              _currentRowIndex -= 1
              return true
            }
            // Row didn't match filters, continue to next row
          } else {
            // No filters, current row is valid
            return true
          }
        } else {
          // Try to load next batch
          if (_currentBatch != null) {
            _currentBatchStartRowOffset += _currentBatch.getRowCount
            _currentBatch.close()
            _currentBatch = null
          }
          _currentBatch = pullNextBatch()
          _currentRowIndex = 0
          if (_currentBatch == null) {
            // EOF — no more batches.
            return false
          }
          // A 0-row batch isn't EOF: fall through, the outer-loop
          // guard (_currentRowIndex < _currentBatch.getRowCount) is
          // false for rowCount=0, so the next iteration re-enters
          // this branch and pulls the following batch.
        }
      }
      false // Unreachable but needed for compilation
    }
  }

  override def get(): InternalRow = {
    if (vectorSearchEnabled) {
      // Vector search mode: return row with distance appended
      val result = vectorSearchResults.next()
      _lastReturnedRowOffset = result.rowOffset
      val rowSeq = result.row.toSeq(sourceSchema)
      val resultRow = InternalRow.fromSeq(rowSeq :+ result.distance)
      resultRow
    } else {
      // Normal mode
      if (_currentBatch == null) {
        throw new IllegalStateException("No batch loaded")
      }

      _lastReturnedRowOffset = _currentBatchStartRowOffset + _currentRowIndex
      val row = ArrowConverter.arrowToInternalRow(
        _currentBatch,
        _currentRowIndex,
        sourceSchema,
        fieldNameToIdString
      )
      _currentRowIndex += 1
      row
    }
  }

  override def close(): Unit = releaseAll()

  private def isDeleted(batch: VectorSchemaRoot, rowIndex: Int): Boolean =
    setup.isDeleted(batch, rowIndex)

  // Each native resource is released in its own try-catch so one failing
  // release doesn't strand the rest. Null/zero sentinels make this
  // idempotent — safe to call from both close() and the ctor rollback
  // path even if Spark calls close() more than once.
  private def releaseAll(): Unit = {
    if (_currentBatch != null) {
      try _currentBatch.close()
      catch { case e: Throwable => logWarning("close currentBatch failed", e) }
      _currentBatch = null
    }
    if (segmentReader != null) {
      try segmentReader.close()
      catch { case e: Throwable => logWarning("close segmentReader failed", e) }
      segmentReader = null
    }
  }

  private def createArrowSchema(): (ArrowSchema, Long) = {
    // Convert Milvus schema to Arrow schema with field IDs as field names
    // This is required because milvus-storage reader matches columns by field ID
    // The manifest stores column groups with field IDs (e.g., "100", "101")
    val arrowSchema = com.zilliz.milvus.storage.schema.SchemaMapper
      .convertToArrowSchemaWithFieldIdNames(milvusSchema)
    val arrowSchemaC = ArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, arrowSchema, null, arrowSchemaC)
    (arrowSchemaC, arrowSchemaC.memoryAddress())
  }

  /** Perform per-segment vector search and maintain top-K results
    */
  private def performSegmentVectorSearch(): Unit = {
    val k = topK.get
    val qv = queryVector.get
    val metric = metricType.getOrElse("L2")
    val vecCol = vectorColumn.getOrElse("vector")

    logInfo(
      s"Starting per-segment vector search: k=$k, metric=$metric, vectorColumn=$vecCol"
    )

    // Find vector column index in source schema
    val vectorColIndex =
      try {
        sourceSchema.fieldIndex(vecCol)
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Vector column '$vecCol' not found in schema: ${sourceSchema.fieldNames.mkString(", ")}"
          )
      }

    MilvusV3PartitionReader.validateVectorSearchField(
      sourceSchema(vectorColIndex),
      metric
    )

    // Use priority queue to maintain top-K
    // For L2: min-heap (smaller distance is better, so we keep max at top to evict)
    // For IP/COSINE: max-heap (larger score is better, so we keep min at top to evict)
    val ordering: Ordering[MilvusV3PartitionReader.VectorSearchResult] =
      metric match {
        case "L2" =>
          Ordering.by[MilvusV3PartitionReader.VectorSearchResult, Double](
            _.distance
          )
        case "IP" | "COSINE" =>
          Ordering
            .by[MilvusV3PartitionReader.VectorSearchResult, Double](
              _.distance
            )
            .reverse
        case _ =>
          Ordering.by[MilvusV3PartitionReader.VectorSearchResult, Double](
            _.distance
          )
      }

    val heap = scala.collection.mutable.PriorityQueue
      .empty[MilvusV3PartitionReader.VectorSearchResult](ordering)
    var rowCount = 0L

    // Helper function to process a batch
    def processBatch(batch: VectorSchemaRoot): Unit = {
      if (batch == null) return
      val batchSize = batch.getRowCount

      // Process each row in batch
      for (i <- 0 until batchSize) {
        if (!(applyDeletes && !deletePlan.isEmpty && isDeleted(batch, i))) {
          val row = ArrowConverter.arrowToInternalRow(
            batch,
            i,
            sourceSchema,
            fieldNameToIdString
          )

          // Extract vector from row
          val vector =
            try {
              extractVectorFromRow(
                row,
                vectorColIndex,
                sourceSchema(vectorColIndex).dataType
              )
            } catch {
              case e: Exception =>
                logWarning(
                  s"Failed to extract vector from row $rowCount: ${e.getMessage}"
                )
                null
            }

          if (vector != null) {
            val distance = calculateDistance(qv, vector, metric)
            val result = MilvusV3PartitionReader.VectorSearchResult(
              row.copy(),
              distance,
              rowCount
            )

            if (heap.size < k) {
              heap.enqueue(result)
            } else {
              val worst = heap.head
              val shouldReplace = metric match {
                case "L2"            => distance < worst.distance
                case "IP" | "COSINE" => distance > worst.distance
                case _               => distance < worst.distance
              }

              if (shouldReplace) {
                heap.dequeue()
                heap.enqueue(result)
              }
            }
          }
        }

        rowCount += 1
      }
    }

    // Process the first batch that was already loaded in constructor, then
    // drop it — each per-batch root is independently owned, so we must close
    // it before pulling the next.
    processBatch(_currentBatch)
    if (_currentBatch != null) {
      _currentBatch.close()
      _currentBatch = null
    }

    // Iterate through remaining batches
    var nextBatch = pullNextBatch()
    while (nextBatch != null) {
      try {
        processBatch(nextBatch)
      } finally {
        nextBatch.close()
      }
      nextBatch = pullNextBatch()
    }

    logInfo(
      s"Per-segment vector search completed: processed $rowCount rows, kept ${heap.size} top-K results"
    )

    val results = heap.dequeueAll
    val sortedResults = metric match {
      case "L2" =>
        results.sortBy((result: MilvusV3PartitionReader.VectorSearchResult) =>
          result.distance
        )
      case "IP" | "COSINE" =>
        results.sortBy((result: MilvusV3PartitionReader.VectorSearchResult) =>
          -result.distance
        )
      case _ =>
        results.sortBy((result: MilvusV3PartitionReader.VectorSearchResult) =>
          result.distance
        )
    }

    vectorSearchResults = sortedResults.iterator
  }

  /** Extract vector from InternalRow based on data type
    */
  private def extractVectorFromRow(
      row: InternalRow,
      colIndex: Int,
      dataType: org.apache.spark.sql.types.DataType
  ): Array[Float] = {
    dataType match {
      case ArrayType(FloatType, _) =>
        val arrayData = row.getArray(colIndex)
        (0 until arrayData.numElements())
          .map(i => arrayData.getFloat(i))
          .toArray

      case BinaryType =>
        MilvusV3PartitionReader.decodeBinaryTypeVectorForSearch(
          row.getBinary(colIndex),
          sourceSchema(colIndex)
        )

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported vector type: $dataType"
        )
    }
  }

  private def calculateDistance(
      queryVec: Array[Float],
      dataVec: Array[Float],
      metric: String
  ): Double = {
    if (queryVec.length != dataVec.length) {
      logWarning(
        s"Vector dimension mismatch: query=${queryVec.length}, data=${dataVec.length}"
      )
      return Double.MaxValue
    }

    val queryVector = Vectors.dense(queryVec.map(_.toDouble))
    val dataVector = Vectors.dense(dataVec.map(_.toDouble))

    val distanceType = metric match {
      case "L2"     => VectorBruteForceSearch.DistanceType.L2
      case "IP"     => VectorBruteForceSearch.DistanceType.IP
      case "COSINE" => VectorBruteForceSearch.DistanceType.COSINE
      case _ =>
        throw new IllegalArgumentException(s"Unsupported metric type: $metric")
    }

    VectorBruteForceSearch.calculateDistance(
      queryVector,
      dataVector,
      distanceType
    )
  }

  /** Apply all pushed filters to a row
    */
  private def applyFilters(row: InternalRow): Boolean = {
    if (pushedFilters.isEmpty) {
      return true
    }
    pushedFilters.forall(filter => evaluateFilter(filter, row))
  }

  /** Recursively evaluate a filter against a row
    */
  private def evaluateFilter(filter: Filter, row: InternalRow): Boolean = {
    import org.apache.spark.sql.sources._

    filter match {
      case EqualTo(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) == 0

      case GreaterThan(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) > 0

      case GreaterThanOrEqual(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) >= 0

      case LessThan(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) < 0

      case LessThanOrEqual(attr, value) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        compareValues(rowValue, value) <= 0

      case In(attr, values) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        val rowValue = getRowValue(row, columnIndex, attr)
        values.exists(v => compareValues(rowValue, v) == 0)

      case IsNull(attr) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        row.isNullAt(columnIndex)

      case IsNotNull(attr) =>
        val columnIndex = getColumnIndex(attr)
        if (columnIndex == -1) return true
        !row.isNullAt(columnIndex)

      case And(left, right) =>
        evaluateFilter(left, row) && evaluateFilter(right, row)

      case Or(left, right) =>
        evaluateFilter(left, row) || evaluateFilter(right, row)

      case _ =>
        // Unsupported filter, don't filter out
        true
    }
  }

  /** Get column index by name, returns -1 if not found
    */
  private def getColumnIndex(columnName: String): Int = {
    try {
      sourceSchema.fieldIndex(columnName)
    } catch {
      case _: IllegalArgumentException => -1
    }
  }

  /** Get value from row at given column index
    */
  private def getRowValue(
      row: InternalRow,
      columnIndex: Int,
      columnName: String
  ): Any = {
    if (row.isNullAt(columnIndex)) {
      return null
    }

    val field = sourceSchema.fields(columnIndex)
    field.dataType match {
      case LongType    => row.getLong(columnIndex)
      case IntegerType => row.getInt(columnIndex)
      case ShortType   => row.getShort(columnIndex)
      case FloatType   => row.getFloat(columnIndex)
      case DoubleType  => row.getDouble(columnIndex)
      case BooleanType => row.getBoolean(columnIndex)
      case StringType  => row.getUTF8String(columnIndex).toString
      case BinaryType  => row.getBinary(columnIndex)
      case _           =>
        // For complex types (arrays, maps, structs), return the raw value
        row.get(columnIndex, field.dataType)
    }
  }

  /** Compare two values, handling type conversions
    */
  private def compareValues(rowValue: Any, filterValue: Any): Int = {
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
      case _ =>
        // For other types, try toString comparison as fallback
        rowValue.toString.compareTo(filterValue.toString)
    }
  }
}
