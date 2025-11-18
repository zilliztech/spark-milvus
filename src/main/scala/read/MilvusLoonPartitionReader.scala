package com.zilliz.spark.connector.read

import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.spark.internal.Logging
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
  StructType
}
import org.apache.spark.ml.linalg.Vectors

import io.milvus.grpc.schema.CollectionSchema
import com.zilliz.spark.connector.MilvusOption
import com.zilliz.spark.connector.loon.Properties
import com.zilliz.spark.connector.serde.ArrowConverter
import io.milvus.storage.{ArrowUtils, NativeLibraryLoader, MilvusStorageReader}
import com.zilliz.spark.connector.filter.VectorBruteForceSearch
import org.apache.arrow.c.{ArrowArrayStream, ArrowSchema, Data}
import org.apache.arrow.vector.VectorSchemaRoot

// for Milvus 2.6+ version data source and milvus lake data
class MilvusLoonPartitionReader(
    schema: StructType,
    manifestJson: String,
    milvusSchema: CollectionSchema,
    milvusOption: MilvusOption,
    optionsMap: Map[String, String],
    topK: Option[Int] = None,
    queryVector: Option[Array[Float]] = None,
    metricType: Option[String] = None,
    vectorColumn: Option[String] = None,
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends PartitionReader[InternalRow] with Logging {

  // Load native library
  NativeLibraryLoader.loadLibrary()

  private val allocator = ArrowUtils.getAllocator

  private val sourceSchema = schema

  // Create Arrow schema from Milvus schema
  private val arrowSchemaC = createArrowSchema()

  // Create reader properties from MilvusOption
  private val readerProperties = Properties.fromMilvusOption(milvusOption)

  // Determine which columns to read
  private val columnNames = getColumnNames()

  // Create Storage V2 reader
  private val reader = new MilvusStorageReader()
  reader.create(manifestJson, arrowSchemaC, columnNames, readerProperties)

  if (!reader.isValid) {
    throw new IllegalStateException("Failed to create MilvusStorageReader")
  }

  // Get Arrow stream
  private val recordBatchReaderPtr = reader.getRecordBatchReaderScala()
  private val arrowArrayStream = ArrowArrayStream.wrap(recordBatchReaderPtr)
  private val arrowReader = Data.importArrayStream(allocator, arrowArrayStream)

  private var currentBatch: VectorSchemaRoot = _
  private var currentRowIndex: Int = 0

  // Vector search state
  private val vectorSearchEnabled = topK.isDefined && queryVector.isDefined
  private var vectorSearchResults: Iterator[(InternalRow, Double)] = _
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
        if (currentBatch != null && currentRowIndex < currentBatch.getRowCount) {
          // If we have filters, check if current row passes
          if (pushedFilters.nonEmpty) {
            val row = ArrowConverter.arrowToInternalRow(currentBatch, currentRowIndex, sourceSchema)
            currentRowIndex += 1
            if (applyFilters(row)) {
              // Found a matching row, back up index so get() will return it
              currentRowIndex -= 1
              return true
            }
            // Row didn't match filters, continue to next row
          } else {
            // No filters, current row is valid
            return true
          }
        } else {
          // Try to load next batch
          if (arrowReader.loadNextBatch()) {
            currentBatch = arrowReader.getVectorSchemaRoot
            currentRowIndex = 0
            if (currentBatch.getRowCount > 0) {
              // Continue loop to check first row of new batch
            } else {
              return false
            }
          } else {
            // No more batches
            return false
          }
        }
      }
      false // Unreachable but needed for compilation
    }
  }

  override def get(): InternalRow = {
    if (vectorSearchEnabled) {
      // Vector search mode: return row with distance appended
      val (row, distance) = vectorSearchResults.next()
      val rowSeq = row.toSeq(sourceSchema)
      val resultRow = InternalRow.fromSeq(rowSeq :+ distance)
      resultRow
    } else {
      // Normal mode
      if (currentBatch == null) {
        throw new IllegalStateException("No batch loaded")
      }

      val row = ArrowConverter.arrowToInternalRow(currentBatch, currentRowIndex, sourceSchema)
      currentRowIndex += 1
      row
    }
  }

  override def close(): Unit = {
    try {
      if (arrowReader != null) arrowReader.close()
      if (arrowArrayStream != null) arrowArrayStream.close()
      if (reader != null) reader.destroy()
      ArrowUtils.releaseArrowSchema(arrowSchemaC)
      readerProperties.free()
    } catch {
      case e: Exception =>
        logWarning("Error closing Storage V2 reader", e)
    }
  }

  private def createArrowSchema(): Long = {
    // Convert Milvus schema to Arrow schema
    val arrowSchemaObj = com.zilliz.spark.connector.MilvusSchemaUtil.convertToArrowSchema(milvusSchema)
    val arrowSchemaC = ArrowSchema.allocateNew(allocator)
    Data.exportSchema(allocator, arrowSchemaObj, null, arrowSchemaC)
    arrowSchemaC.memoryAddress()
  }

  private def getColumnNames(): Array[String] = {
    // Use source schema (which excludes computed distance columns)
    sourceSchema.fieldNames
  }

  /**
   * Perform per-segment vector search and maintain top-K results
   */
  private def performSegmentVectorSearch(): Unit = {
    val k = topK.get
    val qv = queryVector.get
    val metric = metricType.getOrElse("L2")
    val vecCol = vectorColumn.getOrElse("vector")

    logInfo(s"Starting per-segment vector search: k=$k, metric=$metric, vectorColumn=$vecCol")

    // Find vector column index in source schema
    val vectorColIndex = try {
      sourceSchema.fieldIndex(vecCol)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Vector column '$vecCol' not found in schema: ${sourceSchema.fieldNames.mkString(", ")}")
    }

    // Use priority queue to maintain top-K
    // For L2: min-heap (smaller distance is better, so we keep max at top to evict)
    // For IP/COSINE: max-heap (larger score is better, so we keep min at top to evict)
    val ordering: Ordering[(InternalRow, Double)] = metric match {
      case "L2" => Ordering.by[(InternalRow, Double), Double](_._2)  // Max-heap for L2
      case "IP" | "COSINE" => Ordering.by[(InternalRow, Double), Double](_._2).reverse  // Min-heap for IP/COSINE
      case _ => Ordering.by[(InternalRow, Double), Double](_._2)
    }

    val heap = scala.collection.mutable.PriorityQueue.empty[(InternalRow, Double)](ordering)
    var rowCount = 0

    // Iterate through all batches
    while (arrowReader.loadNextBatch()) {
      currentBatch = arrowReader.getVectorSchemaRoot
      val batchSize = currentBatch.getRowCount

      // Process each row in batch
      for (i <- 0 until batchSize) {
        val row = ArrowConverter.arrowToInternalRow(currentBatch, i, sourceSchema)

        // Extract vector from row
        val vector = try {
          extractVectorFromRow(row, vectorColIndex, sourceSchema(vectorColIndex).dataType)
        } catch {
          case e: Exception =>
            logWarning(s"Failed to extract vector from row $rowCount: ${e.getMessage}")
            null
        }

        if (vector != null) {
          // Calculate distance
          val distance = calculateDistance(qv, vector, metric)

          // Maintain top-K heap
          if (heap.size < k) {
            heap.enqueue((row.copy(), distance))
          } else {
            val (_, worstDist) = heap.head
            val shouldReplace = metric match {
              case "L2" => distance < worstDist  // Smaller L2 distance is better
              case "IP" | "COSINE" => distance > worstDist  // Larger IP/COSINE is better
              case _ => distance < worstDist
            }

            if (shouldReplace) {
              heap.dequeue()
              heap.enqueue((row.copy(), distance))
            }
          }
        }

        rowCount += 1
      }
    }

    logInfo(s"Per-segment vector search completed: processed $rowCount rows, kept ${heap.size} top-K results")

    // Sort results and create iterator
    val sortedResults = metric match {
      case "L2" => heap.dequeueAll.sortBy[(Double)](x => x._2)  // Ascending for L2
      case "IP" | "COSINE" => heap.dequeueAll.sortBy[(Double)](x => -x._2)  // Descending for IP/COSINE
      case _ => heap.dequeueAll.sortBy[(Double)](x => x._2)
    }

    vectorSearchResults = sortedResults.iterator
  }

  /**
   * Extract vector from InternalRow based on data type
   */
  private def extractVectorFromRow(row: InternalRow, colIndex: Int, dataType: org.apache.spark.sql.types.DataType): Array[Float] = {
    dataType match {
      case ArrayType(FloatType, _) =>
        // Array[Float] type
        val arrayData = row.getArray(colIndex)
        (0 until arrayData.numElements()).map(i => arrayData.getFloat(i)).toArray

      case BinaryType =>
        // Binary type (for FixedSizeBinary float vectors)
        val bytes = row.getBinary(colIndex)
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        (0 until (bytes.length / 4)).map(_ => buffer.getFloat()).toArray

      case _ =>
        throw new IllegalArgumentException(s"Unsupported vector type: $dataType")
    }
  }

  private def calculateDistance(queryVec: Array[Float], dataVec: Array[Float], metric: String): Double = {
    if (queryVec.length != dataVec.length) {
      logWarning(s"Vector dimension mismatch: query=${queryVec.length}, data=${dataVec.length}")
      return Double.MaxValue
    }

    val queryVector = Vectors.dense(queryVec.map(_.toDouble))
    val dataVector = Vectors.dense(dataVec.map(_.toDouble))

    val distanceType = metric match {
      case "L2" => VectorBruteForceSearch.DistanceType.L2
      case "IP" => VectorBruteForceSearch.DistanceType.IP
      case "COSINE" => VectorBruteForceSearch.DistanceType.COSINE
      case _ => throw new IllegalArgumentException(s"Unsupported metric type: $metric")
    }

    VectorBruteForceSearch.calculateDistance(queryVector, dataVector, distanceType)
  }

  /**
   * Apply all pushed filters to a row
   */
  private def applyFilters(row: InternalRow): Boolean = {
    if (pushedFilters.isEmpty) {
      return true
    }
    pushedFilters.forall(filter => evaluateFilter(filter, row))
  }

  /**
   * Recursively evaluate a filter against a row
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

  /**
   * Get column index by name, returns -1 if not found
   */
  private def getColumnIndex(columnName: String): Int = {
    try {
      sourceSchema.fieldIndex(columnName)
    } catch {
      case _: IllegalArgumentException => -1
    }
  }

  /**
   * Get value from row at given column index
   */
  private def getRowValue(row: InternalRow, columnIndex: Int, columnName: String): Any = {
    if (row.isNullAt(columnIndex)) {
      return null
    }

    val field = sourceSchema.fields(columnIndex)
    field.dataType match {
      case LongType => row.getLong(columnIndex)
      case IntegerType => row.getInt(columnIndex)
      case ShortType => row.getShort(columnIndex)
      case FloatType => row.getFloat(columnIndex)
      case DoubleType => row.getDouble(columnIndex)
      case BooleanType => row.getBoolean(columnIndex)
      case StringType => row.getUTF8String(columnIndex).toString
      case BinaryType => row.getBinary(columnIndex)
      case _ =>
        // For complex types (arrays, maps, structs), return the raw value
        row.get(columnIndex, field.dataType)
    }
  }

  /**
   * Compare two values, handling type conversions
   */
  private def compareValues(rowValue: Any, filterValue: Any): Int = {
    (rowValue, filterValue) match {
      case (null, null) => 0
      case (null, _) => -1
      case (_, null) => 1
      case (rv: Long, fv: Long) => rv.compareTo(fv)
      case (rv: Long, fv: Int) => rv.compareTo(fv.toLong)
      case (rv: Int, fv: Int) => rv.compareTo(fv)
      case (rv: Int, fv: Long) => rv.toLong.compareTo(fv)
      case (rv: Short, fv: Short) => rv.compareTo(fv)
      case (rv: Short, fv: Int) => rv.toInt.compareTo(fv)
      case (rv: Float, fv: Float) => rv.compareTo(fv)
      case (rv: Float, fv: Double) => rv.toDouble.compareTo(fv)
      case (rv: Double, fv: Double) => rv.compareTo(fv)
      case (rv: Double, fv: Float) => rv.compareTo(fv.toDouble)
      case (rv: Boolean, fv: Boolean) => rv.compareTo(fv)
      case (rv: String, fv: String) => rv.compareTo(fv)
      case (rv: Array[Byte], fv: Array[Byte]) =>
        java.util.Arrays.compare(rv, fv)
      case _ =>
        // For other types, try toString comparison as fallback
        rowValue.toString.compareTo(filterValue.toString)
    }
  }
}