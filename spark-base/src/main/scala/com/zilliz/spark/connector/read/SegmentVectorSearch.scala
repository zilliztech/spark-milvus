package com.zilliz.spark.connector.read

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  FloatType,
  StructField,
  StructType
}

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.spark.connector.filter.VectorBruteForceSearch
import com.zilliz.spark.connector.options.VectorSearch
import com.zilliz.spark.connector.types.ArrowConverter
import io.milvus.grpc.schema.DataType

/** The per-segment brute-force vector search a row read runs instead of a scan
  * when `vector.search.*` options are set: every row of the segment is scored
  * against the query vector and the top-k come out in metric order, each with
  * its distance and row offset. Pushed filters do not apply on this path. Where
  * this stage finally belongs is decision 16; until then it is the optional
  * stage of `MilvusRowPartitionReader`.
  */
private[read] object SegmentVectorSearch extends Logging {

  final case class Result(row: InternalRow, distance: Double, rowOffset: Long)

  /** Scores every batch `batches` yields, closing each, and returns the top-k
    * in metric order. Deleted rows are skipped but still count towards the row
    * offset.
    */
  def run(
      search: VectorSearch,
      schema: StructType,
      arrowColumnNames: Map[String, String],
      batches: Iterator[VectorSchemaRoot],
      isDeleted: (VectorSchemaRoot, Int) => Boolean
  ): Iterator[Result] = {
    val k = search.topK
    val metric = search.metricType
    val vectorColIndex =
      try schema.fieldIndex(search.vectorColumn)
      catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Vector column '${search.vectorColumn}' not found in schema: ${schema.fieldNames.mkString(", ")}"
          )
      }
    validateVectorSearchField(schema(vectorColIndex), metric)
    logInfo(
      s"Starting per-segment vector search: k=$k, metric=$metric, vectorColumn=${search.vectorColumn}"
    )

    // The heap keeps the worst of the k on top, so L2 (smaller is better)
    // orders by distance and IP/COSINE (larger is better) by its reverse.
    val byDistance = Ordering.by[Result, Double](_.distance)
    val larger = metric == "IP" || metric == "COSINE"
    val heap = scala.collection.mutable.PriorityQueue
      .empty[Result](if (larger) byDistance.reverse else byDistance)
    var rowCount = 0L

    batches.foreach { batch =>
      try {
        var i = 0
        while (i < batch.getRowCount) {
          if (!isDeleted(batch, i)) {
            val row = ArrowConverter.arrowToInternalRow(
              batch,
              i,
              schema,
              arrowColumnNames
            )
            val vector =
              try extractVector(row, vectorColIndex, schema(vectorColIndex))
              catch {
                case e: Exception =>
                  logWarning(
                    s"Failed to extract vector from row $rowCount: ${e.getMessage}"
                  )
                  null
              }
            if (vector != null) {
              val distance =
                calculateDistance(search.queryVector, vector, metric)
              val result = Result(row.copy(), distance, rowCount)
              if (heap.size < k) heap.enqueue(result)
              else {
                val worst = heap.head.distance
                if (if (larger) distance > worst else distance < worst) {
                  heap.dequeue()
                  heap.enqueue(result)
                }
              }
            }
          }
          rowCount += 1
          i += 1
        }
      } finally batch.close()
    }
    logInfo(
      s"Per-segment vector search completed: processed $rowCount rows, kept ${heap.size} top-K results"
    )
    val results = heap.dequeueAll
    (if (larger) results.sortBy(-_.distance)
     else results.sortBy(_.distance)).iterator
  }

  private def extractVector(
      row: InternalRow,
      colIndex: Int,
      field: StructField
  ): Array[Float] =
    field.dataType match {
      case ArrayType(FloatType, _) =>
        val arrayData = row.getArray(colIndex)
        (0 until arrayData.numElements()).map(arrayData.getFloat).toArray
      case BinaryType =>
        decodeBinaryTypeVectorForSearch(row.getBinary(colIndex), field)
      case other =>
        throw new IllegalArgumentException(s"Unsupported vector type: $other")
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
    val distanceType = metric match {
      case "L2"     => VectorBruteForceSearch.DistanceType.L2
      case "IP"     => VectorBruteForceSearch.DistanceType.IP
      case "COSINE" => VectorBruteForceSearch.DistanceType.COSINE
      case _ =>
        throw new IllegalArgumentException(s"Unsupported metric type: $metric")
    }
    VectorBruteForceSearch.calculateDistance(
      Vectors.dense(queryVec.map(_.toDouble)),
      Vectors.dense(dataVec.map(_.toDouble)),
      distanceType
    )
  }

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
