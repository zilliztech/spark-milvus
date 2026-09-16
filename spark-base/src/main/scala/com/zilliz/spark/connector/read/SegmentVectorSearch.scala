package com.zilliz.spark.connector.read

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.arrow.vector.{
  FixedSizeBinaryVector,
  VarBinaryVector,
  VectorSchemaRoot
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  FloatType,
  StructField,
  StructType
}

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.index.BruteForceSearch
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.spark.connector.options.VectorSearch
import com.zilliz.spark.connector.types.ArrowConverter
import io.milvus.grpc.schema.DataType

/** Adapts `vector.search.*` to core's per-segment Knowhere brute-force search:
  * converts the vector column, excludes deleted/null vectors and copies rows
  * retained by the native TopK. Pushed filters do not apply on this path. The
  * collection-level entry point is decision 16; this remains an optional stage
  * of `MilvusRowPartitionReader`.
  */
private[read] object SegmentVectorSearch extends Logging {

  final case class Result(row: InternalRow, distance: Double, rowOffset: Long)

  /** The top-k in metric order, and the number of candidate rows copied from
    * Arrow into their complete output `InternalRow` during native search.
    */
  final case class Search(results: Iterator[Result], rowsMaterialized: Long)

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
  ): Search = {
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
    val vectorField = schema(vectorColIndex)
    validateVectorSearchField(vectorField, metric)
    if (
      vectorField.metadata.contains(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      )
    ) {
      val dimension = vectorField.metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      )
      require(
        search.queryVector != null && search.queryVector.length.toLong == dimension,
        s"Query vector dimension must match vector column '${vectorField.name}' dimension $dimension"
      )
    }
    logInfo(
      s"Starting per-segment vector search: k=$k, metric=$metric, vectorColumn=${search.vectorColumn}"
    )

    val arrowVectorName =
      arrowColumnNames.getOrElse(vectorField.name, vectorField.name)
    val computation =
      new BruteForceSearch[InternalRow](search.queryVector, k, metric)
    var rowCount = 0L
    var materialized = 0L
    try {
      batches.foreach { batch =>
        try {
          val arrowVector = batch.getVector(arrowVectorName)
          require(
            arrowVector != null,
            s"Vector column '${vectorField.name}' is missing Arrow column '$arrowVectorName'"
          )
          // Keep binary payloads intact until their byte length is validated;
          // generic float-array conversion may discard a trailing partial value.
          val readField = arrowVector match {
            case _: VarBinaryVector | _: FixedSizeBinaryVector =>
              vectorField.copy(dataType = BinaryType)
            case _ => vectorField
          }
          val vectorSchema = StructType(Seq(readField))
          computation.addBatch(batch.getRowCount)(
            i =>
              if (isDeleted(batch, i) || arrowVector.isNull(i)) null
              else
                extractVector(
                  ArrowConverter.arrowToInternalRow(
                    batch,
                    i,
                    vectorSchema,
                    arrowColumnNames
                  ),
                  0,
                  readField
                ),
            i => {
              materialized += 1
              ArrowConverter
                .arrowToInternalRow(batch, i, schema, arrowColumnNames)
                .copy()
            }
          )
          rowCount = Math.addExact(rowCount, batch.getRowCount.toLong)
        } finally batch.close()
      }
      val results = computation.results
      logInfo(
        s"Per-segment vector search completed: processed $rowCount rows, kept ${results.size} top-K results"
      )
      Search(
        results.iterator.map { hit =>
          val score =
            if (search.mode != "index" && metric == "L2")
              math.sqrt(hit.distance)
            else hit.distance
          Result(hit.value, score, hit.rowOffset)
        },
        materialized
      )
    } finally computation.close()
  }

  private[read] def extractVector(
      row: InternalRow,
      colIndex: Int,
      field: StructField
  ): Array[Float] = {
    if (row.isNullAt(colIndex)) return null
    field.dataType match {
      case ArrayType(FloatType, _) =>
        val arrayData = row.getArray(colIndex)
        Array.tabulate(arrayData.numElements()) { index =>
          require(
            !arrayData.isNullAt(index),
            s"Vector column '${field.name}' contains a null element at index $index"
          )
          arrayData.getFloat(index)
        }
      case BinaryType =>
        decodeBinaryTypeVectorForSearch(row.getBinary(colIndex), field)
      case other =>
        throw new IllegalArgumentException(s"Unsupported vector type: $other")
    }
  }

  private[read] def validateVectorSearchField(
      field: StructField,
      metric: String
  ): Unit = {
    require(
      Set("L2", "IP", "COSINE").contains(metric),
      s"Unsupported metric type: $metric"
    )
    field.dataType match {
      case ArrayType(FloatType, _) =>
      case BinaryType =>
        require(
          field.metadata.contains(FieldMetadata.MilvusDataTypeMetadataKey),
          s"BinaryType vector search requires ${FieldMetadata.MilvusDataTypeMetadataKey} metadata"
        )
        field.metadata
          .getLong(FieldMetadata.MilvusDataTypeMetadataKey)
          .toInt match {
          case DataType.BinaryVector.value =>
            throw new IllegalArgumentException(
              s"Vector column '${field.name}' uses BinaryVector storage and does not support vector.search.* dense-float metric '$metric'; use binary search utilities with Hamming/Jaccard instead"
            )
          case DataType.FloatVector.value | DataType.Float16Vector.value |
              DataType.BFloat16Vector.value =>
          case other =>
            throw new IllegalArgumentException(
              s"BinaryType vector search is unsupported for Milvus data type $other"
            )
        }
      case other =>
        throw new IllegalArgumentException(
          s"Vector column '${field.name}' has unsupported vector type: $other"
        )
    }
  }

  private[read] def decodeBinaryTypeVectorForSearch(
      bytes: Array[Byte],
      field: StructField
  ): Array[Float] = {
    if (bytes == null) return null
    if (!field.metadata.contains(FieldMetadata.MilvusDataTypeMetadataKey)) {
      throw new IllegalArgumentException(
        s"BinaryType vector search requires ${FieldMetadata.MilvusDataTypeMetadataKey} metadata"
      )
    }

    field.metadata
      .getLong(FieldMetadata.MilvusDataTypeMetadataKey)
      .toInt match {
      case DataType.FloatVector.value =>
        requireAlignedBytes(bytes, 4, field)
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        (0 until (bytes.length / 4)).map(_ => buffer.getFloat()).toArray
      case DataType.Float16Vector.value =>
        requireAlignedBytes(bytes, 2, field)
        bytes
          .grouped(2)
          .map(b => FloatConverter.fromFloat16Bytes(b.toSeq))
          .toArray
      case DataType.BFloat16Vector.value =>
        requireAlignedBytes(bytes, 2, field)
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

  private def requireAlignedBytes(
      bytes: Array[Byte],
      elementBytes: Int,
      field: StructField
  ): Unit =
    require(
      bytes.length % elementBytes == 0,
      s"Vector column '${field.name}' has ${bytes.length} bytes; expected a multiple of $elementBytes"
    )
}
