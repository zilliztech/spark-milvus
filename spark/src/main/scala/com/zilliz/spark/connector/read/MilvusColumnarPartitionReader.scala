package com.zilliz.spark.connector.read

import scala.collection.mutable.ArrayBuilder

import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.{
  ArrayType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.vectorized.{
  ArrowColumnVector,
  ColumnVector,
  ColumnarBatch
}

import com.zilliz.milvus.storage.expr.{
  ExclusionBitmap,
  PredicateEvaluator,
  PredicateExpr
}
import com.zilliz.milvus.storage.read.exec.{ReadMetrics, SegmentReader}
import com.zilliz.milvus.storage.schema.{FieldMetadata, MilvusTypes}
import com.zilliz.spark.connector.metrics.{
  ReadEndReason,
  ReadFailurePhase,
  ReadProgress,
  ScanMetrics
}
import com.zilliz.spark.connector.types.{
  MilvusSparseVectorColumn,
  MilvusVectorColumn,
  SelectedRowsColumn,
  Utf8FromBinaryColumn
}
import com.zilliz.spark.connector.types.MilvusArrayColumn
import io.milvus.grpc.schema.{CollectionSchema, DataType => MilvusDataType}

/** Hands Spark whole batches instead of rows.
  *
  * A batch arrives from the [[SegmentReader]] as an Arrow `VectorSchemaRoot`.
  * Scalar columns go through Spark's own `ArrowColumnVector`, which wraps the
  * Arrow vector without copying. Vector columns cannot: Milvus stores them as
  * `FixedSizeBinary` and `ArrowColumnVector` has no accessor for that, so they
  * are presented by `MilvusVectorColumn`, which decodes one element at a time
  * out of the same buffer.
  *
  * Deletes and pushed predicates produce the same exclusion decision. Spark's
  * `ColumnarBatch` carries a row count and nothing else, so a batch with any
  * excluded row is delivered as a selection over the surviving ones. A batch
  * where every row survives still passes through without an identity array.
  */
class MilvusColumnarPartitionReader(
    schema: StructType,
    segmentReader: SegmentReader,
    milvusSchema: CollectionSchema,
    deleted: (VectorSchemaRoot, Int) => Boolean,
    arrowColumnFor: String => String,
    rawVectors: Boolean,
    partitionId: Long,
    segmentId: Long,
    // The snapshot's Milvus id; required only when `_milvus_row_id` is requested.
    snapshotId: Long = 0L,
    pushedExpression: Option[PredicateExpr] = None,
    columnNameFor: Long => Option[String] = (_: Long) => None,
    taskAllocatorOwner: Option[AutoCloseable] = None,
    private[read] val progress: ReadProgress = new ReadProgress
) extends PartitionReader[ColumnarBatch]
    with Logging {

  private val fieldsByName: Map[String, io.milvus.grpc.schema.FieldSchema] =
    milvusSchema.fields.map(field => field.name -> field).toMap

  private var current: VectorSchemaRoot = null
  private var batch: ColumnarBatch = null
  private var rowsSeen: Long = 0L
  private var ownedSegmentReader: SegmentReader = segmentReader
  private var allocatorOwner: AutoCloseable = taskAllocatorOwner.orNull
  private var finalMetrics: ReadMetrics = ReadMetrics.Zero
  private var closed = false
  private var exhausted = false

  override def next(): Boolean = {
    if (closed || exhausted) return false
    progress.inPhase(ReadFailurePhase.Close)(closeCurrent())
    progress.inPhase(ReadFailurePhase.Read)(ownedSegmentReader.next()) match {
      case None =>
        exhausted = true
        progress.end(ReadEndReason.Exhausted)
        false
      case Some(root) =>
        current = root
        progress.decodedRows(root.getRowCount.toLong)
        batch = progress.inPhase(ReadFailurePhase.Convert)(toBatch(root))
        true
    }
  }

  override def get(): ColumnarBatch = {
    require(
      batch != null && !closed,
      "No current batch; call next() before get()"
    )
    batch
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] =
    ScanMetrics.taskValues(
      if (ownedSegmentReader != null) ownedSegmentReader.metrics
      else finalMetrics
    ) ++ progress.taskValues

  override def close(): Unit = {
    if (closed) return
    closed = true
    try progress.inPhase(ReadFailurePhase.Close)(releaseAll())
    finally progress.close()
  }

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
    closeResource(closeCurrent())
    if (ownedSegmentReader != null) {
      val owned = ownedSegmentReader
      ownedSegmentReader = null
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

  private def closeCurrent(): Unit = {
    val ownedBatch = batch
    val ownedRoot = current
    batch = null
    current = null
    var failure: Throwable = null
    try if (ownedBatch != null) ownedBatch.close()
    catch {
      case error: Throwable =>
        progress.failed(ReadFailurePhase.Close, error)
        failure = error
    }
    try if (ownedRoot != null) ownedRoot.close()
    catch {
      case error: Throwable =>
        progress.failed(ReadFailurePhase.Close, error)
        if (failure == null) failure = error
        else if (failure ne error) failure.addSuppressed(error)
    }
    if (failure != null) throw failure
  }

  private def toBatch(root: VectorSchemaRoot): ColumnarBatch = {
    val startOffset = rowsSeen
    rowsSeen += root.getRowCount.toLong

    val filterBitmap = progress.inPhase(ReadFailurePhase.Filter) {
      pushedExpression
        .map(PredicateEvaluator.evaluate(_, root, columnNameFor))
        .orNull
    }
    val surviving = progress.inPhase(ReadFailurePhase.Filter)(
      survivingRows(root, filterBitmap)
    )
    val columns =
      schema.fields.map(field => columnFor(root, field, startOffset))
    if (surviving == null) {
      new ColumnarBatch(columns, root.getRowCount)
    } else {
      val selected = columns.zip(schema.fields).map { case (column, field) =>
        new SelectedRowsColumn(column, surviving, field.dataType): ColumnVector
      }
      new ColumnarBatch(selected, surviving.length)
    }
  }

  /** The rows to deliver, or null when every row survives.
    *
    * Null rather than an identity array on purpose: that is the common case and
    * it should cost nothing at all, not an allocation plus an indirection per
    * access.
    */
  private def survivingRows(
      root: VectorSchemaRoot,
      filterBitmap: ExclusionBitmap
  ): Array[Int] = {
    val rows = root.getRowCount
    var keep: ArrayBuilder[Int] = null
    var deletedCount = 0L
    var filteredCount = 0L
    var i = 0
    try {
      while (i < rows) {
        val excluded = if (deleted(root, i)) {
          deletedCount += 1L
          true
        } else if (filterBitmap != null && filterBitmap.isExcluded(i)) {
          filteredCount += 1L
          true
        } else false
        if (excluded && keep == null) {
          keep = Array.newBuilder[Int]
          keep.sizeHint(rows)
          var earlier = 0
          while (earlier < i) {
            keep += earlier
            earlier += 1
          }
        } else if (!excluded && keep != null) keep += i
        i += 1
      }
    } finally {
      progress.excludedRows(deletedCount, filteredCount)
    }
    if (keep == null) null else keep.result()
  }

  private def columnFor(
      root: VectorSchemaRoot,
      field: StructField,
      startOffset: Long
  ): ColumnVector = {
    val name = field.name
    val metadataColumn =
      if (MetadataColumns.isSynthetic(field))
        MetadataColumns.columnFor(
          name,
          partitionId,
          segmentId,
          startOffset,
          snapshotId
        )
      else None
    metadataColumn
      .getOrElse {
        // The Arrow column is not always named after the Spark field: the
        // manifest line names columns by field id, so `vec` arrives as `101`.
        val arrowName = arrowColumnFor(name)
        val vector = root.getVector(arrowName)
        if (vector == null) {
          throw new IllegalStateException(
            s"the batch has no column '$arrowName' for field '$name'; it " +
              s"carries ${root.getSchema.getFields.toString}"
          )
        }
        val milvusType =
          if (
            field.metadata.contains(FieldMetadata.MilvusDataTypeMetadataKey)
          ) {
            Some(
              MilvusDataType.fromValue(
                field.metadata
                  .getLong(FieldMetadata.MilvusDataTypeMetadataKey)
                  .toInt
              )
            )
          } else fieldsByName.get(name).map(_.dataType)
        milvusType match {
          case Some(t) if MilvusTypes.isDenseVectorType(t) =>
            MilvusVectorColumn(vector, t, dimensionOf(name), rawVectors)
          case Some(MilvusDataType.SparseFloatVector) if !rawVectors =>
            MilvusSparseVectorColumn(vector.asInstanceOf[VarBinaryVector])
          case _ =>
            scalarColumn(field, vector)
        }
      }
  }

  /** A scalar column, wrapped for the type Spark's schema declares.
    *
    * The declared type and the stored type are not always the same: a JSON
    * field is `StringType` to Spark and Arrow `Binary` on disk, and Spark's own
    * `ArrowColumnVector` chooses its accessor from the stored type, so it would
    * answer `getBinary` where Spark calls `getUTF8String`. Everything whose two
    * types do agree goes through `ArrowColumnVector`, which wraps the Arrow
    * vector without copying.
    */
  private def scalarColumn(
      field: StructField,
      vector: org.apache.arrow.vector.FieldVector
  ): ColumnVector = (field.dataType, vector) match {
    case (StringType, v: VarBinaryVector) => new Utf8FromBinaryColumn(v)
    // A Milvus Array field is stored as one serialized ScalarField per row.
    case (ArrayType(elementType, _), v: VarBinaryVector) =>
      new MilvusArrayColumn(v, elementType)
    case _ => new ArrowColumnVector(vector)
  }

  private def dimensionOf(name: String): Int = {
    val field = schema(name)
    if (
      !field.metadata.contains(FieldMetadata.MilvusVectorDimensionMetadataKey)
    ) {
      throw new IllegalStateException(
        s"vector column '$name' has no dimension in its metadata, so the " +
          "batch cannot be split into vectors"
      )
    }
    field.metadata
      .getLong(FieldMetadata.MilvusVectorDimensionMetadataKey)
      .toInt
  }
}
