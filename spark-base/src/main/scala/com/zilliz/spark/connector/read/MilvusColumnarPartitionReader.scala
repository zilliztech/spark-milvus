package com.zilliz.spark.connector.read

import org.apache.arrow.vector.{
  FixedSizeBinaryVector,
  VarBinaryVector,
  VectorSchemaRoot
}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{
  ArrowColumnVector,
  ColumnVector,
  ColumnarBatch
}

import com.zilliz.milvus.storage.read.exec.SegmentReader
import com.zilliz.milvus.storage.schema.{FieldMetadata, MilvusTypes}
import com.zilliz.spark.connector.serde.ArrowAllocator
import com.zilliz.spark.connector.types.{
  ConstantColumn,
  MilvusSparseVectorColumn,
  MilvusVectorColumn,
  RowOffsetColumn,
  SelectedRowsColumn
}
import com.zilliz.spark.connector.MilvusOption
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
  * Deletes are the one thing that costs. Spark's `ColumnarBatch` carries a row
  * count and nothing else — there is no way to mark a row invalid — so a batch
  * with deleted rows is delivered as a selection over the surviving ones. That
  * is decision 12, and the cost it names: a batch with no deletes passes
  * through untouched, a batch with deletes pays an int per surviving row.
  */
class MilvusColumnarPartitionReader(
    schema: StructType,
    segmentReader: SegmentReader,
    milvusSchema: CollectionSchema,
    deleted: (VectorSchemaRoot, Int) => Boolean,
    rawVectors: Boolean,
    partitionName: String,
    segmentId: Long
) extends PartitionReader[ColumnarBatch]
    with Logging {

  private val fieldsByName: Map[String, io.milvus.grpc.schema.FieldSchema] =
    milvusSchema.fields.map(field => field.name -> field).toMap

  private var current: VectorSchemaRoot = null
  private var batch: ColumnarBatch = null
  private var rowsSeen: Long = 0L

  override def next(): Boolean = {
    closeCurrent()
    segmentReader.next() match {
      case None => false
      case Some(root) =>
        current = root
        batch = toBatch(root)
        true
    }
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    closeCurrent()
    segmentReader.close()
  }

  private def closeCurrent(): Unit = {
    if (batch != null) {
      try batch.close()
      catch { case e: Throwable => logWarning("closing the batch failed", e) }
      batch = null
    }
    if (current != null) {
      try current.close()
      catch { case e: Throwable => logWarning("closing the root failed", e) }
      current = null
    }
  }

  private def toBatch(root: VectorSchemaRoot): ColumnarBatch = {
    val startOffset = rowsSeen
    rowsSeen += root.getRowCount.toLong

    val columns =
      schema.fields.map(field => columnFor(root, field.name, startOffset))
    val surviving = survivingRows(root)
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
  private def survivingRows(root: VectorSchemaRoot): Array[Int] = {
    val rows = root.getRowCount
    var anyDeleted = false
    var i = 0
    while (i < rows && !anyDeleted) {
      if (deleted(root, i)) anyDeleted = true
      i += 1
    }
    if (!anyDeleted) return null

    val keep = Array.newBuilder[Int]
    keep.sizeHint(rows)
    var j = 0
    while (j < rows) {
      if (!deleted(root, j)) keep += j
      j += 1
    }
    keep.result()
  }

  private def columnFor(
      root: VectorSchemaRoot,
      name: String,
      startOffset: Long
  ): ColumnVector = name match {
    case MilvusOption.MilvusExtraColumnPartition =>
      ConstantColumn.ofString(partitionName)
    case MilvusOption.MilvusExtraColumnSegmentID =>
      ConstantColumn.ofLong(segmentId)
    case MilvusOption.MilvusExtraColumnRowOffset =>
      new RowOffsetColumn(startOffset)
    case _ =>
      val vector = root.getVector(name)
      if (vector == null) {
        throw new IllegalStateException(
          s"the batch has no column '$name'; it carries " +
            root.getSchema.getFields.toString
        )
      }
      val milvusType = fieldsByName.get(name).map(_.dataType)
      milvusType match {
        case Some(t) if MilvusTypes.isDenseVectorType(t) =>
          MilvusVectorColumn(
            vector.asInstanceOf[FixedSizeBinaryVector],
            t,
            dimensionOf(name),
            rawVectors
          )
        case Some(MilvusDataType.SparseFloatVector) if !rawVectors =>
          MilvusSparseVectorColumn(vector.asInstanceOf[VarBinaryVector])
        case _ =>
          // Scalars, and sparse vectors asked for raw: Arrow's own wrapper
          // already presents these without copying.
          new ArrowColumnVector(vector)
      }
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
