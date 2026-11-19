package com.zilliz.spark.connector.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.zilliz.milvus.storage.expr.PredicateExpr
import com.zilliz.milvus.storage.read.exec.SegmentReader
import com.zilliz.spark.connector.metrics.{
  OperationToken,
  OperationalMetrics,
  ReadFailurePhase,
  ReadProgress
}
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.CollectionSchema

object MilvusPartitionReaderFactory {

  private[read] def closeAfterFailure(
      resource: AutoCloseable,
      failure: Throwable,
      progress: Option[ReadProgress] = None
  ): Unit =
    if (resource != null) {
      try resource.close()
      catch {
        case closeFailure: Throwable =>
          progress.foreach(_.failed(ReadFailurePhase.Close, closeFailure))
          if (closeFailure ne failure) failure.addSuppressed(closeFailure)
      }
    }

}

// PartitionReaderFactory for fixed V3 and resolved physical tasks.
class MilvusPartitionReaderFactory(
    schema: StructType,
    rawVectors: Boolean,
    pushedExpression: Option[PredicateExpr] = None,
    // A pushed-down limit, applied per partition. None when Spark pushed none.
    limit: Option[Int] = None,
    // An identified physical snapshot supplies this for `_milvus_row_id`.
    snapshotId: Long = 0L,
    operationToken: Option[OperationToken] = None
) extends PartitionReaderFactory {

  private val dataSchema =
    StructType(schema.fields.filterNot(MetadataColumns.isSynthetic))

  override def supportColumnarReads(partition: InputPartition): Boolean =
    partition.isInstanceOf[MilvusInputPartition]

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException(
      "Milvus snapshot scans support only columnar reads"
    )

  override def createColumnarReader(
      partition: InputPartition
  ): PartitionReader[ColumnarBatch] = {
    val progress = taskProgress(partition)
    var reader: PartitionReader[ColumnarBatch] = null
    try {
      reader = progress.inPhase(ReadFailurePhase.Open)(
        batchReaderFor(partition, progress)
      )
      val limited = limit.fold(reader)(n =>
        new LimitedBatchReader(reader, n, Some(progress))
      )
      new ObservedPartitionReader(limited, progress)
    } catch {
      case failure: Throwable =>
        progress.failed(ReadFailurePhase.Open, failure)
        MilvusPartitionReaderFactory.closeAfterFailure(
          reader,
          failure,
          Some(progress)
        )
        progress.close()
        throw failure
    }
  }

  private def taskProgress(partition: InputPartition): ReadProgress = {
    val logicalPartition = partition match {
      case input: MilvusInputPartition => Some(input.task.segmentId.toString)
      case _                           => None
    }
    new ReadProgress(
      operationToken.map(token =>
        OperationalMetrics.task(token, "reading", logicalPartition)
      )
    )
  }

  private def batchReaderFor(
      partition: InputPartition,
      progress: ReadProgress
  ): PartitionReader[ColumnarBatch] = partition match {
    case p: MilvusInputPartition =>
      val setup = ColumnBinding(p, dataSchema)
      val milvusSchema = CollectionSchema.parseFrom(p.task.schemaBytes)
      val taskAllocator = ArrowAllocator.forReadTask(
        p.task.segmentId,
        p.task.limits.arrowMaxBytes
      )
      var segmentReader: SegmentReader = null
      try {
        progress.inPhase(ReadFailurePhase.DeleteLoad)(setup.deletePlan)
        segmentReader = setup.open(taskAllocator.allocator)
        new MilvusColumnarPartitionReader(
          schema = schema,
          segmentReader = segmentReader,
          milvusSchema = milvusSchema,
          deleted = setup.isDeleted,
          arrowColumnFor = setup.arrowColumnFor,
          rawVectors = rawVectors,
          partitionId = p.task.partitionId,
          segmentId = p.task.segmentId,
          snapshotId = snapshotId,
          pushedExpression = pushedExpression,
          columnNameFor = setup.columnNameFor,
          taskAllocatorOwner = Some(taskAllocator),
          progress = progress
        )
      } catch {
        case failure: Throwable =>
          MilvusPartitionReaderFactory.closeAfterFailure(
            segmentReader,
            failure,
            Some(progress)
          )
          MilvusPartitionReaderFactory.closeAfterFailure(
            taskAllocator,
            failure,
            Some(progress)
          )
          throw failure
      }
    case other =>
      throw new IllegalArgumentException(
        s"cannot read ${other.getClass.getName} a batch at a time"
      )
  }

}
