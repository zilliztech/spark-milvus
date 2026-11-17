package com.zilliz.spark.connector.read

import org.apache.spark.internal.Logging
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
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.CollectionSchema

object MilvusPartitionReaderFactory {

  /** Storage access is already bound in SegmentReadTask. The factory only needs
    * these presentation choices, never service credentials or SDK state.
    */
  private[read] def readerOptions(
      options: Map[String, String]
  ): Map[String, String] = {
    val keys = Set(
      MilvusOption.MilvusExtraColumns,
      MilvusOption.ReadMode,
      MilvusOption.ReadIncludeRowId,
      MilvusOption.ReadColumnar,
      MilvusOption.ReadVectorRaw
    ).map(_.toLowerCase(java.util.Locale.ROOT))
    options.filter { case (key, _) =>
      keys(key.toLowerCase(java.util.Locale.ROOT))
    }
  }
  private[read] def requestedExtraColumns(
      optionsMap: Map[String, String]
  ): Set[String] = MilvusOption.extraColumns(optionsMap).toSet

  private[read] def isMetadataExtraField(
      name: String,
      requestedExtraColumns: Set[String]
  ): Boolean =
    requestedExtraColumns.contains(name) &&
      MetadataColumns.isSyntheticColumn(name)

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
    optionsMap: Map[String, String],
    pushedExpression: Option[PredicateExpr] = None,
    // A pushed-down limit, applied per partition. None when Spark pushed none.
    limit: Option[Int] = None,
    // The snapshot's Milvus id, which `_milvus_row_id` carries; zero when the
    // read does not request it.
    snapshotId: Long = 0L,
    operationToken: Option[OperationToken] = None
) extends PartitionReaderFactory
    with Logging {

  private val requestedExtraColumns =
    MilvusPartitionReaderFactory.requestedExtraColumns(optionsMap)

  private def isMetadataExtraField(name: String): Boolean =
    MilvusPartitionReaderFactory.isMetadataExtraField(
      name,
      requestedExtraColumns
    )

  /** Whether this partition can be read a batch at a time.
    *
    * Both lines use the same SegmentReader. Columnar reads are enabled by
    * default; callers may explicitly select the row path.
    *
    * A connector-owned predicate stays columnar: both readers evaluate the same
    * core expression directly against each Arrow batch.
    */
  override def supportColumnarReads(partition: InputPartition): Boolean =
    MilvusOption.readColumnar(optionsMap) && (partition match {
      case _: MilvusInputPartition => true
      case _                       => false
    })

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
      observe(
        limited,
        progress,
        (batch: ColumnarBatch) => batch.numRows().toLong
      )
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

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val progress = taskProgress(partition)
    var reader: PartitionReader[InternalRow] = null
    try {
      reader = progress.inPhase(ReadFailurePhase.Open)(
        rowReaderFor(partition, progress)
      )
      val limited =
        limit.fold(reader)(n => new LimitedRowReader(reader, n, Some(progress)))
      observe(limited, progress, (_: InternalRow) => 1L)
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

  private def observe[T](
      reader: PartitionReader[T],
      progress: ReadProgress,
      rows: T => Long
  ): PartitionReader[T] = {
    new ObservedPartitionReader(reader, progress, rows)
  }

  private def batchReaderFor(
      partition: InputPartition,
      progress: ReadProgress
  ): PartitionReader[ColumnarBatch] = partition match {
    case p: MilvusInputPartition =>
      val dataSchema = StructType(schema.fields.filterNot { field =>
        isMetadataExtraField(field.name)
      })
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
          rawVectors = MilvusOption.readVectorRaw(optionsMap),
          segmentId = p.task.segmentId,
          requestedExtraColumns = requestedExtraColumns,
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

  private def rowReaderFor(
      partition: InputPartition,
      progress: ReadProgress
  ): PartitionReader[InternalRow] = partition match {
    case p: MilvusInputPartition =>
      logInfo(s"Creating row reader for segment ${p.task.segmentId}")
      val dataSchema = StructType(schema.fields.filterNot { field =>
        isMetadataExtraField(field.name)
      })
      val setup = ColumnBinding(p, dataSchema)
      val taskAllocator = ArrowAllocator.forReadTask(
        p.task.segmentId,
        p.task.limits.arrowMaxBytes
      )
      var rowReader: MilvusRowPartitionReader = null
      try {
        rowReader = new MilvusRowPartitionReader(
          schema = dataSchema,
          setup = setup,
          pushedExpression = pushedExpression,
          allocator = taskAllocator.allocator,
          taskAllocatorOwner = Some(taskAllocator),
          progress = progress
        )
        MetadataColumns.wrapRows(
          rowReader,
          schema,
          requestedExtraColumns,
          p.task.segmentId,
          snapshotId
        )
      } catch {
        case failure: Throwable =>
          MilvusPartitionReaderFactory.closeAfterFailure(
            rowReader,
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
        s"cannot read ${other.getClass.getName} a row at a time"
      )
  }
}
