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
import com.zilliz.spark.connector.options.{MilvusOption, VectorSearch}
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.CollectionSchema

object MilvusPartitionReaderFactory {
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
      failure: Throwable
  ): Unit =
    if (resource != null) {
      try resource.close()
      catch {
        case closeFailure: Throwable =>
          if (closeFailure ne failure) failure.addSuppressed(closeFailure)
      }
    }

}

// PartitionReaderFactory for Storage V2 (Milvus 2.6+)
class MilvusPartitionReaderFactory(
    schema: StructType,
    optionsMap: Map[String, String],
    pushedExpression: Option[PredicateExpr] = None,
    // A pushed-down limit, applied per partition. None when Spark pushed none.
    limit: Option[Int] = None
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
    * core expression directly against each Arrow batch. Brute-force and
    * persisted-index searches still require the row reader.
    */
  override def supportColumnarReads(partition: InputPartition): Boolean =
    MilvusOption.readColumnar(optionsMap) && (partition match {
      case p: MilvusInputPartition => searchFor(p).isEmpty
      case _                       => false
    })

  /** The `vector.search.*` request a partition's row reader runs, for either
    * storage line and either mode.
    */
  private[read] def searchFor(
      partition: MilvusInputPartition
  ): Option[VectorSearch] = partition.milvusOption.vectorSearch

  override def createColumnarReader(
      partition: InputPartition
  ): PartitionReader[ColumnarBatch] = {
    val reader = batchReaderFor(partition)
    try limit.fold(reader)(n => new LimitedBatchReader(reader, n))
    catch {
      case failure: Throwable =>
        MilvusPartitionReaderFactory.closeAfterFailure(reader, failure)
        throw failure
    }
  }

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val reader = rowReaderFor(partition)
    try limit.fold(reader)(n => new LimitedRowReader(reader, n))
    catch {
      case failure: Throwable =>
        MilvusPartitionReaderFactory.closeAfterFailure(reader, failure)
        throw failure
    }
  }

  private def batchReaderFor(
      partition: InputPartition
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
        segmentReader = setup.open(taskAllocator.allocator)
        new MilvusColumnarPartitionReader(
          schema = schema,
          segmentReader = segmentReader,
          milvusSchema = milvusSchema,
          deleted = setup.isDeleted,
          arrowColumnFor = setup.arrowColumnFor,
          rawVectors = MilvusOption.readVectorRaw(optionsMap),
          partitionName = partitionNameOf(p),
          segmentId = p.task.segmentId,
          requestedExtraColumns = requestedExtraColumns,
          pushedExpression = pushedExpression,
          milvusFilter = p.milvusOption.milvusFilter,
          columnNameFor = setup.columnNameFor,
          taskAllocatorOwner = Some(taskAllocator)
        )
      } catch {
        case failure: Throwable =>
          MilvusPartitionReaderFactory.closeAfterFailure(
            segmentReader,
            failure
          )
          MilvusPartitionReaderFactory.closeAfterFailure(
            taskAllocator,
            failure
          )
          throw failure
      }
    case other =>
      throw new IllegalArgumentException(
        s"cannot read ${other.getClass.getName} a batch at a time"
      )
  }

  private def partitionNameOf(p: MilvusInputPartition): String = p match {
    case v3: MilvusV3InputPartition => v3.partitionName
    case other                      => other.task.partitionId.toString
  }

  private def rowReaderFor(
      partition: InputPartition
  ): PartitionReader[InternalRow] = partition match {
    case p: MilvusInputPartition =>
      logInfo(s"Creating row reader for segment ${p.task.segmentId}")
      val dataSchema = StructType(schema.fields.filterNot { field =>
        isMetadataExtraField(field.name) ||
        (p.milvusOption.vectorSearch.exists(
          _.mode == "index"
        ) && field.name == MilvusOption.VectorSearchScore)
      })
      val search = searchFor(p)
      val setup = ColumnBinding(p, dataSchema)
      val includeSearchScore =
        schema.fieldNames.contains(MilvusOption.VectorSearchScore)
      val searchScorePosition = Option(
        schema.fields
          .filterNot(f => isMetadataExtraField(f.name))
          .indexWhere(_.name == MilvusOption.VectorSearchScore)
      ).filter(_ >= 0)
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
          milvusFilter = p.milvusOption.milvusFilter,
          vectorSearch = search,
          includeSearchScore = includeSearchScore,
          searchScorePosition = searchScorePosition,
          allocator = taskAllocator.allocator,
          taskAllocatorOwner = Some(taskAllocator)
        )
        MetadataColumns.wrapRows(
          rowReader,
          schema,
          requestedExtraColumns,
          partitionNameOf(p),
          p.task.segmentId
        )
      } catch {
        case failure: Throwable =>
          MilvusPartitionReaderFactory.closeAfterFailure(rowReader, failure)
          MilvusPartitionReaderFactory.closeAfterFailure(
            taskAllocator,
            failure
          )
          throw failure
      }
    case other =>
      throw new IllegalArgumentException(
        s"cannot read ${other.getClass.getName} a row at a time"
      )
  }
}
