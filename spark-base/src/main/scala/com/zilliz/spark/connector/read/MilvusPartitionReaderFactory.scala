package com.zilliz.spark.connector.read

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.zilliz.milvus.storage.delete.{DeletePlan, DeltaLogReader}
import com.zilliz.milvus.storage.read.plan.DeleteSource
import com.zilliz.spark.connector.types.ArrowAllocator
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.CollectionSchema

object MilvusPartitionReaderFactory {
  private[read] def requestedExtraColumns(
      optionsMap: Map[String, String]
  ): Set[String] = {
    optionsMap
      .collectFirst {
        case (key, value)
            if key.equalsIgnoreCase(MilvusOption.MilvusExtraColumns) =>
          value
      }
      .toSeq
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(MilvusOption.normalizeExtraColumnName)
      .toSet
  }

  private[read] def isMetadataExtraField(
      name: String,
      requestedExtraColumns: Set[String]
  ): Boolean =
    requestedExtraColumns.contains(name)

}

// PartitionReaderFactory for Storage V2 (Milvus 2.6+)
class MilvusPartitionReaderFactory(
    schema: StructType,
    optionsMap: Map[String, String],
    pushedFilters: Array[Filter] = Array.empty[Filter],
    v2InheritedDeletes: V2InheritedDeletes =
      V2InheritedDeletes.empty,
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
    * Both lines can, because both end at the same SegmentReader. It is off
    * unless the read asks, since the row path is what every existing job runs
    * and the two have to be shown to agree before the default moves.
    *
    * Two things the row reader does are not implemented columnar yet, and each
    * of them sends the partition back to the row reader:
    *
    *   - A pushed-down filter. `MilvusScanBuilder.pushFilters` returns the
    *     predicates it cannot handle to Spark and keeps the rest, and Spark's
    *     contract is that the source evaluates what it kept. The columnar
    *     reader does not, so a partition carrying pushed filters read columnar
    *     returns rows that should have been filtered out, with no error.
    *   - Vector search. `topK` and `queryVector` make the row reader run a
    *     brute-force search instead of a scan; the columnar reader would ignore
    *     them and return the whole segment.
    *
    * `MilvusV2InputPartition` carries neither: its scan builder returns
    * every predicate to Spark and it has no search parameters.
    */
  override def supportColumnarReads(partition: InputPartition): Boolean =
    MilvusOption.readColumnar(optionsMap) && (partition match {
      case p: MilvusV3InputPartition =>
        pushedFilters.isEmpty && p.topK.isEmpty && p.queryVector.isEmpty
      case _: MilvusInputPartition => pushedFilters.isEmpty
      case _                       => false
    })

  override def createColumnarReader(
      partition: InputPartition
  ): PartitionReader[ColumnarBatch] = {
    val reader = batchReaderFor(partition)
    limit.fold(reader)(n => new LimitedBatchReader(reader, n))
  }

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val reader = rowReaderFor(partition)
    limit.fold(reader)(n => new LimitedRowReader(reader, n))
  }

  private def batchReaderFor(
      partition: InputPartition
  ): PartitionReader[ColumnarBatch] = partition match {
    case p: MilvusInputPartition =>
      val dataSchema = StructType(schema.fields.filterNot { field =>
        isMetadataExtraField(field.name)
      })
      val setup = ColumnBinding(effectiveSpecOf(p), dataSchema)
      val milvusSchema = CollectionSchema.parseFrom(p.task.schemaBytes)
      new MilvusColumnarPartitionReader(
        schema,
        setup.open(ArrowAllocator.get),
        milvusSchema,
        setup.isDeleted,
        setup.arrowColumnFor,
        MilvusOption.readVectorRaw(optionsMap),
        partitionNameOf(p),
        p.task.segmentId
      )
    case other =>
      throw new IllegalArgumentException(
        s"cannot read ${other.getClass.getName} a batch at a time"
      )
  }

  /** The partition with its inherited delete plan folded in.
    *
    * Only the column-group line defers that: its inherited plan is looked up
    * from the executor-side context rather than shipped per partition.
    */
  private def effectiveSpecOf(p: MilvusInputPartition): MilvusInputPartition =
    p match {
      case v2: MilvusV2InputPartition =>
        val inherited = v2.inheritedDeletePlanPartitionId
          .map(partitionId =>
            DeltaLogReader.effectiveInheritedDeletePlan(
              partitionId,
              v2InheritedDeletes.inheritedPlansByPartition
            )
          )
          .getOrElse(DeletePlan.empty)
        val combined = DeletePlan.union(inherited, v2.task.deletePlan)
        v2.copy(task =
          v2.task.copy(deletes =
            if (combined.isEmpty) DeleteSource.None
            else DeleteSource.Materialized(combined)
          )
        )
      case other => other
    }

  private def partitionNameOf(p: MilvusInputPartition): String = p match {
    case v3: MilvusV3InputPartition => v3.partitionName
    case other                             => other.task.partitionId.toString
  }

  private def rowReaderFor(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    partition match {
      case p: MilvusV3InputPartition =>
        logInfo(
          s"Creating V3 reader for partition with segmentID=${p.task.segmentId}"
        )

        val v2Schema = StructType(schema.fields.filterNot { field =>
          isMetadataExtraField(field.name)
        })

        // Deserialize the protobuf schema
        val milvusSchema = CollectionSchema.parseFrom(p.task.schemaBytes)

        // Create MilvusV3PartitionReader directly
        val underlyingReader = new MilvusV3PartitionReader(
          v2Schema,
          V3ColumnBinding(p, v2Schema),
          milvusSchema,
          p.milvusOption,
          optionsMap,
          p.topK,
          p.queryVector,
          p.metricType,
          p.vectorColumn,
          pushedFilters
        )

        MetadataColumns.wrapRows(
          underlyingReader,
          schema,
          requestedExtraColumns,
          p.partitionName,
          p.task.segmentId
        )

      case p: MilvusV2InputPartition =>
        logInfo(
          s"Creating V2 reader for segmentID=${p.task.segmentId} " +
            s"with ${p.task.dataFiles.size} data file(s)"
        )

        val innerSchema = StructType(schema.fields.filterNot { field =>
          isMetadataExtraField(field.name)
        })

        val milvusSchema = CollectionSchema.parseFrom(p.task.schemaBytes)

        val inheritedDeletePlan = p.inheritedDeletePlanPartitionId
          .map(partitionId =>
            DeltaLogReader.effectiveInheritedDeletePlan(
              partitionId,
              v2InheritedDeletes.inheritedPlansByPartition
            )
          )
          .getOrElse(DeletePlan.empty)
        // The inherited plan is only resolvable here, where the executor-side
        // context is, so the task is finished off rather than rebuilt.
        val effectiveDeletePlan =
          DeletePlan.union(inheritedDeletePlan, p.task.deletePlan)
        val effectiveSpec = p.task.copy(
          deletes =
            if (effectiveDeletePlan.isEmpty) DeleteSource.None
            else DeleteSource.Materialized(effectiveDeletePlan)
        )

        val underlying = new MilvusV2PartitionReader(
          innerSchema,
          V2ColumnBinding(p, innerSchema, effectiveSpec),
          milvusSchema,
          p.milvusOption
        )

        MetadataColumns.wrapRows(
          underlying,
          schema,
          requestedExtraColumns,
          p.task.partitionId.toString,
          p.task.segmentId
        )

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported partition type: ${partition.getClass.getName}. " +
            "This connector requires Milvus 2.6+ (Storage V2)."
        )
    }
  }
}
