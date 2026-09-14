package com.zilliz.spark.connector.scan

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

import com.zilliz.milvus.storage.delete.{MilvusDeletePlan, MilvusDeltaLogReader}
import com.zilliz.milvus.storage.read.plan.DeleteSource
import com.zilliz.spark.connector.serde.ArrowAllocator
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.CollectionSchema

object MilvusPartitionReaderFactory {
  private[scan] def requestedExtraColumns(
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

  private[scan] def isMetadataExtraField(
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
    packedV2DeleteContext: MilvusPackedV2DeleteContext =
      MilvusPackedV2DeleteContext.empty,
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
    * `MilvusPackedV2InputPartition` carries neither: its scan builder returns
    * every predicate to Spark and it has no search parameters.
    */
  override def supportColumnarReads(partition: InputPartition): Boolean =
    MilvusOption.readColumnar(optionsMap) && (partition match {
      case p: MilvusStorageV3InputPartition =>
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
      val setup = SegmentReadSetup(effectiveSpecOf(p), dataSchema)
      val milvusSchema = CollectionSchema.parseFrom(p.spec.schemaBytes)
      new MilvusColumnarPartitionReader(
        schema,
        setup.open(ArrowAllocator.get),
        milvusSchema,
        setup.isDeleted,
        setup.arrowColumnFor,
        MilvusOption.readVectorRaw(optionsMap),
        partitionNameOf(p),
        p.spec.segmentId
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
      case v2: MilvusPackedV2InputPartition =>
        val inherited = v2.inheritedDeletePlanPartitionId
          .map(partitionId =>
            MilvusDeltaLogReader.effectiveInheritedDeletePlan(
              partitionId,
              packedV2DeleteContext.inheritedPlansByPartition
            )
          )
          .getOrElse(MilvusDeletePlan.empty)
        val combined = MilvusDeletePlan.union(inherited, v2.spec.deletePlan)
        v2.copy(spec =
          v2.spec.copy(deletes =
            if (combined.isEmpty) DeleteSource.None
            else DeleteSource.Materialized(combined)
          )
        )
      case other => other
    }

  private def partitionNameOf(p: MilvusInputPartition): String = p match {
    case v3: MilvusStorageV3InputPartition => v3.partitionName
    case other                             => other.spec.partitionId.toString
  }

  private def rowReaderFor(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    partition match {
      case p: MilvusStorageV3InputPartition =>
        logInfo(
          s"Creating V3 reader for partition with segmentID=${p.spec.segmentId}"
        )

        val v2Schema = StructType(schema.fields.filterNot { field =>
          isMetadataExtraField(field.name)
        })

        // Deserialize the protobuf schema
        val milvusSchema = CollectionSchema.parseFrom(p.spec.schemaBytes)

        // Create MilvusLoonPartitionReader directly
        val underlyingReader = new MilvusLoonPartitionReader(
          v2Schema,
          LoonReadSetup(p, v2Schema),
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
          p.spec.segmentId
        )

      case p: MilvusPackedV2InputPartition =>
        logInfo(
          s"Creating packed-V2 reader for segmentID=${p.spec.segmentId} " +
            s"with ${p.spec.dataFiles.size} data file(s)"
        )

        val innerSchema = StructType(schema.fields.filterNot { field =>
          isMetadataExtraField(field.name)
        })

        val milvusSchema = CollectionSchema.parseFrom(p.spec.schemaBytes)

        val inheritedDeletePlan = p.inheritedDeletePlanPartitionId
          .map(partitionId =>
            MilvusDeltaLogReader.effectiveInheritedDeletePlan(
              partitionId,
              packedV2DeleteContext.inheritedPlansByPartition
            )
          )
          .getOrElse(MilvusDeletePlan.empty)
        // The inherited plan is only resolvable here, where the executor-side
        // context is, so the spec is finished off rather than rebuilt.
        val effectiveDeletePlan =
          MilvusDeletePlan.union(inheritedDeletePlan, p.spec.deletePlan)
        val effectiveSpec = p.spec.copy(
          deletes =
            if (effectiveDeletePlan.isEmpty) DeleteSource.None
            else DeleteSource.Materialized(effectiveDeletePlan)
        )

        val underlying = new MilvusPackedV2PartitionReader(
          innerSchema,
          PackedV2ReadSetup(p, innerSchema, effectiveSpec),
          milvusSchema,
          p.milvusOption
        )

        MetadataColumns.wrapRows(
          underlying,
          schema,
          requestedExtraColumns,
          p.spec.partitionId.toString,
          p.spec.segmentId
        )

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported partition type: ${partition.getClass.getName}. " +
            "This connector requires Milvus 2.6+ (Storage V2)."
        )
    }
  }
}
