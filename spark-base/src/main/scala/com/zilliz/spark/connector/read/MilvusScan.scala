package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReaderFactory,
  Scan,
  Statistics,
  SupportsReportStatistics
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.delete.{DeletePlan, DeltaLogReader}
import com.zilliz.milvus.storage.read.plan.ReadPlan
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.{
  SegmentLayout,
  Snapshot,
  SnapshotOrigin
}
import com.zilliz.spark.connector.options.{MilvusOption, StorageOptions}
import com.zilliz.spark.connector.read.plan.DeletePlanning
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** One read of one [[Snapshot]]: the table resolved it once, this plans one
  * partition per data segment and builds the reader factory from it. No storage
  * is opened here except to read delete files.
  */
class MilvusScan(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    private[read] val snapshot: Snapshot,
    pushedFilters: Array[Filter] = Array.empty[Filter],
    private[read] val pushedLimit: Option[Int] = None
) extends Scan
    with Batch
    with SupportsReportStatistics
    with Logging {
  private val milvusOption = MilvusOption(options)

  /** Row count is the sum over planned partitions; the byte size is that count
    * times an estimated row width. Both come from the plan already built, so
    * this opens nothing. Spark uses them to pick a join strategy, which is what
    * makes a broadcast join possible for a small collection.
    */
  override def estimateStatistics(): Statistics =
    MilvusScan.statisticsFor(planInputPartitions(), schema)

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    if (MilvusOption.readColumnar(options)) Scan.ColumnarSupportMode.SUPPORTED
    else Scan.ColumnarSupportMode.UNSUPPORTED

  private lazy val plannedPartitions: Array[InputPartition] = plan()

  /** Every read is of one fixed snapshot, so the plan is computed once. */
  override def planInputPartitions(): Array[InputPartition] = plannedPartitions

  private def bucket: Option[String] =
    Option(snapshot.bucket).map(_.trim).filter(_.nonEmpty)

  /** Hadoop configuration for objects under `path`, from the connector's `fs.*`
    * options plus per-bucket S3A settings.
    */
  private[read] def hadoopConfFor(path: String): Configuration =
    StorageOptions.buildHadoopConfForOptions(milvusOption.options, path)

  /** Per-bucket Hadoop settings are keyed by the location being read. */
  private def hadoopConf = snapshot.origin match {
    case SnapshotOrigin.Backup(dir) => hadoopConfFor(dir)
    case _                          => hadoopConfFor("")
  }

  private def errorContext: String = s"snapshot ${snapshot.name}"

  private def plan(): Array[InputPartition] = {
    logInfo(
      s"Planning ${snapshot.segments.size} segment(s) of ${snapshot.name} from ${snapshot.origin}"
    )
    val conf = hadoopConf
    val v3 = DeletePlanning.loadV3DeletePlanning(
      milvusOption,
      snapshot,
      bucket,
      conf,
      errorContext
    )
    val v2 =
      DeletePlanning.loadV2DeletePlans(
        milvusOption,
        snapshot,
        bucket,
        conf,
        errorContext
      )
    val inherited = DeletePlanning.loadInheritedDeletePlans(
      milvusOption,
      snapshot,
      bucket,
      conf,
      errorContext
    )
    inputPartitions(
      snapshot,
      v3DeletePlans = v3.deletePlans,
      v3ReadVersions = v3.readVersions,
      v2DeletePlans = v2,
      inheritedDeletePlansByPartition = inherited
    )
  }

  /** The plan, `core.read.plan.ReadPlan.of`, wrapped into Spark's input
    * partitions: each task becomes the partition of its storage line, with what
    * only this layer knows on it (the vector search, the partition name, the
    * option map the reader factory reads, the L0 marker a V2 reader resolves on
    * the executor).
    */
  private[read] def inputPartitions(
      snapshot: Snapshot,
      v3DeletePlans: Map[Long, DeletePlan] = Map.empty,
      v3ReadVersions: Map[Long, Long] = Map.empty,
      v2DeletePlans: Map[Long, DeletePlan] = Map.empty,
      inheritedDeletePlansByPartition: Map[Long, DeletePlan] = Map.empty
  ): Array[InputPartition] = {
    // Every path in the snapshot is a key of `snapshot.bucket`, so that is the
    // bucket the native reader is rooted at, whatever the raw options say (a
    // backup read derives it from `milvus.backup.dir`).
    val canonicalMilvusOption = bucket
      .map(b =>
        milvusOption.copy(
          options = milvusOption.options ++
            Map(StorageProperties.BucketName -> b)
        )
      )
      .getOrElse(milvusOption)
    // Parsed once for the whole plan rather than per partition: a bad storage
    // configuration should fail planning, not every task.
    val plan = ReadPlan.of(
      snapshot,
      properties = {
        case 2 => StorageProperties.from(canonicalMilvusOption.options)
        case _ => StorageProperties.from(milvusOption.options)
      },
      applyDeletes = MilvusOption.readApplyDeletes(options),
      deletes = ReadPlan.Deletes(
        v3BySegment = v3DeletePlans,
        v2BySegment = v2DeletePlans,
        inheritedByPartition = inheritedDeletePlansByPartition
      ),
      readVersions = v3ReadVersions
    )
    val vectorSearch = milvusOption.vectorSearch
    plan.specs.map { task =>
      task.layout match {
        case SegmentLayout.Manifest(_, _) =>
          MilvusV3InputPartition(
            task,
            task.partitionId.toString,
            milvusOption,
            vectorSearch.map(_.topK),
            vectorSearch.map(_.queryVector),
            vectorSearch.map(_.metricType),
            vectorSearch.map(_.vectorColumn)
          ): InputPartition
        case SegmentLayout.ColumnGroups(_) =>
          // The inherited (L0) plan is not shipped per partition: the
          // partition carries the marker and the reader factory resolves it.
          MilvusV2InputPartition(
            task,
            canonicalMilvusOption,
            inheritedDeletePlanPartitionId =
              DeltaLogReader.inheritedDeletePlanPartitionMarker(
                task.partitionId,
                inheritedDeletePlansByPartition
              )
          ): InputPartition
      }
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // The partition-scoped (L0) plans are resolved here, from the snapshot
    // the scan holds, so the factory does not depend on planning having run.
    val inheritedPlansByPartition = DeletePlanning.loadInheritedDeletePlans(
      milvusOption,
      snapshot,
      bucket,
      hadoopConf,
      errorContext = "reader factory"
    )
    new MilvusPartitionReaderFactory(
      schema,
      options.asScala.toMap,
      pushedFilters,
      V2InheritedDeletes(inheritedPlansByPartition),
      pushedLimit
    )
  }
}

object MilvusScan extends Logging {

  /** Table statistics from a plan. Pure, so it is testable without storage.
    *
    * `numRows` is known only when every partition knows its own count; one
    * unknown makes the total unknown rather than an undercount Spark would
    * trust. `sizeInBytes` is rows times [[estimatedRowWidth]].
    */
  private[read] def statisticsFor(
      partitions: Array[InputPartition],
      schema: StructType
  ): Statistics = {
    val specs = partitions.collect { case p: MilvusInputPartition => p.task }
    val rows =
      com.zilliz.milvus.storage.read.plan.ReadPlan(specs.toSeq).totalRows
    val width = estimatedRowWidth(schema)
    new Statistics {
      override def numRows(): java.util.OptionalLong =
        rows.fold(java.util.OptionalLong.empty())(java.util.OptionalLong.of)
      override def sizeInBytes(): java.util.OptionalLong =
        rows.fold(java.util.OptionalLong.empty())(r =>
          java.util.OptionalLong.of(r * width)
        )
    }
  }

  /** Bytes per row of `schema`, as stored.
    *
    * A vector field is presented as an array but stored as a fixed-width blob,
    * so its width is dimension times element width from the field metadata;
    * Spark's own `defaultSize` for an array assumes one element and would put a
    * 768-dimensional column at 4 bytes. Everything else takes `defaultSize`.
    */
  private[read] def estimatedRowWidth(schema: StructType): Long =
    schema.fields.map { field =>
      val md = field.metadata
      if (md.contains(FieldMetadata.MilvusVectorDimensionMetadataKey)) {
        val dim = md.getLong(FieldMetadata.MilvusVectorDimensionMetadataKey)
        val kind =
          if (md.contains(FieldMetadata.MilvusDataTypeMetadataKey)) {
            val value = md.getLong(FieldMetadata.MilvusDataTypeMetadataKey)
            if (value >= Int.MinValue && value <= Int.MaxValue)
              Some(MilvusDataType.fromValue(value.toInt))
            else None
          } else None
        kind match {
          case Some(MilvusDataType.FloatVector) => dim * 4
          case Some(MilvusDataType.Float16Vector) |
              Some(MilvusDataType.BFloat16Vector) =>
            dim * 2
          case Some(MilvusDataType.Int8Vector)   => dim
          case Some(MilvusDataType.BinaryVector) => (dim + 7) / 8
          case _                                 => dim * 4
        }
      } else field.dataType.defaultSize.toLong
    }.sum
}
